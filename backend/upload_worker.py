"""
PATCH TAMBAHAN — Bulk import semua file dari /data_csv sekaligus.

Tambahkan ke upload_worker_patch.py atau buat file baru bulk_import.py

Flow:
  GET  /api/directory/bulk-preview  → lihat semua file yang akan diimport
  POST /api/directory/bulk-import   → import semua file sekaligus (paralel)
  GET  /api/directory/bulk-status/{bulk_id} → status keseluruhan + per file
"""

import os
import re
import time
import uuid
import traceback
import threading
import psycopg2
import psycopg2.extras
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from typing import Optional

# ── Config ───────────────────────────────────────────────────────────────────
DATA_DIR            = "/data_csv"
PARQUET_DIR         = "/data_csv/parquet"
SUPPORTED_EXT       = {".csv", ".parquet", ".xlsx", ".xls"}
LARGE_FILE_MB       = 100       # >= 100MB → streaming mode
STREAM_CHUNK_ROWS   = 100_000   # rows per batch insert ke PG
PARQUET_CHUNK_ROWS  = 200_000   # rows per batch tulis Parquet
MAX_PARALLEL        = 3         # max file diproses bersamaan

PG_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "postgres"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "database": os.getenv("POSTGRES_DB", "airflow"),
    "user":     os.getenv("POSTGRES_USER", "airflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
}

# ── In-memory store untuk bulk jobs ──────────────────────────────────────────
# { bulk_id: { status, files: { filename: { status, pct, ... } } } }
_bulk_jobs: dict = {}
_bulk_lock = threading.Lock()


def get_conn():
    return psycopg2.connect(**PG_CONFIG)


def sanitize_col(c: str) -> str:
    return re.sub(r'[^a-z0-9_]', '_',
           c.strip().lower()
            .replace(" ", "_").replace("-", "_").replace(".", "_"))


def sanitize_table(name: str) -> str:
    t = re.sub(r'[^a-z0-9_]', '_', name.lower())
    return re.sub(r'_+', '_', t).strip('_')


def _fmt_size(size_bytes: int) -> str:
    kb = size_bytes / 1024
    if kb < 1024:        return f"{kb:.1f} KB"
    elif kb < 1024**2:   return f"{kb/1024:.1f} MB"
    else:                return f"{kb/1024/1024:.2f} GB"


def _detect_encoding(path: str) -> str:
    with open(path, "rb") as f:
        sample = f.read(4096)
    try:
        sample.decode("utf-8")
        return "utf-8"
    except UnicodeDecodeError:
        return "latin-1"


# ════════════════════════════════════════════════════════════════════════════
# BULK JOB STATE MANAGEMENT
# ════════════════════════════════════════════════════════════════════════════

def _bulk_set(bulk_id: str, **kwargs):
    with _bulk_lock:
        if bulk_id not in _bulk_jobs:
            _bulk_jobs[bulk_id] = {}
        _bulk_jobs[bulk_id].update(kwargs)


def _file_set(bulk_id: str, filename: str, **kwargs):
    """Update status satu file dalam bulk job."""
    with _bulk_lock:
        if bulk_id not in _bulk_jobs:
            _bulk_jobs[bulk_id] = {}
        if "files" not in _bulk_jobs[bulk_id]:
            _bulk_jobs[bulk_id]["files"] = {}
        if filename not in _bulk_jobs[bulk_id]["files"]:
            _bulk_jobs[bulk_id]["files"][filename] = {}
        _bulk_jobs[bulk_id]["files"][filename].update(kwargs)

        # Hitung overall progress dari semua file
        files     = _bulk_jobs[bulk_id]["files"]
        total_pct = sum(f.get("pct", 0) for f in files.values())
        avg_pct   = int(total_pct / max(len(files), 1))
        done      = sum(1 for f in files.values() if f.get("status") in ("done", "error", "skipped"))
        total     = len(files)

        _bulk_jobs[bulk_id]["progress_pct"]    = avg_pct
        _bulk_jobs[bulk_id]["files_done"]      = done
        _bulk_jobs[bulk_id]["files_total"]     = total
        _bulk_jobs[bulk_id]["overall_status"]  = (
            "done"    if done == total else
            "running" if done < total else
            "pending"
        )


def get_bulk_job(bulk_id: str) -> dict:
    return _bulk_jobs.get(bulk_id, {})


# ════════════════════════════════════════════════════════════════════════════
# SCAN: Ambil semua file dari /data_csv (rekursif opsional)
# ════════════════════════════════════════════════════════════════════════════

def scan_all_files(
    directory: str = DATA_DIR,
    recursive: bool = False,
    skip_parquet_subdir: bool = True,
) -> list[dict]:
    """
    Scan semua file yang didukung di dalam direktori.

    Parameters
    ----------
    directory          : direktori yang di-scan (default: /data_csv)
    recursive          : jika True, masuk ke subfolder juga
    skip_parquet_subdir: jika True, skip folder /data_csv/parquet
                         (isinya output pipeline, bukan input)

    Returns
    -------
    list of dict:
        name, path, size_bytes, size_str, ext, modified_at,
        table_name, mode (streaming|standard), already_imported
    """
    base = Path(directory)
    if not base.exists():
        return []

    # Ambil tabel yang sudah ada di staging (untuk deteksi duplikat)
    existing_tables = _get_existing_staging_tables()

    pattern = "**/*" if recursive else "*"
    result  = []

    for p in sorted(base.glob(pattern)):
        # Skip subfolder /parquet jika diminta
        if skip_parquet_subdir and "parquet" in p.parts:
            continue

        if not p.is_file():
            continue

        if p.suffix.lower() not in SUPPORTED_EXT:
            continue

        size       = p.stat().st_size
        size_mb    = size / (1024 * 1024)
        table_name = sanitize_table(p.stem)

        result.append({
            "name":             p.name,
            "path":             str(p),
            "size_bytes":       size,
            "size_str":         _fmt_size(size),
            "ext":              p.suffix.lower().lstrip("."),
            "modified_at":      p.stat().st_mtime,
            "table_name":       table_name,
            "mode":             "streaming" if size_mb >= LARGE_FILE_MB else "standard",
            "already_imported": table_name in existing_tables,
        })

    return result


def _get_existing_staging_tables() -> set:
    """Ambil nama tabel yang sudah ada di schema staging."""
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'staging'
        """)
        tables = {row[0] for row in cur.fetchall()}
        cur.close()
        conn.close()
        return tables
    except Exception:
        return set()


# ════════════════════════════════════════════════════════════════════════════
# STREAMING IMPORT: satu file CSV
# ════════════════════════════════════════════════════════════════════════════

def _import_csv_streaming(
    bulk_id:        str,
    file_path:      str,
    table_name:     str,
    file_size_bytes: int,
    save_parquet:   bool,
) -> dict:
    """Stream CSV langsung ke PostgreSQL tanpa load ke RAM."""
    filename  = Path(file_path).name
    encoding  = _detect_encoding(file_path)
    conn      = get_conn()
    cur       = conn.cursor()
    pq_writer = None
    pq_schema = None
    parquet_path = None

    try:
        conn.autocommit = False
        cur.execute("CREATE SCHEMA IF NOT EXISTS staging")
        conn.commit()

        # Baca header + sample untuk infer dtype
        _file_set(bulk_id, filename, pct=3, message="Membaca header…")
        sample_df = pd.read_csv(
            file_path, encoding=encoding, nrows=500,
            low_memory=False
        )
        orig_cols  = list(sample_df.columns)
        clean_cols = [sanitize_col(c) for c in orig_cols]
        sample_df.columns = clean_cols

        PG_TYPE = {
            "int64": "BIGINT", "int32": "INTEGER",
            "float64": "NUMERIC", "float32": "NUMERIC",
            "bool": "BOOLEAN", "datetime64[ns]": "TIMESTAMP",
        }
        col_types = {
            c: PG_TYPE.get(str(sample_df[c].dtype), "TEXT")
            for c in clean_cols
        }

        # Buat tabel staging
        col_defs   = ", ".join(f'"{c}" {t}' for c, t in col_types.items())
        cur.execute(f'DROP TABLE IF EXISTS staging."{table_name}"')
        cur.execute(f'CREATE TABLE staging."{table_name}" ({col_defs})')
        conn.commit()
        _file_set(bulk_id, filename, pct=5, message="Tabel staging dibuat, streaming…")

        # Siapkan Parquet path
        if save_parquet:
            os.makedirs(PARQUET_DIR, exist_ok=True)
            parquet_path = os.path.join(PARQUET_DIR, f"{table_name}.parquet")

        cols_quoted  = [f'"{c}"' for c in clean_cols]
        placeholders = ", ".join(["%s"] * len(clean_cols))
        insert_sql   = (
            f'INSERT INTO staging."{table_name}" '
            f'({", ".join(cols_quoted)}) VALUES ({placeholders})'
        )

        total_rows   = 0
        pq_buffer    = []
        pq_buf_rows  = 0
        t_start      = time.time()

        reader = pd.read_csv(
            file_path, encoding=encoding,
            chunksize=STREAM_CHUNK_ROWS,
            low_memory=False,
            dtype=str,
            on_bad_lines="skip",
        )

        for chunk_df in reader:
            chunk_df.columns = clean_cols
            chunk_df = chunk_df.where(pd.notnull(chunk_df), None)

            # Insert ke PostgreSQL
            rows = [
                tuple(None if v is None else str(v) for v in row)
                for row in chunk_df.itertuples(index=False)
            ]
            psycopg2.extras.execute_batch(cur, insert_sql, rows, page_size=5000)
            conn.commit()
            total_rows += len(chunk_df)

            # Buffer Parquet
            if save_parquet:
                pq_buffer.append(chunk_df)
                pq_buf_rows += len(chunk_df)

                if pq_buf_rows >= PARQUET_CHUNK_ROWS:
                    combined = pd.concat(pq_buffer, ignore_index=True)
                    if pq_writer is None:
                        tbl       = pa.Table.from_pandas(combined, preserve_index=False)
                        pq_schema = tbl.schema
                        pq_writer = pq.ParquetWriter(parquet_path, pq_schema, compression="snappy")
                        pq_writer.write_table(tbl)
                    else:
                        tbl = pa.Table.from_pandas(combined, schema=pq_schema)
                        pq_writer.write_table(tbl)
                    pq_buffer   = []
                    pq_buf_rows = 0

            # Progress
            elapsed = time.time() - t_start
            speed   = total_rows / max(elapsed, 1)
            pct     = min(90, 5 + int((total_rows * 100) / max(
                file_size_bytes / 50, 1  # estimasi kasar
            )))
            _file_set(bulk_id, filename,
                      pct=min(90, pct),
                      message=f"{total_rows:,} rows ({speed:,.0f} rows/s)…")

        # Flush sisa Parquet buffer
        if save_parquet and pq_buffer:
            combined = pd.concat(pq_buffer, ignore_index=True)
            if pq_writer is None:
                combined.to_parquet(parquet_path, index=False,
                                    engine="pyarrow", compression="snappy")
            else:
                tbl = pa.Table.from_pandas(combined, schema=pq_schema)
                pq_writer.write_table(tbl)

        return {
            "row_count":    total_rows,
            "col_count":    len(clean_cols),
            "parquet_path": parquet_path,
        }

    finally:
        if pq_writer:
            try: pq_writer.close()
            except Exception: pass
        cur.close()
        conn.close()


# ════════════════════════════════════════════════════════════════════════════
# STREAMING IMPORT: satu file Parquet
# ════════════════════════════════════════════════════════════════════════════

def _import_parquet_streaming(
    bulk_id:    str,
    file_path:  str,
    table_name: str,
    file_size_bytes: int,
) -> dict:
    """Baca Parquet per row-group → PostgreSQL."""
    filename = Path(file_path).name
    pf       = pq.ParquetFile(file_path)
    n_groups = pf.num_row_groups
    schema_arrow = pf.schema_arrow

    conn = get_conn()
    cur  = conn.cursor()

    try:
        conn.autocommit = False
        cur.execute("CREATE SCHEMA IF NOT EXISTS staging")
        conn.commit()

        orig_cols  = [f.name for f in schema_arrow]
        clean_cols = [sanitize_col(c) for c in orig_cols]

        PG_TYPE = {
            pa.int64():   "BIGINT",   pa.int32():   "INTEGER",
            pa.float64(): "NUMERIC",  pa.float32(): "NUMERIC",
            pa.bool_():   "BOOLEAN",
        }
        col_types = {
            clean_cols[i]: PG_TYPE.get(f.type, "TEXT")
            for i, f in enumerate(schema_arrow)
        }

        col_defs = ", ".join(f'"{c}" {t}' for c, t in col_types.items())
        cur.execute(f'DROP TABLE IF EXISTS staging."{table_name}"')
        cur.execute(f'CREATE TABLE staging."{table_name}" ({col_defs})')
        conn.commit()
        _file_set(bulk_id, filename, pct=5, message=f"Streaming {n_groups} row groups…")

        cols_quoted  = [f'"{c}"' for c in clean_cols]
        placeholders = ", ".join(["%s"] * len(clean_cols))
        insert_sql   = (
            f'INSERT INTO staging."{table_name}" '
            f'({", ".join(cols_quoted)}) VALUES ({placeholders})'
        )

        total_rows = 0
        for i in range(n_groups):
            rg_df = pf.read_row_group(i).to_pandas()
            rg_df.columns = clean_cols
            rg_df = rg_df.where(pd.notnull(rg_df), None)

            rows = [
                tuple(
                    None if (v is None or (isinstance(v, float) and pd.isna(v)))
                    else v
                    for v in row
                )
                for row in rg_df.itertuples(index=False)
            ]
            psycopg2.extras.execute_batch(cur, insert_sql, rows, page_size=5000)
            conn.commit()
            total_rows += len(rg_df)

            pct = 5 + int(((i + 1) / n_groups) * 87)
            _file_set(bulk_id, filename,
                      pct=pct,
                      message=f"Row group {i+1}/{n_groups} — {total_rows:,} rows…")

        return {
            "row_count":    total_rows,
            "col_count":    len(clean_cols),
            "parquet_path": file_path,
        }

    finally:
        cur.close()
        conn.close()


# ════════════════════════════════════════════════════════════════════════════
# IMPORT SATU FILE (dispatcher)
# ════════════════════════════════════════════════════════════════════════════

def _import_single_file(
    bulk_id:      str,
    file_info:    dict,
    save_parquet: bool,
    skip_existing: bool,
) -> None:
    """
    Import satu file. Dipanggil dari thread pool.
    Update status via _file_set().
    """
    filename   = file_info["name"]
    file_path  = file_info["path"]
    table_name = file_info["table_name"]
    ext        = file_info["ext"]
    size_bytes = file_info["size_bytes"]
    size_mb    = size_bytes / (1024 * 1024)
    is_large   = size_mb >= LARGE_FILE_MB

    try:
        # Skip jika sudah diimport dan skip_existing=True
        if skip_existing and file_info.get("already_imported"):
            _file_set(bulk_id, filename,
                      status="skipped", pct=100,
                      message=f"Dilewati — staging.{table_name} sudah ada")
            return

        _file_set(bulk_id, filename,
                  status="running", pct=2,
                  message=f"Mulai import ({file_info['size_str']}, "
                          f"mode={'streaming' if is_large else 'standard'})…")

        result = None

        # ── CSV ──────────────────────────────────────────────────────────
        if ext == "csv":
            if is_large:
                result = _import_csv_streaming(
                    bulk_id, file_path, table_name, size_bytes, save_parquet
                )
            else:
                # File kecil → pandas biasa
                df = pd.read_csv(file_path, low_memory=False)
                result = _import_df(
                    bulk_id, filename, df, table_name,
                    size_bytes, save_parquet
                )

        # ── Parquet ──────────────────────────────────────────────────────
        elif ext == "parquet":
            if is_large:
                result = _import_parquet_streaming(
                    bulk_id, file_path, table_name, size_bytes
                )
            else:
                df = pd.read_parquet(file_path, engine="pyarrow")
                result = _import_df(
                    bulk_id, filename, df, table_name,
                    size_bytes, save_parquet
                )

        # ── Excel ─────────────────────────────────────────────────────────
        elif ext in ("xlsx", "xls"):
            _file_set(bulk_id, filename, pct=5,
                      message="Membaca Excel (tidak bisa di-stream)…")
            df = pd.read_excel(file_path)
            result = _import_df(
                bulk_id, filename, df, table_name,
                size_bytes, save_parquet
            )

        else:
            _file_set(bulk_id, filename, status="skipped", pct=100,
                      message=f"Format tidak didukung: {ext}")
            return

        if result:
            # Simpan metadata ke meta.datasets
            _save_dataset_meta(
                filename=filename,
                ext=ext,
                table_name=table_name,
                row_count=result["row_count"],
                col_count=result["col_count"],
                size_bytes=size_bytes,
                parquet_path=result.get("parquet_path"),
                is_large=is_large,
            )

            _file_set(bulk_id, filename,
                      status="done", pct=100,
                      table_name=table_name,
                      row_count=result["row_count"],
                      col_count=result["col_count"],
                      parquet_path=result.get("parquet_path"),
                      message=f"Selesai — {result['row_count']:,} rows diimport")

    except Exception as e:
        tb = traceback.format_exc()
        _file_set(bulk_id, filename,
                  status="error", pct=100,
                  message=f"Error: {e}",
                  error=tb)
        print(f"[BulkImport] Error {filename}: {tb}")


def _import_df(
    bulk_id:      str,
    filename:     str,
    df:           pd.DataFrame,
    table_name:   str,
    size_bytes:   int,
    save_parquet: bool,
) -> dict:
    """Import DataFrame kecil ke PostgreSQL (path non-streaming)."""
    df.columns = [sanitize_col(c) for c in df.columns]
    df = df.where(pd.notnull(df), None)
    total_rows = len(df)

    conn = get_conn()
    cur  = conn.cursor()
    try:
        conn.autocommit = False
        cur.execute("CREATE SCHEMA IF NOT EXISTS staging")
        conn.commit()

        PG_TYPE = {
            "int64": "BIGINT", "int32": "INTEGER",
            "float64": "NUMERIC", "float32": "NUMERIC",
            "bool": "BOOLEAN", "datetime64[ns]": "TIMESTAMP",
        }
        col_defs = ", ".join(
            f'"{c}" {PG_TYPE.get(str(df[c].dtype), "TEXT")}'
            for c in df.columns
        )
        cur.execute(f'DROP TABLE IF EXISTS staging."{table_name}"')
        cur.execute(f'CREATE TABLE staging."{table_name}" ({col_defs})')
        conn.commit()

        cols_quoted  = [f'"{c}"' for c in df.columns]
        placeholders = ", ".join(["%s"] * len(df.columns))
        insert_sql   = (
            f'INSERT INTO staging."{table_name}" '
            f'({", ".join(cols_quoted)}) VALUES ({placeholders})'
        )

        for i in range(0, total_rows, STREAM_CHUNK_ROWS):
            chunk = df.iloc[i:i + STREAM_CHUNK_ROWS]
            rows  = [
                tuple(None if (v is None or (isinstance(v, float) and pd.isna(v)))
                      else v for v in row)
                for row in chunk.itertuples(index=False)
            ]
            psycopg2.extras.execute_batch(cur, insert_sql, rows, page_size=5000)
            conn.commit()
            pct = 10 + int(((i + len(chunk)) / total_rows) * 80)
            _file_set(bulk_id, filename, pct=pct,
                      message=f"Insert {i+len(chunk):,}/{total_rows:,} rows…")

    finally:
        cur.close()
        conn.close()

    parquet_path = None
    if save_parquet:
        os.makedirs(PARQUET_DIR, exist_ok=True)
        parquet_path = os.path.join(PARQUET_DIR, f"{table_name}.parquet")
        df.to_parquet(parquet_path, index=False,
                      engine="pyarrow", compression="snappy")

    return {
        "row_count":    total_rows,
        "col_count":    len(df.columns),
        "parquet_path": parquet_path,
    }


def _save_dataset_meta(
    filename:     str,
    ext:          str,
    table_name:   str,
    row_count:    int,
    col_count:    int,
    size_bytes:   int,
    parquet_path: Optional[str],
    is_large:     bool,
) -> None:
    """Simpan atau update record di meta.datasets."""
    conn = get_conn()
    cur  = conn.cursor()
    try:
        cur.execute("CREATE SCHEMA IF NOT EXISTS meta")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS meta.datasets (
                id SERIAL PRIMARY KEY, name TEXT, type TEXT,
                status TEXT DEFAULT 'pending',
                row_count INTEGER, col_count INTEGER,
                file_size TEXT, file_size_bytes BIGINT,
                table_name TEXT, parquet_path TEXT,
                is_large BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        for col, dtype in [
            ("col_count", "INTEGER"), ("file_size_bytes", "BIGINT"),
            ("parquet_path", "TEXT"), ("is_large", "BOOLEAN DEFAULT FALSE"),
        ]:
            try:
                cur.execute(
                    f"ALTER TABLE meta.datasets "
                    f"ADD COLUMN IF NOT EXISTS {col} {dtype}"
                )
            except Exception:
                pass
        conn.commit()

        # Upsert: update jika table_name sudah ada, insert jika belum
        cur.execute(
            "SELECT id FROM meta.datasets WHERE table_name = %s", (table_name,)
        )
        existing = cur.fetchone()

        if existing:
            cur.execute("""
                UPDATE meta.datasets SET
                    status='deployed', row_count=%s, col_count=%s,
                    file_size=%s, file_size_bytes=%s,
                    parquet_path=%s, is_large=%s, updated_at=NOW()
                WHERE table_name=%s
            """, (
                row_count, col_count, _fmt_size(size_bytes),
                size_bytes, parquet_path, is_large, table_name
            ))
        else:
            cur.execute("""
                INSERT INTO meta.datasets
                    (name, type, status, row_count, col_count,
                     file_size, file_size_bytes, table_name,
                     parquet_path, is_large)
                VALUES (%s, %s, 'deployed', %s, %s, %s, %s, %s, %s, %s)
            """, (
                filename, ext.upper(), row_count, col_count,
                _fmt_size(size_bytes), size_bytes,
                table_name, parquet_path, is_large
            ))
        conn.commit()

    finally:
        cur.close()
        conn.close()

# ── In-memory job store untuk single upload ──────────────────────
_jobs: dict = {}
_jobs_lock  = threading.Lock()

def _set(job_id: str, **kwargs):
    with _jobs_lock:
        if job_id not in _jobs:
            _jobs[job_id] = {}
        _jobs[job_id].update(kwargs)

def get_job(job_id: str) -> dict:
    return _jobs.get(job_id, {})


def process_upload(
    job_id: str,
    tmp_path: str,
    filename: str,
    file_size_bytes: int,
) -> None:
    """Process single file upload di background thread."""
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else "csv"
    table_name = sanitize_table(
        filename.rsplit(".", 1)[0] if "." in filename else filename
    )
    size_mb  = file_size_bytes / (1024 * 1024)
    is_large = size_mb >= LARGE_FILE_MB

    try:
        _set(job_id, status="processing", pct=5,
             message="Membaca file…", is_large=is_large)

        if ext == "csv":
            if is_large:
                result = _import_csv_streaming(
                    job_id, tmp_path, table_name, file_size_bytes,
                    save_parquet=True
                )
            else:
                df = pd.read_csv(tmp_path, low_memory=False)
                result = _import_df(
                    job_id, filename, df, table_name,
                    file_size_bytes, save_parquet=False
                )

        elif ext in ("xlsx", "xls"):
            _set(job_id, pct=10, message="Membaca Excel…")
            df = pd.read_excel(tmp_path)
            result = _import_df(
                job_id, filename, df, table_name,
                file_size_bytes, save_parquet=False
            )

        elif ext == "parquet":
            if is_large:
                result = _import_parquet_streaming(
                    job_id, tmp_path, table_name, file_size_bytes
                )
            else:
                df = pd.read_parquet(tmp_path, engine="pyarrow")
                result = _import_df(
                    job_id, filename, df, table_name,
                    file_size_bytes, save_parquet=False
                )
        else:
            _set(job_id, status="error", pct=100,
                 message=f"Format tidak didukung: {ext}")
            return

        _save_dataset_meta(
            filename=filename, ext=ext, table_name=table_name,
            row_count=result["row_count"], col_count=result["col_count"],
            size_bytes=file_size_bytes,
            parquet_path=result.get("parquet_path"),
            is_large=is_large,
        )

        _set(job_id,
             status="done", pct=100,
             message=f"Selesai — {result['row_count']:,} rows",
             row_count=result["row_count"],
             col_count=result["col_count"],
             table_name=table_name,
             is_large=is_large,
             parquet_path=result.get("parquet_path"))

    except Exception as e:
        _set(job_id, status="error", pct=100,
             message=str(e), error=traceback.format_exc())
        print(f"[Upload] Error {filename}: {traceback.format_exc()}")
    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass

# ════════════════════════════════════════════════════════════════════════════
# BULK IMPORT: semua file dari /data_csv
# ════════════════════════════════════════════════════════════════════════════

def process_bulk_import(
    bulk_id:       str,
    directory:     str     = DATA_DIR,
    recursive:     bool    = False,
    save_parquet:  bool    = True,
    skip_existing: bool    = True,
    file_filter:   Optional[list] = None,  # None = semua file
) -> None:
    """
    Import SEMUA file dari direktori secara paralel.

    Parameters
    ----------
    bulk_id       : ID unik untuk tracking
    directory     : direktori sumber (default: /data_csv)
    recursive     : masuk subfolder atau tidak
    save_parquet  : simpan hasil sebagai Parquet juga
    skip_existing : lewati file yang sudah ada di staging
    file_filter   : list nama file yang mau diimport
                    (None = semua file di direktori)
    """
    try:
        _bulk_set(bulk_id, status="scanning",
                  message="Scanning direktori…", progress_pct=0)

        # 1. Scan semua file
        all_files = scan_all_files(directory, recursive)

        if not all_files:
            _bulk_set(bulk_id,
                      status="done",
                      message="Tidak ada file yang ditemukan di direktori.",
                      progress_pct=100,
                      files_total=0,
                      files_done=0)
            return

        # 2. Filter jika ada file_filter
        if file_filter:
            all_files = [f for f in all_files if f["name"] in file_filter]

        total = len(all_files)
        _bulk_set(bulk_id,
                  status="running",
                  message=f"Ditemukan {total} file. Mulai import…",
                  files_total=total,
                  files_done=0,
                  progress_pct=0,
                  files={f["name"]: {"status": "pending", "pct": 0} for f in all_files})

        # 3. Import paralel dengan semaphore (max MAX_PARALLEL bersamaan)
        semaphore = threading.Semaphore(MAX_PARALLEL)
        threads   = []

        def _worker(file_info):
            with semaphore:
                _import_single_file(
                    bulk_id, file_info, save_parquet, skip_existing
                )

        for file_info in all_files:
            t = threading.Thread(target=_worker, args=(file_info,), daemon=True)
            threads.append(t)
            t.start()

        # 4. Tunggu semua selesai
        for t in threads:
            t.join()

        # 5. Hitung summary
        with _bulk_lock:
            files_state = _bulk_jobs[bulk_id].get("files", {})

        done_count    = sum(1 for f in files_state.values() if f["status"] == "done")
        error_count   = sum(1 for f in files_state.values() if f["status"] == "error")
        skipped_count = sum(1 for f in files_state.values() if f["status"] == "skipped")
        total_rows    = sum(f.get("row_count", 0) for f in files_state.values())

        _bulk_set(bulk_id,
                  status="done",
                  overall_status="done",
                  progress_pct=100,
                  files_done=total,
                  message=(
                      f"Selesai! {done_count} berhasil, "
                      f"{skipped_count} dilewati, "
                      f"{error_count} error. "
                      f"Total: {total_rows:,} rows diimport."
                  ),
                  summary={
                      "total":   total,
                      "done":    done_count,
                      "skipped": skipped_count,
                      "error":   error_count,
                      "total_rows_imported": total_rows,
                  })

    except Exception as e:
        tb = traceback.format_exc()
        _bulk_set(bulk_id,
                  status="error",
                  message=f"Bulk import gagal: {e}",
                  error=tb)
        print(f"[BulkImport] Fatal error: {tb}")