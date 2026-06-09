"""
Background upload worker untuk large file processing.
Dijalankan sebagai background task oleh FastAPI.

Perubahan:
- Tambah kemampuan baca data dari direktori file (CSV/Parquet) tanpa PostgreSQL
- Tambah fungsi export Snappy Parquet (save_as_snappy_parquet)
- process_from_directory: pipeline baca direktori → opsional simpan ke DB
"""
import os, re, io, json, time, traceback
import pandas as pd
import psycopg2
import psycopg2.extras
from pathlib import Path
from typing import Optional

PARQUET_THRESHOLD_GB = 5.0      # >= 5 GB → convert to parquet
PARQUET_DIR          = "/data_csv/parquet"
DATA_DIR             = "/data_csv"          # ← direktori default untuk scan file
CHUNK_SIZE           = 50_000              # rows per DB insert batch

PG_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "postgres"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "database": os.getenv("POSTGRES_DB", "airflow"),
    "user":     os.getenv("POSTGRES_USER", "airflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
}

# In-memory job status store (resets on restart — OK for our use case)
# Structure: { job_id: { status, pct, message, dataset_id, error } }
_jobs: dict = {}

def get_job(job_id: str) -> dict:
    return _jobs.get(job_id, {})

def _set(job_id: str, **kwargs):
    if job_id not in _jobs:
        _jobs[job_id] = {}
    _jobs[job_id].update(kwargs)

def get_conn():
    return psycopg2.connect(**PG_CONFIG)

def sanitize_col(c: str) -> str:
    return re.sub(r'[^a-z0-9_]', '_',
           c.strip().lower()
            .replace(" ", "_").replace("-", "_").replace(".", "_"))

def sanitize_table(name: str) -> str:
    t = re.sub(r'[^a-z0-9_]', '_', name.lower())
    return re.sub(r'_+', '_', t).strip('_')


# ══════════════════════════════════════════════════════════════════════════
# PARQUET UTILITIES
# ══════════════════════════════════════════════════════════════════════════

def save_as_snappy_parquet(
    df: pd.DataFrame,
    output_name: str,
    output_dir: str = PARQUET_DIR,
) -> Optional[str]:
    """
    Simpan DataFrame ke file Parquet dengan kompresi Snappy.

    Parameters
    ----------
    df          : DataFrame yang akan disimpan
    output_name : Nama file (tanpa ekstensi), e.g. "sales_2024"
    output_dir  : Direktori tujuan (default: PARQUET_DIR)

    Returns
    -------
    str  : path absolut file yang berhasil disimpan
    None : jika gagal (pyarrow tidak tersedia, dll)
    """
    os.makedirs(output_dir, exist_ok=True)
    parquet_path = os.path.join(output_dir, f"{output_name}.parquet")
    try:
        df.to_parquet(
            parquet_path,
            index=False,
            engine="pyarrow",
            compression="snappy",   # ← Snappy: balance speed vs ratio
        )
        size_mb = os.path.getsize(parquet_path) / (1024 ** 2)
        print(f"[Parquet] Saved → {parquet_path} ({size_mb:.2f} MB, snappy)")
        return parquet_path
    except ImportError:
        # Coba fastparquet sebagai fallback
        try:
            df.to_parquet(parquet_path, index=False, engine="fastparquet",
                          compression="snappy")
            return parquet_path
        except Exception as e2:
            print(f"[Parquet] Both engines failed: {e2}")
            return None
    except Exception as e:
        print(f"[Parquet] Save failed: {e}")
        return None


def read_parquet_from_dir(file_path: str) -> pd.DataFrame:
    """
    Baca file Parquet dari path absolut.
    Mendukung single-file maupun direktori partisi Hive-style.
    """
    p = Path(file_path)
    if p.is_dir():
        # Partitioned parquet (e.g., spark output)
        parts = list(p.glob("**/*.parquet"))
        if not parts:
            raise FileNotFoundError(f"No .parquet files in {file_path}")
        dfs = [pd.read_parquet(str(f), engine="pyarrow") for f in sorted(parts)]
        return pd.concat(dfs, ignore_index=True)
    else:
        return pd.read_parquet(file_path, engine="pyarrow")


# ══════════════════════════════════════════════════════════════════════════
# DIRECTORY SCAN — baca file tanpa harus melalui PostgreSQL
# ══════════════════════════════════════════════════════════════════════════

def list_directory_files(
    directory: str = DATA_DIR,
    extensions: tuple = (".csv", ".parquet", ".xlsx", ".xls"),
) -> list[dict]:
    """
    Scan direktori dan kembalikan daftar file yang didukung.

    Returns
    -------
    list of dict:
        { name, path, size_bytes, size_str, ext, modified_at }
    """
    result = []
    base = Path(directory)
    if not base.exists():
        return result

    for p in sorted(base.rglob("*")):
        if p.is_file() and p.suffix.lower() in extensions:
            size = p.stat().st_size
            result.append({
                "name":        p.name,
                "path":        str(p),
                "size_bytes":  size,
                "size_str":    _fmt_size(size),
                "ext":         p.suffix.lower().lstrip("."),
                "modified_at": p.stat().st_mtime,
            })
    return result


def read_file_from_path(file_path: str) -> pd.DataFrame:
    """
    Baca CSV / Parquet / Excel langsung dari path filesystem.
    Tidak memerlukan koneksi PostgreSQL.
    """
    p = Path(file_path)
    if not p.exists():
        raise FileNotFoundError(f"File tidak ditemukan: {file_path}")

    ext = p.suffix.lower()

    if ext == ".parquet" or p.is_dir():
        return read_parquet_from_dir(file_path)

    elif ext == ".csv":
        size_gb = p.stat().st_size / (1024 ** 3)
        if size_gb >= PARQUET_THRESHOLD_GB:
            # Streaming besar
            enc = _detect_encoding(str(p))
            chunks = pd.read_csv(str(p), encoding=enc, chunksize=100_000, low_memory=False)
            return pd.concat(list(chunks), ignore_index=True)
        else:
            try:
                return pd.read_csv(str(p), encoding="utf-8", low_memory=False)
            except UnicodeDecodeError:
                return pd.read_csv(str(p), encoding="latin-1", low_memory=False)

    elif ext in (".xlsx", ".xls"):
        return pd.read_excel(str(p))

    else:
        raise ValueError(f"Format tidak didukung: {ext}")


def _detect_encoding(path: str) -> str:
    with open(path, "rb") as f:
        sample = f.read(4096)
    try:
        sample.decode("utf-8")
        return "utf-8"
    except UnicodeDecodeError:
        return "latin-1"


def _fmt_size(size_bytes: int) -> str:
    kb = size_bytes / 1024
    if kb < 1024:
        return f"{kb:.1f} KB"
    elif kb < 1024 * 1024:
        return f"{kb/1024:.1f} MB"
    else:
        return f"{kb/1024/1024:.2f} GB"


# ══════════════════════════════════════════════════════════════════════════
# DIRECTORY-BASED PIPELINE PROCESS
# Baca file dari direktori → opsional simpan ke DB
# ══════════════════════════════════════════════════════════════════════════

def process_from_directory(
    job_id: str,
    file_path: str,
    save_to_db: bool = True,
    save_parquet: bool = True,
):
    """
    Pipeline alternatif: baca file dari direktori filesystem,
    tanpa harus upload ulang.

    Parameters
    ----------
    job_id      : ID job untuk tracking progress
    file_path   : Path absolut ke file (CSV/Parquet/Excel)
    save_to_db  : Jika True, insert ke staging PostgreSQL
    save_parquet: Jika True, simpan Snappy Parquet ke PARQUET_DIR
    """
    try:
        p = Path(file_path)
        filename = p.name
        file_size_bytes = p.stat().st_size if p.is_file() else 0

        _set(job_id, status="reading", pct=5,
             message=f"Membaca file dari direktori: {file_path}")

        # ── 1. Baca file ──────────────────────────────────────────────
        df = read_file_from_path(file_path)
        _set(job_id, pct=25,
             message=f"File dibaca: {len(df):,} rows × {len(df.columns)} cols")

        # ── 2. Sanitize ───────────────────────────────────────────────
        df.columns = [sanitize_col(c) for c in df.columns]
        df = df.where(pd.notnull(df), None)

        base_name  = p.stem
        table_name = sanitize_table(base_name)
        ext        = p.suffix.lower().lstrip(".") or "csv"
        total_rows = len(df)
        file_size_gb = file_size_bytes / (1024 ** 3)
        is_large = file_size_gb >= PARQUET_THRESHOLD_GB

        # ── 3. Simpan Snappy Parquet (opsional) ───────────────────────
        parquet_path = None
        if save_parquet:
            _set(job_id, pct=35, message="Menyimpan Snappy Parquet…")
            parquet_path = save_as_snappy_parquet(df, table_name)
            if parquet_path:
                _set(job_id, pct=45,
                     message=f"Parquet tersimpan → {parquet_path}")
            else:
                _set(job_id, pct=40,
                     message="Parquet dilewati (pyarrow tidak tersedia)")

        # ── 4. Insert ke DB (opsional) ────────────────────────────────
        dataset_id = None
        if save_to_db:
            _set(job_id, pct=50, message="Membuat tabel database…")
            dataset_id = _insert_to_db(
                job_id, df, table_name, filename, ext,
                file_size_bytes, parquet_path, is_large,
            )
        else:
            _set(job_id, pct=95,
                 message="Skip DB (save_to_db=False), hanya simpan Parquet")

        size_str = _fmt_size(file_size_bytes)
        _set(job_id,
             status="done", pct=100,
             message=f"Selesai! {total_rows:,} rows diproses.",
             dataset_id=dataset_id,
             table_name=table_name,
             row_count=total_rows,
             col_count=len(df.columns),
             file_size=size_str,
             is_large=is_large,
             parquet_path=parquet_path,
             source="directory")

    except Exception as e:
        tb = traceback.format_exc()
        _set(job_id, status="error", pct=100,
             message=f"Error: {e}", error=tb)


# ══════════════════════════════════════════════════════════════════════════
# ORIGINAL UPLOAD PROCESS (tetap dipertahankan)
# ══════════════════════════════════════════════════════════════════════════

def process_upload(job_id: str, tmp_path: str, filename: str, file_size_bytes: int):
    """
    Background worker: parse → optional parquet → insert DB.
    Updates _jobs[job_id] with progress.
    """
    try:
        _set(job_id, status="parsing", pct=5, message="Membaca file…")
        ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else "csv"
        file_size_gb = file_size_bytes / (1024 ** 3)
        is_large = file_size_gb >= PARQUET_THRESHOLD_GB

        # ── 1. Read file ─────────────────────────────────────────────────
        _set(job_id, pct=10,
             message=f"Parsing {filename} ({file_size_gb:.2f} GB)…")

        if ext == "csv":
            if is_large:
                _set(job_id, pct=12, message="File besar: streaming CSV chunks…")
                enc = _detect_encoding(tmp_path)
                reader = pd.read_csv(tmp_path, encoding=enc,
                                     chunksize=100_000, low_memory=False)
                df = pd.concat(list(reader), ignore_index=True)
            else:
                try:
                    df = pd.read_csv(tmp_path, encoding="utf-8", low_memory=False)
                except UnicodeDecodeError:
                    df = pd.read_csv(tmp_path, encoding="latin-1", low_memory=False)
        elif ext in ("xlsx", "xls"):
            df = pd.read_excel(tmp_path)
        elif ext == "parquet":
            df = read_parquet_from_dir(tmp_path)
        else:
            _set(job_id, status="error", pct=100,
                 message="Tipe file tidak didukung",
                 error="Only CSV, Excel, and Parquet supported")
            return

        _set(job_id, pct=25,
             message=f"Parsed {len(df):,} rows × {len(df.columns)} cols")

        # ── 2. Sanitize ───────────────────────────────────────────────────
        df.columns = [sanitize_col(c) for c in df.columns]
        df = df.where(pd.notnull(df), None)

        base_name  = filename.rsplit(".", 1)[0]
        table_name = sanitize_table(base_name)

        # ── 3. Convert to Snappy Parquet if large ─────────────────────────
        parquet_path = None
        if is_large:
            _set(job_id, pct=35,
                 message="Mengkonversi ke Snappy Parquet (file besar)…")
            parquet_path = save_as_snappy_parquet(df, table_name)
            if parquet_path:
                _set(job_id, pct=45,
                     message=f"Parquet tersimpan → {parquet_path}")
            else:
                _set(job_id, pct=40,
                     message="Parquet dilewati (engine tidak tersedia)")

        # ── 4. Create staging table & insert in batches ───────────────────
        dataset_id = _insert_to_db(
            job_id, df, table_name, filename, ext,
            file_size_bytes, parquet_path, is_large,
        )

        size_str = _fmt_size(file_size_bytes)
        _set(job_id,
             status="done", pct=100,
             message=f"Selesai! {len(df):,} rows diinsert.",
             dataset_id=dataset_id,
             table_name=table_name,
             row_count=len(df),
             col_count=len(df.columns),
             file_size=size_str,
             is_large=is_large,
             parquet_path=parquet_path,
             source="upload")

    except Exception as e:
        tb = traceback.format_exc()
        _set(job_id, status="error", pct=100,
             message=f"Error: {e}", error=tb)
    finally:
        try:
            os.unlink(tmp_path)
        except:
            pass


# ══════════════════════════════════════════════════════════════════════════
# SHARED: Insert DataFrame ke staging DB
# ══════════════════════════════════════════════════════════════════════════

def _insert_to_db(
    job_id: str,
    df: pd.DataFrame,
    table_name: str,
    filename: str,
    ext: str,
    file_size_bytes: int,
    parquet_path: Optional[str],
    is_large: bool,
) -> int:
    """
    Insert DataFrame ke staging PostgreSQL dan catat ke meta.datasets.
    Dipakai bersama oleh process_upload dan process_from_directory.

    Returns
    -------
    int : dataset_id dari meta.datasets
    """
    _set(job_id, pct=50, message="Membuat tabel database…")

    conn = get_conn()
    cur  = conn.cursor()
    total_rows = len(df)

    cur.execute("CREATE SCHEMA IF NOT EXISTS meta")
    cur.execute("CREATE SCHEMA IF NOT EXISTS staging")
    conn.commit()

    # Ensure meta.datasets
    cur.execute("""
        CREATE TABLE IF NOT EXISTS meta.datasets (
            id SERIAL PRIMARY KEY, name TEXT, type TEXT,
            status TEXT DEFAULT 'pending', row_count INTEGER,
            col_count INTEGER, file_size TEXT, file_size_bytes BIGINT,
            table_name TEXT, parquet_path TEXT,
            is_large BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """)
    for col, dtype in [("col_count", "INTEGER"), ("file_size_bytes", "BIGINT"),
                       ("parquet_path", "TEXT"), ("is_large", "BOOLEAN DEFAULT FALSE")]:
        try:
            cur.execute(
                f"ALTER TABLE meta.datasets ADD COLUMN IF NOT EXISTS {col} {dtype}")
        except:
            pass
    conn.commit()

    # Drop + create staging table
    type_map = {
        "int64": "BIGINT", "int32": "INTEGER",
        "float64": "NUMERIC", "float32": "NUMERIC",
        "bool": "BOOLEAN",
    }
    col_defs = ", ".join([
        f'"{c}" {type_map.get(str(df[c].dtype), "TEXT")}'
        for c in df.columns
    ])
    cur.execute(f'DROP TABLE IF EXISTS staging."{table_name}"')
    cur.execute(f'CREATE TABLE staging."{table_name}" ({col_defs})')
    conn.commit()

    # ── Insert in batches with progress ───────────────────────────────
    cols_quoted  = [f'"{c}"' for c in df.columns]
    placeholders = ", ".join(["%s"] * len(df.columns))
    insert_sql   = (
        f'INSERT INTO staging."{table_name}" ({", ".join(cols_quoted)}) '
        f'VALUES ({placeholders})'
    )

    inserted = 0
    for chunk_start in range(0, total_rows, CHUNK_SIZE):
        chunk = df.iloc[chunk_start: chunk_start + CHUNK_SIZE]
        rows = [
            tuple(
                None if (v is None or (isinstance(v, float) and pd.isna(v))) else v
                for v in row
            )
            for row in chunk.itertuples(index=False)
        ]
        psycopg2.extras.execute_batch(cur, insert_sql, rows, page_size=2000)
        conn.commit()
        inserted += len(chunk)
        pct = 50 + int((inserted / total_rows) * 45)
        _set(job_id, pct=pct,
             message=f"Inserting rows… {inserted:,}/{total_rows:,}")

    # ── Save meta record ───────────────────────────────────────────────
    _set(job_id, pct=97, message="Finalisasi…")

    size_str = _fmt_size(file_size_bytes)
    file_size_gb = file_size_bytes / (1024 ** 3)

    cur.execute("""
        INSERT INTO meta.datasets
            (name, type, status, row_count, col_count,
             file_size, file_size_bytes, table_name, parquet_path, is_large)
        VALUES (%s, %s, 'deployed', %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
    """, (filename, ext.upper(), total_rows, len(df.columns),
          size_str, file_size_bytes, table_name, parquet_path, is_large))

    dataset_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return dataset_id