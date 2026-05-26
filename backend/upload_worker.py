"""
Background upload worker untuk large file processing.
Dijalankan sebagai background task oleh FastAPI.
"""
import os, re, io, json, time, traceback
import pandas as pd
import psycopg2
import psycopg2.extras
from pathlib import Path

PARQUET_THRESHOLD_GB = 5.0   # >= 5 GB → convert to parquet
PARQUET_DIR          = "/data_csv/parquet"
CHUNK_SIZE           = 50_000  # rows per DB insert batch

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

def process_upload(job_id: str, tmp_path: str, filename: str, file_size_bytes: int):
    """
    Background worker: parse → optional parquet → insert DB.
    Updates _jobs[job_id] with progress.
    """
    try:
        _set(job_id, status="parsing", pct=5, message="Reading file…")
        ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else "csv"
        file_size_gb = file_size_bytes / (1024 ** 3)
        is_large = file_size_gb >= PARQUET_THRESHOLD_GB

        # ── 1. Read file ─────────────────────────────────────────────────
        _set(job_id, pct=10, message=f"Parsing {filename} ({file_size_gb:.2f} GB)…")

        if ext == "csv":
            # For large CSVs use chunked reading
            if is_large:
                _set(job_id, pct=12, message="Large file: streaming CSV chunks…")
                # Read header first
                with open(tmp_path, "rb") as f:
                    sample = f.read(4096)
                try:
                    sample.decode("utf-8")
                    enc = "utf-8"
                except:
                    enc = "latin-1"
                reader = pd.read_csv(tmp_path, encoding=enc, chunksize=100_000, low_memory=False)
                chunks = list(reader)
                df = pd.concat(chunks, ignore_index=True)
            else:
                try:
                    df = pd.read_csv(tmp_path, encoding="utf-8", low_memory=False)
                except UnicodeDecodeError:
                    df = pd.read_csv(tmp_path, encoding="latin-1", low_memory=False)
        elif ext in ("xlsx", "xls"):
            df = pd.read_excel(tmp_path)
        else:
            _set(job_id, status="error", pct=100, message="Unsupported file type", error="Only CSV and Excel supported")
            return

        _set(job_id, pct=25, message=f"Parsed {len(df):,} rows × {len(df.columns)} cols")

        # ── 2. Sanitize ───────────────────────────────────────────────────
        df.columns = [sanitize_col(c) for c in df.columns]
        # Replace NaN with None for DB compat
        df = df.where(pd.notnull(df), None)

        base_name  = filename.rsplit(".", 1)[0]
        table_name = sanitize_table(base_name)

        # ── 3. Convert to Parquet if large ────────────────────────────────
        parquet_path = None
        if is_large:
            _set(job_id, pct=35, message="Converting to Parquet (large file)…")
            os.makedirs(PARQUET_DIR, exist_ok=True)
            parquet_path = f"{PARQUET_DIR}/{table_name}.parquet"
            try:
                df.to_parquet(parquet_path, index=False, engine="pyarrow",
                              compression="snappy")  # snappy = fast compress
                _set(job_id, pct=45, message=f"Parquet saved → {parquet_path}")
            except Exception as e:
                # pyarrow not available? fallback to fastparquet or skip
                parquet_path = None
                _set(job_id, pct=40, message=f"Parquet conversion skipped: {e}")

        # ── 4. Create staging table & insert in batches ───────────────────
        _set(job_id, pct=50, message="Creating database table…")

        conn = get_conn()
        cur  = conn.cursor()

        # Schemas
        cur.execute("CREATE SCHEMA IF NOT EXISTS meta")
        cur.execute("CREATE SCHEMA IF NOT EXISTS staging")
        conn.commit()

        # Ensure meta.datasets
        cur.execute("""
            CREATE TABLE IF NOT EXISTS meta.datasets (
                id SERIAL PRIMARY KEY, name TEXT, type TEXT,
                status TEXT DEFAULT 'pending', row_count INTEGER,
                col_count INTEGER, file_size TEXT, file_size_bytes BIGINT,
                table_name TEXT, parquet_path TEXT, is_large BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW(), updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        for col, dtype in [("col_count","INTEGER"),("file_size_bytes","BIGINT"),
                           ("parquet_path","TEXT"),("is_large","BOOLEAN DEFAULT FALSE")]:
            try:
                cur.execute(f"ALTER TABLE meta.datasets ADD COLUMN IF NOT EXISTS {col} {dtype}")
            except:
                pass
        conn.commit()

        # Drop + create staging table
        type_map = {
            "int64": "BIGINT", "int32": "INTEGER", "float64": "NUMERIC",
            "float32": "NUMERIC", "bool": "BOOLEAN",
        }
        col_defs = ", ".join([
            f'"{c}" {type_map.get(str(df[c].dtype), "TEXT")}'
            for c in df.columns
        ])
        cur.execute(f'DROP TABLE IF EXISTS staging."{table_name}"')
        cur.execute(f'CREATE TABLE staging."{table_name}" ({col_defs})')
        conn.commit()

        # ── Insert in batches with progress ───────────────────────────────
        total_rows = len(df)
        cols_quoted = [f'"{c}"' for c in df.columns]
        placeholders = ", ".join(["%s"] * len(df.columns))
        insert_sql = (
            f'INSERT INTO staging."{table_name}" ({", ".join(cols_quoted)}) '
            f'VALUES ({placeholders})'
        )

        inserted = 0
        for chunk_start in range(0, total_rows, CHUNK_SIZE):
            chunk = df.iloc[chunk_start : chunk_start + CHUNK_SIZE]
            rows = [
                tuple(None if (v is None or (isinstance(v, float) and pd.isna(v))) else v
                      for v in row)
                for row in chunk.itertuples(index=False)
            ]
            psycopg2.extras.execute_batch(cur, insert_sql, rows, page_size=2000)
            conn.commit()
            inserted += len(chunk)
            pct = 50 + int((inserted / total_rows) * 45)
            _set(job_id, pct=pct,
                 message=f"Inserting rows… {inserted:,}/{total_rows:,}")

        # ── 5. Save meta record ────────────────────────────────────────────
        _set(job_id, pct=97, message="Finalizing…")

        size_kb = file_size_bytes / 1024
        if size_kb < 1024:
            size_str = f"{size_kb:.1f} KB"
        elif size_kb < 1024 * 1024:
            size_str = f"{size_kb/1024:.1f} MB"
        else:
            size_str = f"{file_size_gb:.2f} GB"

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

        _set(job_id,
             status="done", pct=100,
             message=f"Done! {total_rows:,} rows inserted.",
             dataset_id=dataset_id,
             table_name=table_name,
             row_count=total_rows,
             col_count=len(df.columns),
             file_size=size_str,
             is_large=is_large,
             parquet_path=parquet_path)

    except Exception as e:
        tb = traceback.format_exc()
        _set(job_id, status="error", pct=100,
             message=f"Error: {e}", error=tb)
    finally:
        # Clean up tmp file
        try:
            os.unlink(tmp_path)
        except:
            pass