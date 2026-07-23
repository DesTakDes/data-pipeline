"""
datasets.parquet_service
────────────────────────────
Small helper used by both the upload flow (optional parquet snapshot for
fast repeated preview reads) and the DuckDB engine tier (transform_lib
.duckdb_executor reads Postgres directly, but very large staging tables
benefit from a one-time parquet materialization here to avoid repeated
full-table JDBC/psycopg2 scans).
"""
import os
from pathlib import Path

import pandas as pd
import psycopg2

from core.config import PARQUET_DIR


def save_dataframe_to_parquet(df: pd.DataFrame, table_name: str, parquet_dir: str = PARQUET_DIR) -> str:
    safe_name = table_name.replace(".", "__")
    out_dir = Path(parquet_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{safe_name}.parquet"
    df.to_parquet(out_path, index=False)
    return str(out_path)


def table_to_parquet(pg_conn_factory, table_name: str, parquet_dir: str = PARQUET_DIR,
                      chunk_size: int = 200_000) -> str:
    """Streams a (possibly large) Postgres table to a single parquet file in
    chunks, so this never has to hold the full table in memory at once."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    conn = pg_conn_factory()
    safe_name = table_name.replace(".", "__")
    out_dir = Path(parquet_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{safe_name}.parquet"

    writer = None
    try:
        for chunk in pd.read_sql(f"SELECT * FROM {table_name}", conn, chunksize=chunk_size):
            table = pa.Table.from_pandas(chunk, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(out_path, table.schema)
            writer.write_table(table)
    finally:
        if writer is not None:
            writer.close()
        conn.close()
    return str(out_path)


def list_parquet_files(parquet_dir: str = PARQUET_DIR) -> list[dict]:
    out_dir = Path(parquet_dir)
    if not out_dir.exists():
        return []
    files = []
    for p in sorted(out_dir.glob("*.parquet")):
        stat = p.stat()
        files.append({
            "file_name": p.name,
            "path": str(p),
            "size_mb": round(stat.st_size / 1024 / 1024, 3),
            "modified_at": stat.st_mtime,
        })
    return files


def delete_parquet(table_name: str, parquet_dir: str = PARQUET_DIR) -> bool:
    safe_name = table_name.replace(".", "__")
    path = Path(parquet_dir) / f"{safe_name}.parquet"
    if path.exists():
        os.remove(path)
        return True
    return False