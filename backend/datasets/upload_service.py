"""
datasets.upload_service
───────────────────────────
Wraps the existing `upload_worker.py` (kept at repo root, unchanged) so
routers/datasets.py only deals with request/response shape and never
touches file parsing, chunking, or COPY-into-Postgres logic directly.
"""
import re
import time
from pathlib import Path

import upload_worker  # existing module at backend/upload_worker.py, unchanged
from core.config import UPLOAD_DIR
from .repository import DatasetRepository


def _safe_table_name(filename: str) -> str:
    stem = Path(filename).stem
    safe = re.sub(r"[^a-z0-9_]", "_", stem.lower()).strip("_") or "dataset"
    if safe[0].isdigit():
        safe = f"t_{safe}"
    return f"{safe}_{int(time.time())}"


class UploadService:
    def __init__(self, pg_conn_factory):
        self.pg_conn_factory = pg_conn_factory
        self.repo = DatasetRepository(pg_conn_factory)

    def ingest_file(self, file_path: str, display_name: str | None = None) -> dict:
        """
        Delegates the actual CSV/Excel -> staging.{table} load to
        upload_worker.load_to_postgres (existing, battle-tested chunked
        loader) and just handles bookkeeping around it.
        """
        table_name = _safe_table_name(Path(file_path).name)
        result = upload_worker.load_to_postgres(
            file_path=file_path,
            pg_conn_factory=self.pg_conn_factory,
            target_schema="staging",
            target_table=table_name,
        )
        dataset_id = self.repo.register(
            name=display_name or Path(file_path).name,
            table_name=f"staging.{table_name}",
            source_file=file_path,
            row_count=result.get("rows", 0),
            size_mb=result.get("size_mb", 0.0),
        )
        return {
            "dataset_id": dataset_id,
            "table_name": f"staging.{table_name}",
            "rows": result.get("rows", 0),
            "columns": result.get("columns", []),
        }

    def list_datasets(self) -> list[dict]:
        return self.repo.list_all()

    def delete_dataset(self, table_name: str) -> None:
        schema, tname = table_name.split(".", 1) if "." in table_name else ("staging", table_name)
        conn = self.pg_conn_factory()
        try:
            cur = conn.cursor()
            cur.execute(f'DROP TABLE IF EXISTS {schema}."{tname}"')
            conn.commit()
            cur.close()
        finally:
            conn.close()
        self.repo.delete(table_name)