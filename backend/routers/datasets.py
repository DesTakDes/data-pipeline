"""
routers.datasets
────────────────────
Pure HTTP layer for dataset upload/listing/deletion. Business logic lives
in datasets/upload_service.py.
"""
import shutil
from pathlib import Path

from fastapi import APIRouter, HTTPException, UploadFile, File
from core.db import get_conn
from core.config import UPLOAD_DIR
from datasets.upload_service import UploadService

router = APIRouter()
service = UploadService(pg_conn_factory=get_conn)


@router.get("/datasets")
def list_datasets():
    return service.list_datasets()


@router.post("/datasets/upload")
async def upload_dataset(file: UploadFile = File(...)):
    dest_dir = Path(UPLOAD_DIR)
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / file.filename
    try:
        with dest_path.open("wb") as out:
            shutil.copyfileobj(file.file, out)
        result = service.ingest_file(str(dest_path), display_name=file.filename)
        return result
    except Exception as e:
        raise HTTPException(500, detail=f"Upload failed: {e}")


@router.delete("/datasets/{table_name:path}")
def delete_dataset(table_name: str):
    try:
        service.delete_dataset(table_name)
        return {"deleted": table_name}
    except Exception as e:
        raise HTTPException(500, detail=f"Delete failed: {e}")