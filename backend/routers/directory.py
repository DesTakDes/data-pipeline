"""
routers.directory
─────────────────────
Bulk import: point at a server-side directory (e.g. a mounted network
share or the upload folder) and ingest every CSV/Excel file in it in one
call, reusing datasets/upload_service.py per file so there's exactly one
ingestion code path whether a file arrives via drag-and-drop or bulk scan.
"""
from pathlib import Path

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from core.db import get_conn
from core.config import UPLOAD_DIR
from datasets.upload_service import UploadService

router = APIRouter()
service = UploadService(pg_conn_factory=get_conn)

SUPPORTED_EXTENSIONS = {".csv", ".tsv", ".xlsx", ".xls"}


class BulkImportRequest(BaseModel):
    directory: str = UPLOAD_DIR
    recursive: bool = False


@router.get("/directory/scan")
def scan_directory(directory: str = UPLOAD_DIR, recursive: bool = False):
    """Lists importable files without ingesting them, so the frontend can
    show a preview/checklist before the user confirms bulk import."""
    base = Path(directory)
    if not base.exists():
        raise HTTPException(404, detail=f"Directory '{directory}' does not exist")
    glob_fn = base.rglob if recursive else base.glob
    files = [
        {"name": p.name, "path": str(p), "size_mb": round(p.stat().st_size / 1024 / 1024, 3)}
        for p in glob_fn("*") if p.is_file() and p.suffix.lower() in SUPPORTED_EXTENSIONS
    ]
    return {"directory": directory, "files": files}


@router.post("/directory/import")
def bulk_import(payload: BulkImportRequest):
    base = Path(payload.directory)
    if not base.exists():
        raise HTTPException(404, detail=f"Directory '{payload.directory}' does not exist")

    glob_fn = base.rglob if payload.recursive else base.glob
    targets = [p for p in glob_fn("*") if p.is_file() and p.suffix.lower() in SUPPORTED_EXTENSIONS]

    results, errors = [], []
    for path in targets:
        try:
            result = service.ingest_file(str(path), display_name=path.name)
            results.append({"file": path.name, **result})
        except Exception as e:
            errors.append({"file": path.name, "error": str(e)})

    return {"imported": results, "errors": errors, "total_found": len(targets)}