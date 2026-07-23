"""
routers.warehouse
─────────────────────
Pure HTTP layer for browsing/downloading the outputs of "Run Pipeline".
Business logic lives in warehouse/service.py.
"""
from fastapi import APIRouter, HTTPException
from fastapi.responses import PlainTextResponse
from core.db import get_conn
from warehouse.service import WarehouseService

router = APIRouter()
service = WarehouseService(pg_conn_factory=get_conn)


@router.get("/warehouse/tables")
def list_tables():
    return service.list_tables()


@router.get("/warehouse/tables/{table_name}/schema")
def table_schema(table_name: str):
    schema = service.get_table_schema(table_name)
    if not schema:
        raise HTTPException(404, detail=f"Table '{table_name}' not found")
    return schema


@router.get("/warehouse/tables/{table_name}/preview")
def table_preview(table_name: str, limit: int = 100):
    try:
        return service.preview_rows(table_name, limit=limit)
    except Exception as e:
        raise HTTPException(404, detail=f"Table '{table_name}' not found or unreadable: {e}")


@router.get("/warehouse/tables/{table_name}/download")
def download_table(table_name: str):
    try:
        csv_text = service.export_csv(table_name)
    except Exception as e:
        raise HTTPException(404, detail=f"Table '{table_name}' not found: {e}")
    headers = {"Content-Disposition": f'attachment; filename="{table_name}.csv"'}
    return PlainTextResponse(csv_text, media_type="text/csv", headers=headers)


@router.delete("/warehouse/tables/{table_name}")
def drop_table(table_name: str):
    service.drop_table(table_name)
    return {"deleted": table_name}