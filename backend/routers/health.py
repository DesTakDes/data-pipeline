"""
routers.health
─────────────────
Liveness/readiness probe. Checks Postgres connectivity and (best-effort)
whether the Preview SparkSession has already been started, without forcing
one to spin up just to answer a health check.
"""
from fastapi import APIRouter
from core.db import get_conn

router = APIRouter()


@router.get("/health")
def health():
    checks = {"postgres": _check_postgres()}
    checks["spark_session"] = _check_spark_session()
    healthy = checks["postgres"]
    return {"status": "ok" if healthy else "degraded", "checks": checks}


def _check_postgres() -> bool:
    try:
        conn = get_conn()
        conn.cursor().execute("SELECT 1")
        conn.close()
        return True
    except Exception:
        return False


def _check_spark_session() -> str:
    from preview.spark_session_pool import _session
    return "running" if _session is not None else "not_started"