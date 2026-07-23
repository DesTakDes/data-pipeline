"""
core.config
─────────────
Single source of truth for every environment variable used across the
backend. Nothing else in the codebase should call os.getenv(...) directly —
that keeps every "where does this come from / what's the default" question
answerable by reading exactly one file.
"""
import os


def _bool(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in ("1", "true", "yes", "on")


def _int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


# ── Postgres (both the app metadata DB and the data warehouse live here) ──
PG_CONFIG = {
    "host": os.getenv("PG_HOST", "postgres"),
    "port": _int("PG_PORT", 5432),
    "database": os.getenv("PG_DB", "airflow"),
    "user": os.getenv("PG_USER", "airflow"),
    "password": os.getenv("PG_PASS", "airflow"),
}

# ── Airflow ────────────────────────────────────────────────────────────────
AIRFLOW_BASE_URL = os.getenv("AIRFLOW_BASE_URL", "http://airflow-webserver:8080")
AIRFLOW_USER = os.getenv("AIRFLOW_USER", "airflow")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "airflow")
DAGS_FOLDER = os.getenv("DAGS_FOLDER", "/opt/airflow/dags")

# ── Backend ─────────────────────────────────────────────────────────────────
BACKEND_URL = os.getenv("BACKEND_URL", "http://backend:8000")
DEFAULT_TASK_TIMEOUT_MINUTES = _int("DEFAULT_TASK_TIMEOUT_MINUTES", 90)

# ── Storage paths ────────────────────────────────────────────────────────────
UPLOAD_DIR = os.getenv("UPLOAD_DIR", "/data_csv/uploads")
PARQUET_DIR = os.getenv("PARQUET_DIR", "/data_csv/parquet")

# ── Preview engine tuning ───────────────────────────────────────────────────
PREVIEW_DEFAULT_LIMIT = _int("PREVIEW_DEFAULT_LIMIT", 100)
PREVIEW_MAX_LIMIT = _int("PREVIEW_MAX_LIMIT", 5000)
PREVIEW_SPARK_SAMPLE_MULTIPLIER = _int("PREVIEW_SPARK_SAMPLE_MULTIPLIER", 50)
PREVIEW_BROADCAST_MAX_MB = _int("PREVIEW_BROADCAST_MAX_MB", 500)
PREVIEW_DUCKDB_THRESHOLD_MB = _int("PREVIEW_DUCKDB_THRESHOLD_MB", 50)
PREVIEW_SPARK_THRESHOLD_MB = _int("PREVIEW_SPARK_THRESHOLD_MB", 5000)  # >= this -> spark, else postgres/duckdb
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark:7077")

# ── Feature flags ────────────────────────────────────────────────────────────
ENABLE_QUERY_CACHE = _bool("ENABLE_QUERY_CACHE", "true")