"""
core.db
─────────
Lowest-level Postgres access. Every other module (preview/validator,
pipelines/repository, datasets/repository, warehouse/service, ...) is
handed a `pg_conn_factory` callable — usually `get_conn` from here — rather
than importing psycopg2 itself. That's what lets preview/validator.py be
unit-tested with a FakeConn instead of a live database.
"""
import psycopg2
from .config import PG_CONFIG


def get_conn():
    """Returns a brand-new psycopg2 connection. Callers are responsible for
    closing it (or using it as a context manager) — no pooling here because
    the SparkSession / DuckDB paths already hold connections open only for
    the duration of a single request."""
    return psycopg2.connect(**PG_CONFIG)


def ensure_schemas() -> None:
    """
    Idempotent bootstrap, safe to call on every backend startup.
    - staging:   raw uploaded/ingested tables, plus Postgres-engine intermediate
                 `_etl_{task_id}_s{N}` tables created by PostgresCompiler.
    - warehouse: final transformed outputs (what "Run Pipeline" writes to).
    - meta:      application bookkeeping (datasets, pipeline_runs).
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS staging")
        cur.execute("CREATE SCHEMA IF NOT EXISTS warehouse")
        cur.execute("CREATE SCHEMA IF NOT EXISTS meta")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS meta.datasets (
                id            SERIAL PRIMARY KEY,
                name          TEXT NOT NULL,
                table_name    TEXT NOT NULL UNIQUE,
                source_file   TEXT,
                row_count     BIGINT DEFAULT 0,
                size_mb       DOUBLE PRECISION DEFAULT 0,
                created_at    TIMESTAMP NOT NULL DEFAULT now()
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS meta.pipeline_runs (
                id            SERIAL PRIMARY KEY,
                run_id        TEXT NOT NULL UNIQUE,
                workflow_id   TEXT NOT NULL,
                dag_id        TEXT NOT NULL,
                status        TEXT NOT NULL DEFAULT 'queued',
                message       TEXT,
                started_at    TIMESTAMP NOT NULL DEFAULT now(),
                finished_at   TIMESTAMP,
                triggered_by  TEXT
            )
        """)
        conn.commit()
        cur.close()
    finally:
        conn.close()