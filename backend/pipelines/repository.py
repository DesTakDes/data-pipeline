"""
pipelines.repository
────────────────────────
All SQL against meta.pipeline_runs lives here — service.py never writes
raw SQL. Takes a pg_conn_factory (same convention as preview/validator.py)
so it can be unit-tested with a fake connection.
"""
import uuid
from datetime import datetime


class PipelineRunRepository:
    def __init__(self, pg_conn_factory):
        self.pg_conn_factory = pg_conn_factory

    def create_run(self, workflow_id: str, dag_id: str, triggered_by: str | None = None) -> str:
        run_id = str(uuid.uuid4())
        conn = self.pg_conn_factory()
        try:
            cur = conn.cursor()
            cur.execute(
                """INSERT INTO meta.pipeline_runs (run_id, workflow_id, dag_id, status, triggered_by)
                   VALUES (%s, %s, %s, 'queued', %s)""",
                (run_id, workflow_id, dag_id, triggered_by),
            )
            conn.commit()
            cur.close()
        finally:
            conn.close()
        return run_id

    def update_status(self, run_id: str, status: str, message: str | None = None,
                       finished: bool = False) -> None:
        conn = self.pg_conn_factory()
        try:
            cur = conn.cursor()
            if finished:
                cur.execute(
                    """UPDATE meta.pipeline_runs
                       SET status = %s, message = %s, finished_at = %s
                       WHERE run_id = %s""",
                    (status, message, datetime.now(), run_id),
                )
            else:
                cur.execute(
                    "UPDATE meta.pipeline_runs SET status = %s, message = %s WHERE run_id = %s",
                    (status, message, run_id),
                )
            conn.commit()
            cur.close()
        finally:
            conn.close()

    def get_run(self, run_id: str) -> dict | None:
        conn = self.pg_conn_factory()
        try:
            cur = conn.cursor()
            cur.execute(
                """SELECT run_id, workflow_id, dag_id, status, message, started_at, finished_at, triggered_by
                   FROM meta.pipeline_runs WHERE run_id = %s""",
                (run_id,),
            )
            row = cur.fetchone()
            cur.close()
            if not row:
                return None
            return self._row_to_dict(row)
        finally:
            conn.close()

    def list_runs(self, workflow_id: str | None = None, limit: int = 50) -> list[dict]:
        conn = self.pg_conn_factory()
        try:
            cur = conn.cursor()
            if workflow_id:
                cur.execute(
                    """SELECT run_id, workflow_id, dag_id, status, message, started_at, finished_at, triggered_by
                       FROM meta.pipeline_runs WHERE workflow_id = %s
                       ORDER BY started_at DESC LIMIT %s""",
                    (workflow_id, limit),
                )
            else:
                cur.execute(
                    """SELECT run_id, workflow_id, dag_id, status, message, started_at, finished_at, triggered_by
                       FROM meta.pipeline_runs ORDER BY started_at DESC LIMIT %s""",
                    (limit,),
                )
            rows = cur.fetchall()
            cur.close()
            return [self._row_to_dict(r) for r in rows]
        finally:
            conn.close()

    @staticmethod
    def _row_to_dict(row) -> dict:
        keys = ("run_id", "workflow_id", "dag_id", "status", "message",
                "started_at", "finished_at", "triggered_by")
        d = dict(zip(keys, row))
        for k in ("started_at", "finished_at"):
            if d.get(k) is not None:
                d[k] = d[k].isoformat()
        return d