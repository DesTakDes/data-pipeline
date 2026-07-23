"""
datasets.repository
───────────────────────
All SQL against meta.datasets. Mirrors the same pg_conn_factory convention
used by preview/validator.py and pipelines/repository.py.
"""


class DatasetRepository:
    def __init__(self, pg_conn_factory):
        self.pg_conn_factory = pg_conn_factory

    def register(self, name: str, table_name: str, source_file: str,
                 row_count: int = 0, size_mb: float = 0.0) -> int:
        conn = self.pg_conn_factory()
        try:
            cur = conn.cursor()
            cur.execute(
                """INSERT INTO meta.datasets (name, table_name, source_file, row_count, size_mb)
                   VALUES (%s, %s, %s, %s, %s)
                   ON CONFLICT (table_name) DO UPDATE
                     SET row_count = EXCLUDED.row_count, size_mb = EXCLUDED.size_mb
                   RETURNING id""",
                (name, table_name, source_file, row_count, size_mb),
            )
            new_id = cur.fetchone()[0]
            conn.commit()
            cur.close()
            return new_id
        finally:
            conn.close()

    def list_all(self) -> list[dict]:
        conn = self.pg_conn_factory()
        try:
            cur = conn.cursor()
            cur.execute(
                """SELECT id, name, table_name, source_file, row_count, size_mb, created_at
                   FROM meta.datasets ORDER BY created_at DESC"""
            )
            rows = cur.fetchall()
            cur.close()
            keys = ("id", "name", "table_name", "source_file", "row_count", "size_mb", "created_at")
            out = []
            for r in rows:
                d = dict(zip(keys, r))
                d["created_at"] = d["created_at"].isoformat() if d["created_at"] else None
                out.append(d)
            return out
        finally:
            conn.close()

    def get_by_table(self, table_name: str) -> dict | None:
        conn = self.pg_conn_factory()
        try:
            cur = conn.cursor()
            cur.execute(
                """SELECT id, name, table_name, source_file, row_count, size_mb, created_at
                   FROM meta.datasets WHERE table_name = %s""",
                (table_name,),
            )
            row = cur.fetchone()
            cur.close()
            if not row:
                return None
            keys = ("id", "name", "table_name", "source_file", "row_count", "size_mb", "created_at")
            d = dict(zip(keys, row))
            d["created_at"] = d["created_at"].isoformat() if d["created_at"] else None
            return d
        finally:
            conn.close()

    def delete(self, table_name: str) -> None:
        conn = self.pg_conn_factory()
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM meta.datasets WHERE table_name = %s", (table_name,))
            conn.commit()
            cur.close()
        finally:
            conn.close()