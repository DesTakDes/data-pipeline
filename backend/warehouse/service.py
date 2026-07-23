"""
warehouse.service
──────────────────────
Read-only access to the `warehouse` schema — the final outputs produced by
"Run Pipeline" (Airflow), never touched by the Preview Engine directly.
"""
import csv
import io


class WarehouseService:
    def __init__(self, pg_conn_factory):
        self.pg_conn_factory = pg_conn_factory

    def list_tables(self) -> list[dict]:
        conn = self.pg_conn_factory()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT t.table_name,
                       pg_total_relation_size(format('warehouse.%I', t.table_name)) / 1024.0 / 1024.0 AS size_mb
                FROM information_schema.tables t
                WHERE t.table_schema = 'warehouse'
                ORDER BY t.table_name
            """)
            rows = cur.fetchall()
            cur.close()
            tables = []
            for name, size_mb in rows:
                tables.append({
                    "table_name": name,
                    "size_mb": round(size_mb or 0, 3),
                    "row_count": self._row_count(conn, name),
                })
            return tables
        finally:
            conn.close()

    def _row_count(self, conn, table_name: str) -> int:
        cur = conn.cursor()
        cur.execute(f'SELECT COUNT(*) FROM warehouse."{table_name}"')
        n = cur.fetchone()[0]
        cur.close()
        return n

    def get_table_schema(self, table_name: str) -> list[dict]:
        conn = self.pg_conn_factory()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT column_name, data_type FROM information_schema.columns
                WHERE table_schema = 'warehouse' AND table_name = %s
                ORDER BY ordinal_position
            """, (table_name,))
            rows = cur.fetchall()
            cur.close()
            return [{"column": c, "type": t} for c, t in rows]
        finally:
            conn.close()

    def preview_rows(self, table_name: str, limit: int = 100) -> dict:
        conn = self.pg_conn_factory()
        try:
            cur = conn.cursor()
            cur.execute(f'SELECT * FROM warehouse."{table_name}" LIMIT %s', (limit,))
            columns = [d.name for d in cur.description]
            rows = [dict(zip(columns, r)) for r in cur.fetchall()]
            cur.close()
            return {"columns": columns, "rows": rows}
        finally:
            conn.close()

    def export_csv(self, table_name: str) -> str:
        """Streams the whole warehouse table out as CSV text — used by the
        /api/warehouse/{table}/download endpoint. For very large tables the
        router should prefer a StreamingResponse over this in-memory buffer;
        kept simple here since warehouse outputs are already aggregated/final."""
        conn = self.pg_conn_factory()
        try:
            cur = conn.cursor()
            cur.execute(f'SELECT * FROM warehouse."{table_name}"')
            columns = [d.name for d in cur.description]
            buf = io.StringIO()
            writer = csv.writer(buf)
            writer.writerow(columns)
            writer.writerows(cur.fetchall())
            cur.close()
            return buf.getvalue()
        finally:
            conn.close()

    def drop_table(self, table_name: str) -> None:
        conn = self.pg_conn_factory()
        try:
            cur = conn.cursor()
            cur.execute(f'DROP TABLE IF EXISTS warehouse."{table_name}"')
            conn.commit()
            cur.close()
        finally:
            conn.close()