"""
preview.validator
────────────────────
Static validation of the graph BEFORE any Spark job is submitted. Depends
only on Postgres (for schema lookups) — never on Spark or Airflow.
"""
import re
from dataclasses import dataclass, field


class ValidationError(Exception):
    def __init__(self, errors: list[str]):
        self.errors = errors
        super().__init__("; ".join(errors))


@dataclass
class ValidationResult:
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


ALLOWED_DTYPES = {"TEXT", "INTEGER", "BIGINT", "NUMERIC", "BOOLEAN", "DATE", "TIMESTAMP", "VARCHAR(255)"}
FORBIDDEN_SQL_KEYWORDS = re.compile(r"\b(DROP|DELETE|INSERT|UPDATE|ALTER|GRANT|;)\b", re.IGNORECASE)


class GraphValidator:
    def __init__(self, nodes: list[dict], edges: list[dict], pg_conn_factory):
        self.nodes = {n["id"]: n for n in nodes}
        self.edges = edges
        self.pg_conn_factory = pg_conn_factory
        self._schema_cache: dict[str, list[str]] = {}

    def validate(self, target_node_id: str) -> ValidationResult:
        result = ValidationResult()
        if target_node_id not in self.nodes:
            result.errors.append(f"Node '{target_node_id}' does not exist in graph")
            return result

        available_cols: dict[str, list[str]] = {}

        for node_id, node in self.nodes.items():
            ntype = node.get("data", {}).get("type", "")
            config = node.get("data", {}).get("config", {}) or {}

            if ntype == "input_dataset":
                table = (config.get("dataset") or {}).get("table_name")
                if not table:
                    result.errors.append(f"[{node_id}] Input Dataset belum dikonfigurasi")
                    continue
                cols = self._get_table_columns(table)
                if cols is None:
                    result.errors.append(f"[{node_id}] Table '{table}' tidak ditemukan")
                else:
                    available_cols[node_id] = cols

            elif ntype == "filter_rows":
                self._validate_sql_expression(config.get("formula", ""), node_id, result)

            elif ntype in ("select_col", "drop_col"):
                upstream = self._resolve_upstream_cols(node_id, available_cols)
                missing = [c for c in config.get("columns", []) if upstream and c not in upstream]
                if missing:
                    result.errors.append(f"[{node_id}] Missing column(s): {missing}")

            elif ntype == "rename_col":
                renames = config.get("renames", {})
                targets = list(renames.values())
                if len(targets) != len(set(targets)):
                    result.errors.append(f"[{node_id}] Duplicate output column name in rename_col")
                upstream = self._resolve_upstream_cols(node_id, available_cols)
                missing = [c for c in renames if upstream and c not in upstream]
                if missing:
                    result.errors.append(f"[{node_id}] Rename source column(s) not found: {missing}")

            elif ntype == "change_type":
                for col, dtype in (config.get("types") or {}).items():
                    if dtype not in ALLOWED_DTYPES:
                        result.errors.append(f"[{node_id}] Invalid dtype '{dtype}' for column '{col}'")

            elif ntype == "join_data":
                right_table = config.get("rightTable", "")
                left_col, right_col = config.get("leftCol", ""), config.get("rightCol", "")
                if not right_table:
                    result.errors.append(f"[{node_id}] Join: right table not configured")
                if config.get("joinType", "").upper() != "CROSS JOIN" and not (left_col and right_col):
                    result.errors.append(f"[{node_id}] Invalid join key: leftCol/rightCol required")
                elif left_col and right_col:
                    right_cols = self._get_table_columns(right_table)
                    if right_cols is not None and right_col not in right_cols:
                        result.errors.append(f"[{node_id}] Join key '{right_col}' not found in '{right_table}'")

            elif ntype == "group_agg":
                if not config.get("groupCols") or not config.get("aggCols"):
                    result.errors.append(f"[{node_id}] group_agg requires groupCols and aggCols")

        return result

    def _get_table_columns(self, table_name: str) -> list[str] | None:
        if table_name in self._schema_cache:
            return self._schema_cache[table_name]
        schema, tname = table_name.split(".", 1) if "." in table_name else ("staging", table_name)
        conn = self.pg_conn_factory()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (schema, tname.strip('"')))
            cols = [r[0] for r in cur.fetchall()]
            cur.close()
            if not cols:
                return None
            self._schema_cache[table_name] = cols
            return cols
        finally:
            conn.close()

    def _resolve_upstream_cols(self, node_id: str, available_cols: dict) -> list[str] | None:
        for e in self.edges:
            if e["target"] == node_id and e["source"] in available_cols:
                return available_cols[e["source"]]
        return None

    def _validate_sql_expression(self, expr: str, node_id: str, result: ValidationResult):
        if not expr or not expr.strip():
            result.errors.append(f"[{node_id}] Empty filter formula")
            return
        if FORBIDDEN_SQL_KEYWORDS.search(expr):
            result.errors.append(f"[{node_id}] Formula contains forbidden keyword: {expr}")