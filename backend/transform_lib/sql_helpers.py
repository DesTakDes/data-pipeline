"""
transform_lib.sql_helpers
──────────────────────────
Pure, engine-agnostic SQL string builders shared by DuckDBCompiler and
PostgresCompiler. No side effects, no DB connections — 100% unit-testable
without any infrastructure.
"""

DUCKDB_DTYPE_MAP = {
    "TEXT": "VARCHAR", "INTEGER": "INTEGER", "BIGINT": "BIGINT",
    "NUMERIC": "DOUBLE", "BOOLEAN": "BOOLEAN",
    "DATE": "DATE", "TIMESTAMP": "TIMESTAMP", "VARCHAR(255)": "VARCHAR",
}

POSTGRES_DTYPE_MAP = {
    "TEXT": "TEXT", "INTEGER": "INTEGER", "BIGINT": "BIGINT",
    "NUMERIC": "NUMERIC", "BOOLEAN": "BOOLEAN",
    "DATE": "DATE", "TIMESTAMP": "TIMESTAMP", "VARCHAR(255)": "VARCHAR(255)",
}

SPARK_DTYPE_MAP = {
    "TEXT": "string", "INTEGER": "int", "BIGINT": "bigint",
    "NUMERIC": "double", "BOOLEAN": "boolean",
    "DATE": "date", "TIMESTAMP": "timestamp", "VARCHAR(255)": "string",
}

SCI_FUNC_MAP = {
    "sin": "SIN", "cos": "COS", "sqrt": "SQRT",
    "radians": "RADIANS", "atan2": "ATAN2", "power": "POWER",
}


def quote_cols(cols: list[str]) -> str:
    """SELECT "a", "b", "c" style column list."""
    return ", ".join(f'"{c}"' for c in cols)


def sql_when_fragment(col: str, condition: str, value, result) -> str | None:
    """
    Build one WHEN ... THEN ... fragment for a CASE expression, used by
    val_mapper on both DuckDB and Postgres (identical SQL dialect for CASE).
    Returns None if the fragment cannot be safely constructed.
    """
    result_lit = repr(result)
    col_ref = f'"{col}"'

    if condition == "IS NULL":
        return f"WHEN {col_ref} IS NULL THEN {result_lit}"
    if condition == "IS NOT NULL":
        return f"WHEN {col_ref} IS NOT NULL THEN {result_lit}"

    if condition in ("IN", "NOT IN"):
        vals = [v.strip() for v in str(value).split(",") if v.strip() != ""]
        if not vals:
            return None
        vals_sql = ", ".join(repr(v) for v in vals)
        op = "IN" if condition == "IN" else "NOT IN"
        return f"WHEN {col_ref} {op} ({vals_sql}) THEN {result_lit}"

    if condition == "LIKE":
        return f"WHEN {col_ref} LIKE {repr(value)} THEN {result_lit}"

    if condition in (">", ">=", "<", "<="):
        try:
            num = float(value)
        except (TypeError, ValueError):
            return None
        return f"WHEN TRY_CAST({col_ref} AS DOUBLE PRECISION) {condition} {num} THEN {result_lit}"

    op = condition if condition in ("=", "!=") else "="
    return f"WHEN {col_ref} {op} {repr(value)} THEN {result_lit}"


def build_case_expression(src: str, whens: list[dict], else_value, new_col: str) -> str | None:
    """Assemble the full `CASE WHEN ... ELSE ... END AS "new_col"` expression for val_mapper."""
    fragments = []
    for w in whens:
        condition = w.get("condition", "=")
        value = w.get("value", "")
        result = w.get("result", "")
        if condition not in ("IS NULL", "IS NOT NULL") and value == "":
            continue
        if result == "" and result != 0:
            continue
        frag = sql_when_fragment(src, condition, value, result)
        if frag:
            fragments.append(frag)
    if not fragments:
        return None
    return f'CASE {" ".join(fragments)} ELSE {repr(else_value)} END AS "{new_col}"'


def concat_expression(selected: list[str], sep: str, cast_fn: str = "VARCHAR") -> str:
    """Build the `COALESCE(CAST(col AS X), '') || sep || ...` expression used by combine_cols."""
    parts = [f'COALESCE(CAST("{c}" AS {cast_fn}), \'\')' for c in selected]
    return f" || {sep!r} || ".join(parts)