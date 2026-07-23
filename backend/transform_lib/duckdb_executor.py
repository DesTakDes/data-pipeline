"""
transform_lib.duckdb_executor
────────────────────────────────
Thin glue layer that wires DuckDBCompiler to an actual DuckDB in-memory
connection + a Postgres source. Used identically by:
  - /api/preview/transform (fast preview on a 5000-row sample)
  - Airflow's DuckDB task runner (full dataset, chunked load)

The compiler itself (duckdb_compiler.py) never touches a live connection —
that's this module's job, which keeps the compiler unit-testable in isolation.
"""
import duckdb
import pandas as pd
from .duckdb_compiler import DuckDBCompiler
from .spec import TransformStep


def register_right_tables(con: "duckdb.DuckDBPyConnection", pg_conn, steps: list,
                           row_limit: int | None = None) -> dict:
    """
    For every join_data step, read the right-hand table from Postgres and
    register it into the DuckDB connection. Returns the right_tables map
    expected by DuckDBCompiler(right_tables=...).
    """
    right_tables: dict = {}
    for idx, raw in enumerate(steps):
        step = raw if isinstance(raw, TransformStep) else TransformStep.from_dict(raw)
        if step.type != "join_data":
            continue
        r_table = step.config.get("rightTable")
        if not r_table or r_table in right_tables:
            continue
        limit_clause = f" LIMIT {row_limit}" if row_limit else ""
        r_df = pd.read_sql(f"SELECT * FROM {r_table}{limit_clause}", pg_conn)
        r_alias = f"_right_{idx}"
        con.register(r_alias, r_df)
        right_tables[r_table] = {"alias": r_alias, "columns": list(r_df.columns)}
    return right_tables


def run_duckdb_transform(input_df: pd.DataFrame, pg_conn, steps: list,
                          limit: int | None = None) -> pd.DataFrame:
    """
    High-level convenience function: given an already-loaded input DataFrame,
    compiles + executes the full transform chain and returns the result.
    """
    con = duckdb.connect(":memory:")
    try:
        con.register("_input", input_df)
        right_tables = register_right_tables(con, pg_conn, steps)
        compiler = DuckDBCompiler(right_tables=right_tables)
        sql = compiler.compile("_input", steps, limit=limit)
        return con.execute(sql).df()
    finally:
        con.close()