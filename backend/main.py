from fastapi import FastAPI, UploadFile, File, HTTPException, Form, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
import psycopg2
import psycopg2.extras
import pandas as pd
import requests
import os
import io
import json
import re
import time
import uuid
import tempfile
import shutil
import traceback
from datetime import datetime, timedelta
from typing import Optional
from pathlib import Path
import upload_worker
import spark_engine

app = FastAPI(title="ETLFlow API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

AIRFLOW_URL  = os.getenv("AIRFLOW_URL", "http://airflow-webserver:8080")
AIRFLOW_AUTH = ("admin", "admin123")
PG_CONFIG    = {
    "host":     os.getenv("POSTGRES_HOST", "postgres"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "database": os.getenv("POSTGRES_DB", "airflow"),
    "user":     os.getenv("POSTGRES_USER", "airflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
}
DAGS_FOLDER  = os.getenv("DAGS_FOLDER", "/opt/airflow/dags")
DATA_CSV     = "/data_csv"
PARQUET_DIR  = "/data_csv/parquet"
BATCH_INSERT_SIZE = 5_000
CHUNK_ROWS   = 100_000


def get_conn():
    return psycopg2.connect(**PG_CONFIG)


def ensure_schemas(cur, conn):
    cur.execute("CREATE SCHEMA IF NOT EXISTS meta")
    cur.execute("CREATE SCHEMA IF NOT EXISTS staging")
    cur.execute("CREATE SCHEMA IF NOT EXISTS warehouse")
    conn.commit()


def _q(cols: list) -> str:
    return ", ".join(f'"{c}"' for c in cols)


# ════════════════════════════════════════════════════════════════════════════
# PARQUET UTILITIES
# ════════════════════════════════════════════════════════════════════════════

def save_dataframe_to_parquet(df: pd.DataFrame, output_name: str, subdir: str = "") -> str:
    base_dir = os.path.join(PARQUET_DIR, subdir) if subdir else PARQUET_DIR
    os.makedirs(base_dir, exist_ok=True)
    parquet_path = os.path.join(base_dir, f"{output_name}.parquet")
    df.to_parquet(parquet_path, index=False, engine="pyarrow", compression="snappy")
    meta_path = os.path.join(base_dir, f"{output_name}.meta.json")
    with open(meta_path, "w") as f:
        json.dump({
            "output_name": output_name,
            "row_count":   len(df),
            "col_count":   len(df.columns),
            "columns":     list(df.columns),
            "saved_at":    datetime.now().isoformat(),
            "compression": "snappy",
            "file_size_bytes": os.path.getsize(parquet_path),
        }, f, indent=2)
    print(f"[Parquet] Saved {len(df):,} rows → {parquet_path}")
    return parquet_path


def batch_insert_to_postgres(conn, table: str, columns: list, rows: list, batch_size: int = BATCH_INSERT_SIZE):
    if not rows:
        return 0
    cols_quoted  = [f'"{c}"' for c in columns]
    placeholders = ", ".join(["%s"] * len(columns))
    insert_sql   = f'INSERT INTO {table} ({", ".join(cols_quoted)}) VALUES ({placeholders})'
    cur = conn.cursor()
    total_inserted = 0
    try:
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            psycopg2.extras.execute_batch(cur, insert_sql, batch, page_size=batch_size)
            conn.commit()
            total_inserted += len(batch)
    finally:
        cur.close()
    return total_inserted


def dataframe_to_postgres_batch(conn, df: pd.DataFrame, table: str, batch_size: int = BATCH_INSERT_SIZE):
    columns = list(df.columns)
    rows = []
    for row in df.itertuples(index=False):
        rows.append(tuple(
            None if (v is None or (isinstance(v, float) and pd.isna(v))) else v
            for v in row
        ))
    return batch_insert_to_postgres(conn, table, columns, rows, batch_size)


# ════════════════════════════════════════════════════════════════════════════
# DUCKDB TRANSFORM ENGINE
# ════════════════════════════════════════════════════════════════════════════

def _duckdb_available() -> bool:
    try:
        import duckdb
        return True
    except ImportError:
        return False


def _build_duckdb_sql(input_alias: str, transforms: list, limit: Optional[int] = None,
                       right_tables: Optional[dict] = None) -> str:
    """Compile all transforms into a single DuckDB CTE chain SQL."""
    right_tables = right_tables or {}
    cte_parts  = []
    step       = 0
    cur_alias  = input_alias
    cur_cols   = None

    DTYPE_MAP = {
        "TEXT": "VARCHAR", "INTEGER": "INTEGER", "BIGINT": "BIGINT",
        "NUMERIC": "DOUBLE", "BOOLEAN": "BOOLEAN",
        "DATE": "DATE", "TIMESTAMP": "TIMESTAMP", "VARCHAR(255)": "VARCHAR",
    }

    for tx in transforms:
        ntype  = tx.get("type", "")
        config = tx.get("config") or {}
        step  += 1
        alias  = f"s{step}"
        try:
            if ntype == "filter_rows":
                formula = config.get("formula", "1=1")
                cte_parts.append(f"{alias} AS (SELECT * FROM {cur_alias} WHERE {formula})")
            elif ntype == "select_col":
                cols = [c for c in config.get("columns", []) if c]
                if cols:
                    cte_parts.append(f"{alias} AS (SELECT {_q(cols)} FROM {cur_alias})")
                    cur_cols = cols
                else:
                    step -= 1; continue
            elif ntype == "drop_col":
                drop = set(config.get("columns", []))
                if cur_cols:
                    keep = [c for c in cur_cols if c not in drop]
                    cte_parts.append(f"{alias} AS (SELECT {_q(keep)} FROM {cur_alias})")
                    cur_cols = keep
                else:
                    excl = ", ".join(f'"{c}"' for c in drop)
                    cte_parts.append(f"{alias} AS (SELECT * EXCLUDE ({excl}) FROM {cur_alias})")
            elif ntype == "rename_col":
                renames = config.get("renames", {})
                if not renames:
                    step -= 1; continue
                if cur_cols:
                    exprs = [f'"{c}" AS "{renames.get(c, c)}"' for c in cur_cols]
                    cte_parts.append(f"{alias} AS (SELECT {', '.join(exprs)} FROM {cur_alias})")
                    cur_cols = [renames.get(c, c) for c in cur_cols]
                else:
                    rename_sql = ", ".join(f'"{o}" AS "{n}"' for o, n in renames.items())
                    cte_parts.append(f"{alias} AS (SELECT * RENAME ({rename_sql}) FROM {cur_alias})")
            elif ntype == "add_const":
                name  = config.get("name", "new_col")
                val   = config.get("value", "")
                dtype = DTYPE_MAP.get(config.get("dtype", "TEXT"), "VARCHAR")
                cte_parts.append(f'{alias} AS (SELECT *, CAST({repr(val)} AS {dtype}) AS "{name}" FROM {cur_alias})')
                if cur_cols:
                    cur_cols = cur_cols + [name]
            elif ntype == "set_val":
                target = config.get("targetCol", "")
                if not target:
                    step -= 1; continue
                if config.get("useExpr"):
                    expr = config.get("expr", f'"{target}"')
                else:
                    src  = config.get("sourceCol", target)
                    expr = f'"{src}"'
                cte_parts.append(f'{alias} AS (SELECT * REPLACE ({expr} AS "{target}") FROM {cur_alias})')
            elif ntype == "val_mapper":
                src     = config.get("sourceCol", "")
                new_col = config.get("newColName", "mapped")
                whens   = config.get("whens", [])
                else_v  = config.get("elseValue", "")
                if not src or not whens:
                    step -= 1; continue
                fragments = []
                for w in whens:
                    condition = w.get("condition", "=")
                    value     = w.get("value", "")
                    result    = w.get("result", "")
                    # IS NULL / IS NOT NULL tidak butuh value, sisanya wajib punya value
                    if condition not in ("IS NULL", "IS NOT NULL") and value == "":
                        continue
                    if result == "" and result != 0:
                        continue
                    frag = _sql_when_fragment(src, condition, value, result)
                    if frag:
                        fragments.append(frag)
                if not fragments:
                    step -= 1; continue
                case_expr = f'CASE {" ".join(fragments)} ELSE {repr(else_v)} END AS "{new_col}"'
                cte_parts.append(f"{alias} AS (SELECT *, {case_expr} FROM {cur_alias})")
                if cur_cols:
                    cur_cols = cur_cols + [new_col]
            elif ntype == "fill_null":
                fill_cols = config.get("columns", [])
                fill_val  = config.get("fillValue", "")
                if fill_cols and config.get("fillType", "value") == "value":
                    replace_parts = ", ".join(
                        f'COALESCE("{c}", {repr(str(fill_val))}) AS "{c}"'
                        for c in fill_cols
                    )
                    cte_parts.append(f"{alias} AS (SELECT * REPLACE ({replace_parts}) FROM {cur_alias})")
                else:
                    step -= 1; continue
            elif ntype == "change_type":
                types = config.get("types", {})
                if not types:
                    step -= 1; continue
                replace_parts = ", ".join(
                    f'TRY_CAST("{c}" AS {DTYPE_MAP.get(t, "VARCHAR")}) AS "{c}"'
                    for c, t in types.items()
                )
                cte_parts.append(f"{alias} AS (SELECT * REPLACE ({replace_parts}) FROM {cur_alias})")
            elif ntype == "order_table":
                orders = config.get("orders", [])
                if not orders:
                    step -= 1; continue
                oc = ", ".join(
                    f'"{o["col"]}" {o.get("dir", "ASC")}'
                    for o in orders if o.get("col")
                )
                cte_parts.append(f"{alias} AS (SELECT * FROM {cur_alias} ORDER BY {oc})")
            elif ntype == "group_agg":
                gcols = config.get("groupCols", [])
                acols = config.get("aggCols", [])
                if not gcols or not acols:
                    step -= 1; continue
                agg_exprs = []
                for a in acols:
                    fn  = a.get("func", "COUNT")
                    col = a.get("col", "")
                    aln = a.get("alias", f'{col}_{fn.lower()}')
                    if fn == "COUNT DISTINCT":
                        agg_exprs.append(f'COUNT(DISTINCT "{col}") AS "{aln}"')
                    else:
                        agg_exprs.append(f'{fn}("{col}") AS "{aln}"')
                g = _q(gcols)
                cte_parts.append(f"{alias} AS (SELECT {g}, {', '.join(agg_exprs)} FROM {cur_alias} GROUP BY {g})")
                cur_cols = gcols + [a.get("alias", "") for a in acols]
            elif ntype == "calc":
                new_col   = (config.get("newColName") or "result").strip()
                col_a     = config.get("colA", "")
                col_b     = config.get("colB", "")
                operation = config.get("operation", "+")
                if not (new_col and col_a and col_b):
                    step -= 1; continue
                op_expr = f'TRY_CAST("{col_a}" AS DOUBLE) {operation} TRY_CAST("{col_b}" AS DOUBLE)'
                cte_parts.append(f'{alias} AS (SELECT *, ({op_expr}) AS "{new_col}" FROM {cur_alias})')
                if cur_cols:
                    cur_cols = cur_cols + [new_col]
            elif ntype == "adv_calculator":
                calcs   = config.get("calculations", [])
                SCI_MAP = {
                    "sin": "SIN", "cos": "COS", "sqrt": "SQRT",
                    "radians": "RADIANS", "atan2": "ATAN2", "power": "POWER",
                }
                exprs = []
                for calc in calcs:
                    fn    = SCI_MAP.get(calc.get("operation", "sin"), "SIN")
                    col_a = calc.get("colA", "")
                    col_b = calc.get("colB", "")
                    new_c = (calc.get("newColName") or "").strip()
                    if not new_c or not col_a:
                        continue
                    if fn in ("ATAN2", "POWER"):
                        exprs.append(f'{fn}(TRY_CAST("{col_a}" AS DOUBLE), TRY_CAST("{col_b}" AS DOUBLE)) AS "{new_c}"')
                    else:
                        exprs.append(f'{fn}(TRY_CAST("{col_a}" AS DOUBLE)) AS "{new_c}"')
                if exprs:
                    cte_parts.append(f'{alias} AS (SELECT *, {", ".join(exprs)} FROM {cur_alias})')
                else:
                    step -= 1; continue
            elif ntype == "combine_cols":
                new_col     = (config.get("newColName") or "combined").strip()
                sep         = config.get("separator", " ")
                selected    = config.get("selectedCols", [])
                remove_orig = config.get("removeOriginal", False)
                if not new_col or not selected:
                    step -= 1; continue
                concat_parts = f" || {repr(sep)} || ".join(
                    f"COALESCE(CAST(\"{c}\" AS VARCHAR), '')" for c in selected
                )
                if remove_orig:
                    excl = ", ".join(f'"{c}"' for c in selected)
                    cte_parts.append(f'{alias} AS (SELECT * EXCLUDE ({excl}), ({concat_parts}) AS "{new_col}" FROM {cur_alias})')
                else:
                    cte_parts.append(f'{alias} AS (SELECT *, ({concat_parts}) AS "{new_col}" FROM {cur_alias})')
                if cur_cols:
                    cur_cols = [c for c in cur_cols if c not in (selected if remove_orig else [])]
                    if new_col not in cur_cols:
                        cur_cols = cur_cols + [new_col]
            elif ntype == "join_data":
                right_table = config.get("rightTable", "")
                left_col    = config.get("leftCol", "")
                right_col   = config.get("rightCol", "")
                r_info      = right_tables.get(right_table)

                if not (right_table and left_col and r_info):
                    step -= 1; continue

                r_alias  = r_info["alias"]
                r_cols   = r_info.get("columns", [])
                raw_type = config.get("joinType", "INNER JOIN").upper()
                is_cross = "CROSS" in raw_type
                sql_join = "CROSS JOIN" if is_cross else raw_type

                dup = [c for c in r_cols if cur_cols and c in cur_cols and c != right_col]
                right_select = ", ".join(
                    f'{r_alias}."{c}" AS "{c}_right"' if c in dup else f'{r_alias}."{c}"'
                    for c in r_cols
                ) if r_cols else f"{r_alias}.*"

                select_clause = f"{cur_alias}.*, {right_select}"

                if is_cross:
                    cte_parts.append(
                        f"{alias} AS (SELECT {select_clause} FROM {cur_alias} CROSS JOIN {r_alias})"
                    )
                elif right_col:
                    cte_parts.append(
                        f'{alias} AS (SELECT {select_clause} FROM {cur_alias} '
                        f'{sql_join} {r_alias} ON {cur_alias}."{left_col}" = {r_alias}."{right_col}")'
                    )
                else:
                    step -= 1; continue

                cur_cols = None
            else:
                step -= 1; continue
        except Exception as e:
            print(f"[DuckDB builder] step {step} ({ntype}) error: {e} — skipped")
            step -= 1; continue
        cur_alias = alias

    limit_clause = f" LIMIT {limit}" if limit else ""
    if cte_parts:
        return f"WITH {', '.join(cte_parts)} SELECT * FROM {cur_alias}{limit_clause}"
    return f"SELECT * FROM {input_alias}{limit_clause}"


def _run_duckdb_pipeline(input_table, output_name, transforms, progress_cb=None):
    import duckdb

    def upd(pct, msg):
        print(f"[DuckDB] {pct}% — {msg}")
        if progress_cb:
            try:
                progress_cb(pct, msg)
            except Exception:
                pass

    t0  = time.time()
    con = duckdb.connect(":memory:")
    pg  = get_conn()

    try:
        cur = pg.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {input_table}")
        row_count = cur.fetchone()[0]
        cur.execute("SELECT pg_total_relation_size(%s) / 1024.0 / 1024.0", (input_table,))
        size_mb = float(cur.fetchone()[0] or 0)
        cur.close()
        upd(3, f"{row_count:,} rows | {size_mb:.1f} MB")

        upd(5, "Reading data from PostgreSQL…")
        chunks = []
        loaded = 0
        offset = 0
        cs     = 200_000
        while True:
            chunk = pd.read_sql(f"SELECT * FROM {input_table} LIMIT {cs} OFFSET {offset}", pg)
            if chunk.empty:
                break
            chunks.append(chunk)
            loaded  += len(chunk)
            offset  += cs
            pct      = 5 + int((loaded / max(row_count, 1)) * 35)
            upd(pct, f"Loaded {loaded:,}/{row_count:,}…")
            if len(chunk) < cs:
                break

        df_input = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
        con.register("_input", df_input)
        upd(40, f"Data ready in DuckDB. Running {len(transforms)} transform(s)…")

        sql        = _build_duckdb_sql("_input", transforms)
        t_tx       = time.time()
        result_df  = con.execute(sql).df()
        elapsed_tx = time.time() - t_tx
        actual     = len(result_df)
        upd(75, f"Transforms done: {actual:,} rows in {elapsed_tx:.1f}s")

        upd(78, "Saving to Parquet (snappy)…")
        parquet_path = save_dataframe_to_parquet(result_df, output_name)
        parquet_mb   = os.path.getsize(parquet_path) / (1024 * 1024)
        upd(82, f"Parquet saved: {parquet_path} ({parquet_mb:.1f} MB)")

        safe_out  = re.sub(r'[^a-z0-9_]', '_', output_name.lower()).strip('_') or "output"
        out_table = f'warehouse."{safe_out}"'

        pg.rollback()
        wcur = pg.cursor()
        wcur.execute("CREATE SCHEMA IF NOT EXISTS warehouse")
        wcur.execute(f"DROP TABLE IF EXISTS {out_table}")

        PG_TYPE = {
            "int64": "BIGINT", "int32": "INTEGER",
            "float64": "NUMERIC", "float32": "NUMERIC",
            "bool": "BOOLEAN", "object": "TEXT",
            "datetime64[ns]": "TIMESTAMP",
        }
        col_defs = ", ".join(
            f'"{c}" {PG_TYPE.get(str(result_df[c].dtype), "TEXT")}'
            for c in result_df.columns
        )
        wcur.execute(f"CREATE TABLE {out_table} ({col_defs}, loaded_at TIMESTAMP DEFAULT NOW())")
        pg.commit()
        wcur.close()

        inserted = dataframe_to_postgres_batch(pg, result_df, out_table, BATCH_INSERT_SIZE)
        upd(98, f"Batch insert complete: {inserted:,} rows")

        elapsed = time.time() - t0
        upd(100, f"Done! {actual:,} rows in {elapsed:.1f} seconds")
        return {
            "status":       "success",
            "engine":       "duckdb",
            "rows":         actual,
            "cols":         len(result_df.columns),
            "parquet_path": parquet_path,
            "elapsed_s":    round(elapsed, 1),
            "size_mb":      round(size_mb, 1),
        }
    finally:
        con.close()
        pg.close()


# ════════════════════════════════════════════════════════════════════════════
# SPLIT DAG GENERATOR
#
# Architecture:
#   /dags/
#     dag_{dag_id}.py          ← Workflow orchestrator: imports & wires tasks
#     tasks/
#       task_{task_id}.py      ← Individual task: all transform logic lives here
#
# The workflow DAG file only knows about:
#   - Which tasks exist
#   - Which input table to use
#   - Task dependencies (depends_on)
#
# Each task file knows about:
#   - Its transforms
#   - How to run DuckDB / Spark / Postgres
#   - How to write output to warehouse + parquet
# ════════════════════════════════════════════════════════════════════════════

def generate_task_file(
    task_id:      str,
    dag_id:       str,
    workflow_id:  str,
    input_table:  str,
    output_name:  str,
    transforms:   list,
    execution_timeout_minutes: int = 90,
) -> str:
    """
    Generate a standalone task Python file.
    This file contains ALL the transform logic for one output node.
    It exposes a single callable: run_task(task_def, **context)
    """
    tasks_json = json.dumps(transforms, ensure_ascii=True)
    safe_input = re.sub(r'[^a-zA-Z0-9_.]', '', input_table)
    safe_out   = re.sub(r'[^a-z0-9_]', '_', output_name.lower()).strip('_') or "output"
    now_str    = datetime.now().isoformat()
    pg_host    = PG_CONFIG["host"]
    pg_port    = PG_CONFIG["port"]
    pg_db      = PG_CONFIG["database"]
    pg_user    = PG_CONFIG["user"]
    pg_pass    = PG_CONFIG["password"]

    code = '''# Auto-generated TASK file
# Task ID   : TASK_ID_PLACEHOLDER
# DAG ID    : DAG_ID_PLACEHOLDER
# Output    : warehouse.OUTPUT_NAME_PLACEHOLDER
# Generated : NOW_PLACEHOLDER
# ─────────────────────────────────────────────────────────────────────────────
# This file contains ALL transform logic for this task.
# It is imported by the workflow DAG file (dag_DAG_ID_PLACEHOLDER.py).
# ─────────────────────────────────────────────────────────────────────────────

import os, re, time, json, requests, traceback
import psycopg2, psycopg2.extras
import pandas as pd

TASK_ID     = 'TASK_ID_PLACEHOLDER'
DAG_ID      = 'DAG_ID_PLACEHOLDER'
INPUT_TABLE = 'INPUT_TABLE_PLACEHOLDER'
OUTPUT_NAME = 'OUTPUT_NAME_PLACEHOLDER'
TRANSFORMS  = json.loads(TRANSFORMS_JSON_PLACEHOLDER)
BACKEND_URL = "http://backend:8000"
PARQUET_DIR = "/data_csv/parquet"
BATCH_INSERT_SIZE = 5000
CHUNK_ROWS  = 100_000
SHUFFLE_PARTITIONS = 200

PG_CONFIG = {
    'host': 'PG_HOST_PLACEHOLDER', 'port': PG_PORT_PLACEHOLDER,
    'database': 'PG_DB_PLACEHOLDER',
    'user': 'PG_USER_PLACEHOLDER', 'password': 'PG_PASS_PLACEHOLDER',
}


# ── Helpers ──────────────────────────────────────────────────────────────────

def _get_conn():
    return psycopg2.connect(**PG_CONFIG)


def _q(cols):
    return ", ".join(f\'"{c}"\' for c in cols)

def _sql_when_fragment(col, condition, value, result):
    result_lit = repr(result)
    col_ref = f\'"{col}"\'
    if condition == \'IS NULL\':
        return f"WHEN {col_ref} IS NULL THEN {result_lit}"
    if condition == \'IS NOT NULL\':
        return f"WHEN {col_ref} IS NOT NULL THEN {result_lit}"
    if condition in (\'IN\', \'NOT IN\'):
        vals = [v.strip() for v in str(value).split(\',\') if v.strip() != \'\']
        if not vals:
            return None
        vals_sql = \', \'.join(repr(v) for v in vals)
        op = \'IN\' if condition == \'IN\' else \'NOT IN\'
        return f"WHEN {col_ref} {op} ({vals_sql}) THEN {result_lit}"
    if condition == \'LIKE\':
        return f"WHEN {col_ref} LIKE {repr(value)} THEN {result_lit}"
    if condition in (\'>\', \'>=\', \'<\', \'<=\'):
        try:
            num = float(value)
        except (TypeError, ValueError):
            return None
        return f"WHEN TRY_CAST({col_ref} AS DOUBLE) {condition} {num} THEN {result_lit}"
    op = condition if condition in (\'=\', \'!=\') else \'=\'
    return f"WHEN {col_ref} {op} {repr(value)} THEN {result_lit}"


def _estimate_mb(pg_conn, table):
    try:
        cur = pg_conn.cursor()
        cur.execute("SELECT pg_total_relation_size(%s) / 1024.0 / 1024.0", (table,))
        val = float(cur.fetchone()[0] or 0)
        cur.close()
        return val
    except Exception:
        return 0.0


def save_parquet_snappy(df, output_name, subdir=""):
    base_dir = os.path.join(PARQUET_DIR, subdir) if subdir else PARQUET_DIR
    os.makedirs(base_dir, exist_ok=True)
    parquet_path = os.path.join(base_dir, f"{output_name}.parquet")
    df.to_parquet(parquet_path, index=False, engine="pyarrow", compression="snappy")
    meta_path = os.path.join(base_dir, f"{output_name}.meta.json")
    import json as _json
    with open(meta_path, "w") as f:
        _json.dump({
            "output_name": output_name,
            "row_count":   len(df),
            "col_count":   len(df.columns),
            "columns":     list(df.columns),
            "saved_at":    time.strftime("%Y-%m-%dT%H:%M:%S"),
            "compression": "snappy",
            "file_size_bytes": os.path.getsize(parquet_path),
        }, f, indent=2)
    print(f"[Parquet] {len(df):,} rows → {parquet_path} (snappy)")
    return parquet_path


def batch_insert_df(conn, df, table, batch_size=BATCH_INSERT_SIZE):
    columns      = list(df.columns)
    cols_quoted  = [f\'"{c}"\' for c in columns]
    placeholders = ", ".join(["%s"] * len(columns))
    insert_sql   = f"INSERT INTO {table} ({', '.join(cols_quoted)}) VALUES ({placeholders})"
    rows = [
        tuple(None if (v is None or (isinstance(v, float) and pd.isna(v))) else v
              for v in row)
        for row in df.itertuples(index=False)
    ]
    cur = conn.cursor()
    total = 0
    try:
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            psycopg2.extras.execute_batch(cur, insert_sql, batch, page_size=batch_size)
            conn.commit()
            total += len(batch)
            print(f"[BatchInsert] {total:,}/{len(rows):,} → {table}")
    finally:
        cur.close()
    return total


# ── DuckDB SQL Builder ────────────────────────────────────────────────────────

def _build_transform_sql(input_alias, transforms, limit=None):
    DTYPE_MAP = {
        'TEXT':'VARCHAR','INTEGER':'INTEGER','BIGINT':'BIGINT',
        'NUMERIC':'DOUBLE','BOOLEAN':'BOOLEAN','DATE':'DATE',
        'TIMESTAMP':'TIMESTAMP','VARCHAR(255)':'VARCHAR',
    }
    cte_parts = []
    step      = 0
    cur_alias = input_alias
    cur_cols  = None

    for tx in transforms:
        ntype  = tx.get('type','')
        config = tx.get('config') or {}
        step  += 1
        alias  = f's{step}'
        try:
            if ntype == 'filter_rows':
                formula = config.get('formula','1=1')
                cte_parts.append(f'{alias} AS (SELECT * FROM {cur_alias} WHERE {formula})')
            elif ntype == 'select_col':
                cols = [c for c in config.get('columns',[]) if c]
                if cols:
                    cte_parts.append(f"{alias} AS (SELECT {_q(cols)} FROM {cur_alias})")
                    cur_cols = cols
                else:
                    step -= 1; continue
            elif ntype == 'drop_col':
                drop = set(config.get('columns',[]))
                if cur_cols:
                    keep = [c for c in cur_cols if c not in drop]
                    cte_parts.append(f"{alias} AS (SELECT {_q(keep)} FROM {cur_alias})")
                    cur_cols = keep
                else:
                    excl = ', '.join(f\'"{c}"\' for c in drop)
                    cte_parts.append(f'{alias} AS (SELECT * EXCLUDE ({excl}) FROM {cur_alias})')
            elif ntype == 'rename_col':
                renames = config.get('renames',{})
                if not renames: step -= 1; continue
                if cur_cols:
                    exprs = [f\'"{c}" AS "{renames.get(c,c)}"\' for c in cur_cols]
                    cte_parts.append(f"{alias} AS (SELECT {', '.join(exprs)} FROM {cur_alias})")
                    cur_cols = [renames.get(c,c) for c in cur_cols]
                else:
                    rs = ', '.join(f\'"{o}" AS "{n}"\' for o,n in renames.items())
                    cte_parts.append(f'{alias} AS (SELECT * RENAME ({rs}) FROM {cur_alias})')
            elif ntype == 'add_const':
                name  = config.get('name','new_col')
                val   = config.get('value','')
                dtype = DTYPE_MAP.get(config.get('dtype','TEXT'),'VARCHAR')
                cte_parts.append(f\'{alias} AS (SELECT *, CAST({repr(val)} AS {dtype}) AS "{name}" FROM {cur_alias})\')
                if cur_cols: cur_cols = cur_cols + [name]
            elif ntype == 'set_val':
                target = config.get('targetCol','')
                if not target: step -= 1; continue
                if config.get('useExpr'):
                    expr = config.get('expr', f\'"{target}"\')
                else:
                    src  = config.get('sourceCol', target)
                    expr = f\'"{src}"\'
                cte_parts.append(f\'{alias} AS (SELECT * REPLACE ({expr} AS "{target}") FROM {cur_alias})\')
            elif ntype == 'val_mapper':
                src     = config.get('sourceCol','')
                new_col = config.get('newColName','mapped')
                whens   = config.get('whens',[])
                else_v  = config.get('elseValue','')
                if not src or not whens: step -= 1; continue
                fragments = []
                for w in whens:
                    condition = w.get(\'condition\', \'=\')
                    value     = w.get(\'value\', \'\')
                    result    = w.get(\'result\', \'\')
                    if condition not in (\'IS NULL\', \'IS NOT NULL\') and value == \'\':
                        continue
                    frag = _sql_when_fragment(src, condition, value, result)
                    if frag:
                        fragments.append(frag)
                if not fragments:
                    step -= 1; continue
                wc = \' \'.join(fragments)
                cte_parts.append(f\'{alias} AS (SELECT *, CASE {wc} ELSE {repr(else_v)} END AS "{new_col}" FROM {cur_alias})\')
                if cur_cols: cur_cols = cur_cols + [new_col]
            elif ntype == 'fill_null':
                fill_cols = config.get('columns',[])
                fill_val  = config.get('fillValue','')
                if fill_cols and config.get('fillType','value') == 'value':
                    rp = ', '.join(f\'COALESCE("{c}", {repr(str(fill_val))}) AS "{c}"\' for c in fill_cols)
                    cte_parts.append(f'{alias} AS (SELECT * REPLACE ({rp}) FROM {cur_alias})')
                else: step -= 1; continue
            elif ntype == 'change_type':
                types = config.get('types',{})
                if not types: step -= 1; continue
                rp = ', '.join(f\'TRY_CAST("{c}" AS {DTYPE_MAP.get(t,"VARCHAR")}) AS "{c}"\' for c,t in types.items())
                cte_parts.append(f'{alias} AS (SELECT * REPLACE ({rp}) FROM {cur_alias})')
            elif ntype == 'order_table':
                orders = config.get('orders',[])
                if not orders: step -= 1; continue
                oc = ', '.join(f\'"{o["col"]}" {o.get("dir","ASC")}\' for o in orders if o.get('col'))
                cte_parts.append(f'{alias} AS (SELECT * FROM {cur_alias} ORDER BY {oc})')
            elif ntype == 'group_agg':
                gcols = config.get('groupCols',[])
                acols = config.get('aggCols',[])
                if not gcols or not acols: step -= 1; continue
                agg_exprs = []
                for a in acols:
                    fn  = a.get('func','COUNT')
                    col = a.get('col','')
                    aln = a.get('alias', f\'{col}_{fn.lower()}\')
                    if fn == 'COUNT DISTINCT':
                        agg_exprs.append(f\'COUNT(DISTINCT "{col}") AS "{aln}"\')
                    else:
                        agg_exprs.append(f\'{fn}("{col}") AS "{aln}"\')
                cte_parts.append(f"{alias} AS (SELECT {_q(gcols)}, {', '.join(agg_exprs)} FROM {cur_alias} GROUP BY {_q(gcols)})")
                cur_cols = gcols + [a.get('alias','') for a in acols]
            elif ntype == 'calc':
                new_col = (config.get('newColName') or 'result').strip()
                col_a   = config.get('colA','')
                col_b   = config.get('colB','')
                op      = config.get('operation','+')
                if not (new_col and col_a and col_b): step -= 1; continue
                cte_parts.append(f\'{alias} AS (SELECT *, (TRY_CAST("{col_a}" AS DOUBLE) {op} TRY_CAST("{col_b}" AS DOUBLE)) AS "{new_col}" FROM {cur_alias})\')
                if cur_cols: cur_cols = cur_cols + [new_col]
            elif ntype == 'adv_calculator':
                calcs   = config.get('calculations',[])
                SCI_MAP = {'sin':'SIN','cos':'COS','sqrt':'SQRT','radians':'RADIANS','atan2':'ATAN2','power':'POWER'}
                exprs = []
                for calc in calcs:
                    fn    = SCI_MAP.get(calc.get('operation','sin'),'SIN')
                    col_a = calc.get('colA','')
                    col_b = calc.get('colB','')
                    new_c = (calc.get('newColName') or '').strip()
                    if not new_c or not col_a: continue
                    if fn in ('ATAN2','POWER'):
                        exprs.append(f\'{fn}(TRY_CAST("{col_a}" AS DOUBLE), TRY_CAST("{col_b}" AS DOUBLE)) AS "{new_c}"\')
                    else:
                        exprs.append(f\'{fn}(TRY_CAST("{col_a}" AS DOUBLE)) AS "{new_c}"\')
                if exprs:
                    cte_parts.append(f"{alias} AS (SELECT *, {', '.join(exprs)} FROM {cur_alias})")
                else: step -= 1; continue
            elif ntype == 'combine_cols':
                new_col     = (config.get('newColName') or 'combined').strip()
                sep         = config.get('separator',' ')
                selected    = config.get('selectedCols',[])
                remove_orig = config.get('removeOriginal',False)
                if not new_col or not selected: step -= 1; continue
                cp = f\' || {repr(sep)} || \'.join(f\'COALESCE(CAST("{c}" AS VARCHAR), \\\'\\\')\' for c in selected)
                if remove_orig:
                    excl = ', '.join(f\'"{c}"\' for c in selected)
                    cte_parts.append(f\'{alias} AS (SELECT * EXCLUDE ({excl}), ({cp}) AS "{new_col}" FROM {cur_alias})\')
                else:
                    cte_parts.append(f\'{alias} AS (SELECT *, ({cp}) AS "{new_col}" FROM {cur_alias})\')
            else:
                step -= 1; continue
        except Exception as e:
            print(f"[SQL Builder] {ntype} error: {e}")
            step -= 1; continue
        cur_alias = alias

    lc = f" LIMIT {limit}" if limit else ""
    if cte_parts:
        return f"WITH {', '.join(cte_parts)} SELECT * FROM {cur_alias}{lc}"
    return f"SELECT * FROM {input_alias}{lc}"


# ── DuckDB Runner ─────────────────────────────────────────────────────────────

def _run_duckdb(input_table, output_name, transforms, progress_cb=None):
    import duckdb
    def upd(pct, msg):
        print(f"[DuckDB] {pct}% — {msg}")
        if progress_cb:
            try: progress_cb(pct, msg)
            except Exception: pass

    t0  = time.time()
    con = duckdb.connect(":memory:")
    pg  = _get_conn()
    try:
        cur = pg.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {input_table}")
        row_count = cur.fetchone()[0]
        cur.close()
        upd(3, f"{row_count:,} rows — reading data…")

        chunks = []
        loaded = 0
        offset = 0
        cs     = 200_000
        while True:
            chunk = pd.read_sql(f"SELECT * FROM {input_table} LIMIT {cs} OFFSET {offset}", pg)
            if chunk.empty: break
            chunks.append(chunk)
            loaded += len(chunk)
            offset += cs
            upd(5 + int((loaded / max(row_count,1)) * 35), f"Loaded {loaded:,}/{row_count:,}…")
            if len(chunk) < cs: break

        df_input = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
        con.register("_input", df_input)
        upd(40, f"Data ready. Running {len(transforms)} transform(s)…")

        right_tables = {}
        for idx, tx in enumerate(transforms):
            if tx.get("type") == "join_data":
                r_table = (tx.get("config") or {}).get("rightTable")
                if r_table and r_table not in right_tables:
                    r_df = pd.read_sql(f"SELECT * FROM {r_table}", pg)
                    r_alias = f"_right_{idx}"
                    con.register(r_alias, r_df)
                    right_tables[r_table] = {"alias": r_alias, "columns": list(r_df.columns)}
                    upd(42, f"Loaded right table for join: {r_table} ({len(r_df):,} rows)")

        sql       = _build_transform_sql("_input", transforms)
        t_tx      = time.time()
        result_df = con.execute(sql).df()
        actual    = len(result_df)
        upd(75, f"Transforms done: {actual:,} rows ({time.time()-t_tx:.1f}s)")

        upd(78, "Saving rows as Parquet file (snappy)…")
        pq_path = save_parquet_snappy(result_df, output_name)
        upd(82, f"Parquet: {pq_path}")

        safe_out  = re.sub(r\'[^a-z0-9_]\',\'_\',output_name.lower()).strip(\'_\') or "output"
        out_table = f\'warehouse."{safe_out}"\'
        pg.rollback()
        wcur = pg.cursor()
        wcur.execute("CREATE SCHEMA IF NOT EXISTS warehouse")
        wcur.execute(f"DROP TABLE IF EXISTS {out_table}")
        PG_TYPE = {"int64":"BIGINT","int32":"INTEGER","float64":"NUMERIC","float32":"NUMERIC",
                    "bool":"BOOLEAN","object":"TEXT","datetime64[ns]":"TIMESTAMP"}
        col_defs = ", ".join(f\'"{c}" {PG_TYPE.get(str(result_df[c].dtype),"TEXT")}\' for c in result_df.columns)
        wcur.execute(f"CREATE TABLE {out_table} ({col_defs}, loaded_at TIMESTAMP DEFAULT NOW())")
        pg.commit()
        wcur.close()

        upd(84, f"Batch inserting {actual:,} rows to warehouse…")
        inserted = batch_insert_df(pg, result_df, out_table, BATCH_INSERT_SIZE)
        upd(99, f"Batch insert done: {inserted:,} rows")

        elapsed = time.time() - t0
        upd(100, f"Done! {actual:,} rows in {elapsed:.1f}s")
        return {"status":"success","engine":"duckdb","rows":actual,"elapsed_s":round(elapsed,1)}
    finally:
        con.close()
        pg.close()


# ── Postgres Runner ───────────────────────────────────────────────────────────

def _run_postgres(pg_hook, input_table, output_name, transforms, task_id):
    safe_out   = re.sub(r\'[^a-z0-9_]\',\'_\',output_name.lower()).strip(\'_\') or "output"
    out_table  = f"warehouse.{safe_out}"
    schema, tname = input_table.split(".",1) if "." in input_table else ("staging", input_table)

    cols = [r[0] for r in pg_hook.get_records(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema=\'{schema}\' AND table_name=\'{tname}\'
        AND column_name NOT IN (\'loaded_at\',\'date_partition\')
        ORDER BY ordinal_position
    """)]

    pg_hook.run("CREATE SCHEMA IF NOT EXISTS warehouse")
    pg_hook.run(f"DROP TABLE IF EXISTS {out_table}")
    pg_hook.run(f"""
        CREATE TABLE {out_table} AS
        WITH _base AS (SELECT {", ".join(f\'"{c}"\' for c in cols)} FROM {input_table})
        SELECT * FROM _base LIMIT 0
    """)

    cur_from = input_table
    step     = 0
    for tx in transforms:
        ntype  = tx.get("type","")
        config = tx.get("config") or {}
        step  += 1
        tmp    = f"staging._etl_{task_id}_s{step}"
        try:
            if   ntype == "filter_rows":
                pg_hook.run(f"CREATE TABLE {tmp} AS SELECT * FROM {cur_from} WHERE {config.get(\'formula\',\'1=1\')}")
            elif ntype == "select_col":
                sc = [c for c in config.get("columns",[]) if c in cols]
                if sc:
                    pg_hook.run(f"CREATE TABLE {tmp} AS SELECT {_q(sc)} FROM {cur_from}"); cols=sc
                else: tmp=cur_from; step-=1; continue
            elif ntype == "drop_col":
                kc = [c for c in cols if c not in set(config.get("columns",[]))]
                pg_hook.run(f"CREATE TABLE {tmp} AS SELECT {_q(kc)} FROM {cur_from}"); cols=kc
            elif ntype == "rename_col":
                rn = config.get("renames",{})
                ex = ", ".join(f\'"{c}" AS "{rn.get(c,c)}"\' for c in cols)
                pg_hook.run(f"CREATE TABLE {tmp} AS SELECT {ex} FROM {cur_from}")
                cols = [rn.get(c,c) for c in cols]
            elif ntype == "group_agg":
                gc=config.get("groupCols",[]); ac=config.get("aggCols",[])
                if gc and ac:
                    ae=", ".join(f\'{a["func"]}("{a["col"]}") AS "{a["alias"]}"\' for a in ac)
                    pg_hook.run(f"CREATE TABLE {tmp} AS SELECT {_q(gc)}, {ae} FROM {cur_from} GROUP BY {_q(gc)}")
                    cols=gc+[a["alias"] for a in ac]
                else: tmp=cur_from; step-=1; continue
            elif ntype == "join_data":
                right_table = config.get("rightTable", "")
                left_col    = config.get("leftCol", "")
                right_col   = config.get("rightCol", "")
                if not (right_table and left_col):
                    tmp = cur_from; step -= 1; continue

                raw_type = config.get("joinType", "INNER JOIN").upper()
                is_cross = "CROSS" in raw_type
                sql_join = raw_type if not is_cross else "CROSS JOIN"

                schema_r, tname_r = right_table.split(".", 1) if "." in right_table else ("staging", right_table)
                r_cols = [r[0] for r in pg_hook.get_records(f"""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_schema=\'{schema_r}\' AND table_name=\'{tname_r.strip(chr(34))}\'
                    ORDER BY ordinal_position
                """)]

                dup = [c for c in r_cols if c in cols and c != right_col]
                left_select  = ", ".join(f\'l."{c}"\' for c in cols)
                right_select = ", ".join(
                    f\'r."{c}" AS "{c}_right"\' if c in dup else f\'r."{c}"\'
                    for c in r_cols
                )

                if is_cross:
                    pg_hook.run(f"CREATE TABLE {tmp} AS SELECT {left_select}, {right_select} FROM {cur_from} l CROSS JOIN {right_table} r")
                elif right_col:
                    pg_hook.run(
                        f"CREATE TABLE {tmp} AS SELECT {left_select}, {right_select} "
                        f'FROM {cur_from} l {sql_join} {right_table} r '
                        f'ON l."{left_col}" = r."{right_col}"'
                    )
                else:
                    tmp = cur_from; step -= 1; continue

                cols = cols + [f"{c}_right" if c in dup else c for c in r_cols]

            else:
                tmp=cur_from; step-=1; continue
        except Exception as e:
            print(f"[PG] step {step} {ntype}: {e}")
            tmp=cur_from; step-=1
        if tmp != cur_from: cur_from=tmp

    pg_hook.run(f"DROP TABLE IF EXISTS {out_table}")
    pg_hook.run(f"CREATE TABLE {out_table} AS SELECT {_q(cols)}, NOW() AS loaded_at FROM {cur_from}")

    for r in pg_hook.get_records(f"""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema=\'staging\' AND table_name LIKE \'_etl_{task_id}_s%\'
    """):
        pg_hook.run(f\'DROP TABLE IF EXISTS staging."{r[0]}"\')


# ── Spark Runner ──────────────────────────────────────────────────────────────

def _run_spark(input_table, output_name, transforms, row_count):
    from pyspark.sql import SparkSession, functions as F, Window

    safe_out = re.sub(r\'[^a-z0-9_]\',\'_\',output_name.lower()).strip(\'_\') or "output"
    optimal_partitions = max(SHUFFLE_PARTITIONS, row_count // 100_000 + 1)
    BROADCAST_MAX_MB = 200

    spark = (SparkSession.builder
        .appName(f"ETLFlow_{DAG_ID}_{TASK_ID}_{safe_out}")
        .config("spark.master","spark://spark:7077")
        .config("spark.jars","/opt/spark/jars/postgresql-42.6.0.jar")
        .config("spark.executor.memory","2g")
        .config("spark.dynamicAllocation.enabled","true")
        .config("spark.dynamicAllocation.maxExecutors","4")
        .config("spark.sql.adaptive.enabled","true")
        .config("spark.sql.adaptive.coalescePartitions.enabled","true")
        .config("spark.sql.shuffle.partitions", str(optimal_partitions))
        .config("spark.default.parallelism", str(optimal_partitions))
        .config("spark.sql.autoBroadcastJoinThreshold", str(BROADCAST_MAX_MB * 1024 * 1024))
        .getOrCreate())

    TYPE_MAP = _resolve_types_spark_map()

    # GANTI: dulu spark.read.jdbc(numPartitions=...) tanpa partitionColumn
    # -> Spark abaikan numPartitions, baca cuma 1 partisi.
    # Sekarang pakai _read_jdbc_table() yang benar-benar paralel.
    df = _read_jdbc_table(spark, input_table, num_partitions=min(8, optimal_partitions))

    for tx in transforms:
        ntype = tx.get("type","")
        cfg   = tx.get("config") or {}
        try:
            if ntype == "filter_rows":
                df = df.filter(F.expr(cfg.get("formula", "1=1")))

            elif ntype == "select_col":
                c = [x for x in cfg.get("columns", []) if x in df.columns]
                if c:
                    df = df.select(*c)

            elif ntype == "drop_col":
                drop = [c for c in cfg.get("columns", []) if c in df.columns]
                if drop:
                    df = df.drop(*drop)

            elif ntype == "rename_col":
                for o, n in cfg.get("renames", {}).items():
                    if o in df.columns:
                        df = df.withColumnRenamed(o, n)

            elif ntype == "add_const":
                # GANTI: dulu F.lit(value) tanpa cast -> selalu jadi string.
                # Sekarang cast sesuai dtype yang dipilih user di UI.
                name  = cfg.get("name", "new_col")
                val   = cfg.get("value", "")
                dtype = TYPE_MAP.get(cfg.get("dtype", "TEXT"), "string")
                df = df.withColumn(name, F.lit(val).cast(dtype))

            # ── BARU: node yang sebelumnya tidak ditangani sama sekali ──
            elif ntype == "set_val":
                target = cfg.get("targetCol")
                if target:
                    if cfg.get("useExpr"):
                        df = df.withColumn(target, F.expr(cfg.get("expr", target)))
                    else:
                        src = cfg.get("sourceCol", target)
                        if src in df.columns:
                            df = df.withColumn(target, F.col(src))

            elif ntype == "val_mapper":
                src, new_col = cfg.get("sourceCol"), cfg.get("newColName", "mapped")
                whens, else_v = cfg.get("whens", []), cfg.get("elseValue", "")
                if src and src in df.columns:
                    expr = None
                    for w in whens:
                        condition = w.get("condition", "=")
                        value     = w.get("value", "")
                        result    = w.get("result", "")
                        if condition not in ("IS NULL", "IS NOT NULL") and not value:
                            continue
                        try:
                            cond_expr = _spark_when_condition(F, src, condition, value)
                        except Exception:
                            continue
                        expr = (F.when(cond_expr, F.lit(result)) if expr is None
                                 else expr.when(cond_expr, F.lit(result)))
                    df = df.withColumn(new_col, expr.otherwise(F.lit(else_v)) if expr is not None else F.lit(else_v))

            elif ntype == "fill_null":
                fc = [c for c in cfg.get("columns", []) if c in df.columns]
                ft = cfg.get("fillType", "value")
                fv = cfg.get("fillValue", "")
                if fc:
                    if ft == "value":
                        df = df.fillna(fv, subset=fc)
                    elif ft == "mean":
                        stats = df.select([F.mean(F.col(c)).alias(c) for c in fc]).collect()[0].asDict()
                        stats = {k: v for k, v in stats.items() if v is not None}
                        if stats: df = df.fillna(stats)
                    elif ft == "median":
                        meds = {}
                        for c in fc:
                            q = df.approxQuantile(c, [0.5], 0.001)
                            if q: meds[c] = q[0]
                        if meds: df = df.fillna(meds)
                    elif ft == "mode":
                        modes = {}
                        for c in fc:
                            row = (df.filter(F.col(c).isNotNull()).groupBy(c).count()
                                     .orderBy(F.desc("count")).limit(1).collect())
                            if row: modes[c] = row[0][c]
                        if modes: df = df.fillna(modes)
                    elif ft in ("forward", "backward"):
                        df = df.withColumn("_rn", F.monotonically_increasing_id())
                        if ft == "forward":
                            win = Window.orderBy("_rn").rowsBetween(Window.unboundedPreceding, 0)
                            fn = F.last
                        else:
                            win = Window.orderBy("_rn").rowsBetween(0, Window.unboundedFollowing)
                            fn = F.first
                        for c in fc:
                            df = df.withColumn(c, fn(F.col(c), ignorenulls=True).over(win))
                        df = df.drop("_rn")

            elif ntype == "order_table":
                orders = cfg.get("orders", [])
                cols = [F.col(o["col"]).asc() if o.get("dir","ASC")=="ASC" else F.col(o["col"]).desc()
                        for o in orders if o.get("col") in df.columns]
                if cols:
                    df = df.orderBy(*cols)

            elif ntype == 'join_data':
                right_table = config.get('rightTable', '')
                left_col    = config.get('leftCol', '')
                right_col   = config.get('rightCol', '')
                r_info      = right_tables.get(right_table)

                if not (right_table and left_col and r_info):
                    step -= 1; continue

                r_alias  = r_info['alias']
                r_cols   = r_info.get('columns', [])
                raw_type = config.get('joinType', 'INNER JOIN').upper()
                is_cross = 'CROSS' in raw_type
                sql_join = 'CROSS JOIN' if is_cross else raw_type.replace(
                    'FULL OUTER JOIN', 'FULL OUTER JOIN'
                )

                # Bangun select list eksplisit agar kolom yang bentrok
                # antara kiri & kanan tidak jadi ambiguous.
                left_select = f'{cur_alias}.*'
                if cur_cols:
                    dup = [c for c in r_cols if c in cur_cols and c != right_col]
                else:
                    dup = [c for c in r_cols if c != right_col]  # asumsi konservatif
                if dup:
                    right_select_parts = ", ".join(
                        f'{r_alias}."{c}" AS "{c}_right"' if c in dup else f'{r_alias}."{c}"'
                        for c in r_cols
                    )
                else:
                    right_select_parts = f'{r_alias}.*'

                select_clause = f'{left_select}, {right_select_parts}'

                if is_cross:
                    cte_parts.append(
                        f'{alias} AS (SELECT {select_clause} FROM {cur_alias} CROSS JOIN {r_alias})'
                    )
                elif right_col:
                    cte_parts.append(
                        f'{alias} AS (SELECT {select_clause} FROM {cur_alias} '
                        f'{sql_join} {r_alias} ON {cur_alias}."{left_col}" = {r_alias}."{right_col}")'
                    )
                else:
                    step -= 1; continue

                cur_cols = None  # kolom berubah signifikan, reset tracking

            else:
                step -= 1; continue
        except Exception as e:
            print(f"[SQL Builder] {ntype} error: {e}")
            step -= 1; continue
        cur_alias = alias

    lc = f" LIMIT {limit}" if limit else ""
    if cte_parts:
        return f"WITH {', '.join(cte_parts)} SELECT * FROM {cur_alias}{lc}"
    return f"SELECT * FROM {input_alias}{lc}"

            elif ntype == "calc":
                new_col = (cfg.get("newColName") or "result").strip()
                col_a, col_b, op = cfg.get("colA"), cfg.get("colB"), cfg.get("operation", "+")
                if new_col and col_a in df.columns and col_b in df.columns:
                    a, b = F.col(col_a).cast("double"), F.col(col_b).cast("double")
                    expr = {"+": a+b, "-": a-b, "*": a*b, "/": F.when(b != 0, a/b)}.get(op, a+b)
                    df = df.withColumn(new_col, expr)

            elif ntype == "adv_calculator":
                SCI = {"sin":F.sin,"cos":F.cos,"sqrt":F.sqrt,"radians":F.radians,
                       "atan2":F.atan2,"power":F.pow}
                for calc in cfg.get("calculations", []):
                    fn    = SCI.get(calc.get("operation","sin"), F.sin)
                    new_c = (calc.get("newColName") or "").strip()
                    col_a, col_b = calc.get("colA"), calc.get("colB")
                    if not new_c or col_a not in df.columns:
                        continue
                    if calc.get("operation") in ("atan2","power") and col_b in df.columns:
                        df = df.withColumn(new_c, fn(F.col(col_a).cast("double"), F.col(col_b).cast("double")))
                    else:
                        df = df.withColumn(new_c, fn(F.col(col_a).cast("double")))

            elif ntype == "combine_cols":
                new_col, sep = (cfg.get("newColName") or "combined").strip(), cfg.get("separator", " ")
                selected = [c for c in cfg.get("selectedCols", []) if c in df.columns]
                remove_orig = cfg.get("removeOriginal", False)
                if new_col and selected:
                    parts = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in selected]
                    combined = parts[0]
                    for p in parts[1:]:
                        combined = F.concat(combined, F.lit(sep), p)
                    df = df.withColumn(new_col, combined)
                    if remove_orig:
                        df = df.drop(*selected)

            elif ntype == "change_type":
                for col, dtype in (cfg.get("types") or {}).items():
                    if col in df.columns:
                        df = df.withColumn(col, F.col(col).cast(TYPE_MAP.get(dtype, "string")))

            elif ntype == "group_agg":
                gc = [c for c in cfg.get("groupCols", []) if c in df.columns]
                ac = cfg.get("aggCols", [])
                if gc and ac:
                    fn_map = {"COUNT":F.count,"SUM":F.sum,"AVG":F.avg,"MIN":F.min,
                              "MAX":F.max,"COUNT DISTINCT":F.countDistinct}
                    aggs = [fn_map.get(a["func"],F.count)(a["col"]).alias(a["alias"])
                            for a in ac if a.get("col") in df.columns]
                    if aggs:
                        df = df.groupBy(*gc).agg(*aggs)

            elif ntype == "pyspark":
                code = cfg.get("code", "")
                if code:
                    ns = {"df": df, "spark": spark, "F": F}
                    exec(code, ns)
                    df = ns.get("df", df)

        except Exception as e:
            print(f"[Spark] {ntype} error: {e} — dilewati")

    df.write.jdbc(url="jdbc:postgresql://postgres:5432/airflow",
                  table=f"warehouse.{safe_out}", mode="overwrite",
                  properties={"user":"airflow","password":"airflow","driver":"org.postgresql.Driver"})

    if row_count > 10_000:
        os.makedirs(PARQUET_DIR, exist_ok=True)
        parquet_path = f"{PARQUET_DIR}/{safe_out}.parquet"
        output_parts = max(1, row_count // 500_000)
        (df.coalesce(output_parts).write.mode("overwrite")
           .option("compression","snappy").parquet(parquet_path))
        print(f"[Spark] Snappy Parquet saved: {parquet_path}")

    spark.stop()

def _resolve_types_spark_map():
    return {
        'TEXT': 'string', 'INTEGER': 'int', 'BIGINT': 'bigint',
        'NUMERIC': 'double', 'BOOLEAN': 'boolean',
        'DATE': 'date', 'TIMESTAMP': 'timestamp', 'VARCHAR(255)': 'string',
    }


def _spark_when_condition(F, col, condition, value):
    """Bangun kondisi Spark untuk semua jenis condition di val_mapper."""
    c = F.col(col)
    if condition == '=':
        return c == value
    if condition == '!=':
        return c != value
    if condition in ('>', '>=', '<', '<='):
        num = float(value)
        c_num = c.cast('double')
        return {'>': c_num > num, '>=': c_num >= num,
                '<': c_num < num, '<=': c_num <= num}[condition]
    if condition == 'LIKE':
        return c.like(value)
    if condition == 'IS NULL':
        return c.isNull()
    if condition == 'IS NOT NULL':
        return c.isNotNull()
    if condition == 'IN':
        vals = [v.strip() for v in str(value).split(',')]
        return c.isin(*vals)
    if condition == 'NOT IN':
        vals = [v.strip() for v in str(value).split(',')]
        return ~c.isin(*vals)
    return c == value


def _read_jdbc_table(spark, table, num_partitions=8):
    """
    Baca tabel Postgres via JDBC dengan partisi PARALEL.
    Tanpa partitionColumn/lowerBound/upperBound, numPartitions
    diabaikan Spark dan baca cuma jalan di 1 partisi.
    """
    JDBC_URL = "jdbc:postgresql://postgres:5432/airflow"
    PROPS    = {"user": "airflow", "password": "airflow", "driver": "org.postgresql.Driver"}

    wrapped = f"(SELECT *, ROW_NUMBER() OVER () AS _partition_key FROM {table}) AS t"
    try:
        return (spark.read.format("jdbc")
                .option("url", JDBC_URL)
                .option("dbtable", wrapped)
                .option("partitionColumn", "_partition_key")
                .option("lowerBound", "1")
                .option("upperBound", "100000000")
                .option("numPartitions", str(num_partitions))
                .options(**PROPS)
                .load()
                .drop("_partition_key"))
    except Exception as e:
        print(f"[Spark] Partitioned read gagal ({e}), fallback ke single-partition read")
        return (spark.read.format("jdbc")
                .option("url", JDBC_URL)
                .option("dbtable", f"(SELECT * FROM {table}) AS t")
                .options(**PROPS)
                .load())

# ── Main Entry Point (called by the workflow DAG) ─────────────────────────────

def run(run_ids, backend_url=BACKEND_URL):
    """
    Entry point called by the workflow DAG.
    run_ids: list of pipeline run IDs for progress reporting.
    """
    tbl      = INPUT_TABLE if "." in INPUT_TABLE else f"staging.{INPUT_TABLE}"
    safe_out = re.sub(r\'[^a-z0-9_]\',\'_\',OUTPUT_NAME.lower()).strip(\'_\') or "output"

    pg_tmp  = _get_conn()
    size_mb = _estimate_mb(pg_tmp, tbl)
    pg_tmp.close()

    if size_mb < 50:
        engine = "postgres"
    elif size_mb < 5000:
        engine = "duckdb"
    else:
        try:
            import importlib.util
            engine = "spark" if importlib.util.find_spec("pyspark") else "duckdb"
        except Exception:
            engine = "duckdb"

    print(f"[Task:{TASK_ID}] {size_mb:.1f}MB | engine={engine}")

    last_pct = [0]
    def progress(pct, msg):
        if pct - last_pct[0] >= 10:
            last_pct[0] = pct
            for run_id in run_ids:
                try:
                    requests.patch(
                        f"{backend_url}/api/pipelines/runs/{run_id}",
                        json={"status":"running","progress_pct":pct,"message":msg},
                        timeout=3,
                    )
                except Exception: pass

    if engine == "duckdb":
        result = _run_duckdb(tbl, safe_out, TRANSFORMS, progress_cb=progress)
        rows   = result.get("rows", 0)
    elif engine == "spark":
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        pg_hook   = PostgresHook(postgres_conn_id="postgres_default")
        row_count = pg_hook.get_first(f"SELECT COUNT(*) FROM {tbl}")[0]
        _run_spark(tbl, safe_out, TRANSFORMS, row_count)
        rows = pg_hook.get_first(f\'SELECT COUNT(*) FROM warehouse."{safe_out}"\')[0]
    else:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        pg_hook   = PostgresHook(postgres_conn_id="postgres_default")
        row_count = pg_hook.get_first(f"SELECT COUNT(*) FROM {tbl}")[0]
        _run_postgres(pg_hook, tbl, safe_out, TRANSFORMS, TASK_ID)
        try: rows = pg_hook.get_first(f\'SELECT COUNT(*) FROM warehouse."{safe_out}"\')[0]
        except Exception: rows = 0

    for run_id in run_ids:
        try:
            requests.patch(
                f"{backend_url}/api/pipelines/runs/{run_id}",
                json={
                    "status":"success", "row_count":rows,
                    "progress_pct":100,
                    "message":f"Done: {rows:,} rows via {engine}",
                },
                timeout=5,
            )
        except Exception as e:
            print(f"[Task:{TASK_ID}] Backend update failed: {e}")

    print(f"[Task:{TASK_ID}] Done → warehouse.{safe_out} ({rows:,} rows via {engine})")
    return rows
'''

    code = code.replace("TASK_ID_PLACEHOLDER",        task_id)
    code = code.replace("DAG_ID_PLACEHOLDER",         dag_id)
    code = code.replace("NOW_PLACEHOLDER",            now_str)
    code = code.replace("INPUT_TABLE_PLACEHOLDER",    safe_input)
    code = code.replace("OUTPUT_NAME_PLACEHOLDER",    safe_out)
    code = code.replace("TRANSFORMS_JSON_PLACEHOLDER", repr(tasks_json))
    code = code.replace("PG_HOST_PLACEHOLDER",        pg_host)
    code = code.replace("PG_PORT_PLACEHOLDER",        str(pg_port))
    code = code.replace("PG_DB_PLACEHOLDER",          pg_db)
    code = code.replace("PG_USER_PLACEHOLDER",        pg_user)
    code = code.replace("PG_PASS_PLACEHOLDER",        pg_pass)

    return code


def generate_workflow_dag(
    dag_id:       str,
    workflow_id:  str,
    workflow_name: str,
    tasks:        list,
    description:  str = "",
    execution_timeout_minutes: int = 90,
) -> str:
    """
    Generate the workflow DAG orchestrator file.
    This file ONLY:
      1. Imports each task module from the tasks/ subfolder
      2. Wraps each task's run() in a PythonOperator
      3. Wires dependencies between tasks
    It contains NO transform logic itself.
    """
    safe_wf_id  = workflow_id.replace("'", "")
    safe_name   = workflow_name.replace("'", "").replace('"', '')
    now_str     = datetime.now().isoformat()

    # Build import lines and task definitions
    import_lines = []
    task_defs    = []
    dep_lines    = []

    for task in tasks:
        tid       = task["task_id"]
        safe_tid  = re.sub(r'[^a-z0-9_]', '_', tid.lower()).strip('_') or f"task_{len(import_lines)+1}"
        module_nm = f"task_{safe_tid}"
        import_lines.append(f"from tasks.{module_nm} import run as run_{safe_tid}")
        task_defs.append(f"""
    {safe_tid}_op = PythonOperator(
        task_id             = '{safe_tid}',
        python_callable     = lambda **ctx: run_{safe_tid}(
            run_ids     = (ctx.get('dag_run').conf or {{}}).get('run_ids', []),
            backend_url = BACKEND_URL,
        ),
        on_failure_callback = _on_failure,
        execution_timeout   = timedelta(minutes={execution_timeout_minutes}),
    )
    airflow_tasks['{safe_tid}'] = {safe_tid}_op""")

    for task in tasks:
        tid      = task["task_id"]
        safe_tid = re.sub(r'[^a-z0-9_]', '_', tid.lower()).strip('_')
        for dep in task.get("depends_on", []):
            safe_dep = re.sub(r'[^a-z0-9_]', '_', dep.lower()).strip('_')
            dep_lines.append(f"    airflow_tasks['{safe_dep}'] >> airflow_tasks['{safe_tid}']")

    imports_block = "\n".join(import_lines)
    tasks_block   = "\n".join(task_defs)
    deps_block    = "\n".join(dep_lines) if dep_lines else "    pass  # no inter-task dependencies"

    dag_code = f'''# Auto-generated WORKFLOW DAG
# Workflow  : {safe_name}
# DAG ID    : {dag_id}
# Generated : {now_str}
# ─────────────────────────────────────────────────────────────────────────────
# This file is the ORCHESTRATOR only.
# It imports task modules from tasks/ and wires them together.
# Transform logic lives in tasks/task_*.py files.
# ─────────────────────────────────────────────────────────────────────────────

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests

# ── Import task modules ───────────────────────────────────────────────────────
{imports_block}

DAG_ID      = {repr(dag_id)}
WORKFLOW_ID = {repr(safe_wf_id)}
BACKEND_URL = "http://backend:8000"

default_args = {{
    'owner'            : 'etlflow',
    'retries'          : 2,
    'retry_delay'      : timedelta(minutes=3),
    'execution_timeout': timedelta(minutes={execution_timeout_minutes}),
}}


def _on_failure(context):
    conf    = context.get("dag_run").conf or {{}}
    run_ids = conf.get("run_ids", [])
    err     = str(context.get("exception", "Unknown"))[:400]
    for run_id in run_ids:
        try:
            requests.patch(
                f"{{BACKEND_URL}}/api/pipelines/runs/{{run_id}}",
                json={{"status": "failed", "message": err}},
                timeout=5,
            )
        except Exception:
            pass


with DAG(
    dag_id           = DAG_ID,
    default_args     = default_args,
    schedule_interval= None,
    start_date       = datetime(2024, 1, 1),
    catchup          = False,
    max_active_tasks = 4,
    tags             = ["etl", "workflow", {repr(safe_wf_id[:50])}],
    description      = {repr(description)},
) as dag:

    airflow_tasks = {{}}

    # ── Register tasks ────────────────────────────────────────────────────────
{tasks_block}

    # ── Wire dependencies ─────────────────────────────────────────────────────
{deps_block}
'''

    return dag_code


# Keep backward-compatible alias
def generate_dag(
    dag_id:       str,
    workflow_id:  str,
    workflow_name: str,
    input_table:  str,
    tasks:        list,
    description:  str = "",
    execution_timeout_minutes: int = 90,
) -> tuple[str, list[tuple[str, str]], dict]:
    """
    Returns (dag_file_content, [(task_filename, task_file_content), ...], task_outputs)
    """
    dag_content = generate_workflow_dag(
        dag_id=dag_id,
        workflow_id=workflow_id,
        workflow_name=workflow_name,
        tasks=tasks,
        description=description,
        execution_timeout_minutes=execution_timeout_minutes,
    )

    # ── Map output table per task_id (dipakai untuk resolusi input antar-task) ──
    task_outputs = {}
    for t in tasks:
        tid = t["task_id"]
        out_name = t.get("output_name", "output")
        safe_out = re.sub(r'[^a-z0-9_]', '_', out_name.lower()).strip('_') or f"out_{tid}"
        if safe_out and safe_out[0].isdigit():
            safe_out = 't_' + safe_out
        task_outputs[tid] = f"warehouse.{safe_out}"

    task_files = []
    for task in tasks:
        task_id    = re.sub(r'[^a-z0-9_]', '_', task["task_id"].lower()).strip('_')
        depends_on = task.get("depends_on", [])

        if task.get("input_table"):
            raw_task_input = task["input_table"]
        elif depends_on:
            parent_id = depends_on[0]
            raw_task_input = task_outputs.get(parent_id, input_table)
        else:
            raw_task_input = input_table

        safe_task_input = re.sub(r'[^a-zA-Z0-9_.]', '', raw_task_input) if raw_task_input else ""

        task_code = generate_task_file(
            task_id=task_id,
            dag_id=dag_id,
            workflow_id=workflow_id,
            input_table=safe_task_input,
            output_name=task.get("output_name", "output"),
            transforms=task.get("transforms", []),
            execution_timeout_minutes=execution_timeout_minutes,
        )
        task_files.append((f"task_{task_id}.py", task_code))

    return dag_content, task_files, task_outputs


generate_spark_dag = generate_dag

# ════════════════════════════════════════════════════════════════════════════
# HEALTH
# ════════════════════════════════════════════════════════════════════════════

@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.now().isoformat()}


# ════════════════════════════════════════════════════════════════════════════
# DIRECTORY BULK IMPORT ENDPOINTS
# ════════════════════════════════════════════════════════════════════════════

@app.get("/api/directory/bulk-preview")
def bulk_preview(directory: str = "/data_csv", recursive: bool = False):
    from upload_worker import scan_all_files
    try:
        files      = scan_all_files(directory, recursive)
        total_size = sum(f["size_bytes"] for f in files)
        return {
            "directory":   directory,
            "total_files": len(files),
            "total_size":  upload_worker._fmt_size(total_size),
            "files":       files,
        }
    except Exception as e:
        raise HTTPException(500, f"Scan failed: {e}")


@app.post("/api/directory/bulk-import")
async def bulk_import(background_tasks: BackgroundTasks, payload: dict):
    from upload_worker import process_bulk_import, scan_all_files
    directory     = payload.get("directory",     "/data_csv")
    recursive     = payload.get("recursive",     False)
    save_parquet  = payload.get("save_parquet",  True)
    skip_existing = payload.get("skip_existing", True)
    file_filter   = payload.get("file_filter",   None)
    try:
        files = scan_all_files(directory, recursive)
    except FileNotFoundError:
        raise HTTPException(404, f"Directory not found: {directory}")
    if file_filter:
        files = [f for f in files if f["name"] in file_filter]
    if not files:
        return {"bulk_id": None, "status": "skipped", "total_files": 0, "message": "No files found."}
    bulk_id = str(uuid.uuid4())
    background_tasks.add_task(process_bulk_import, bulk_id, directory, recursive, save_parquet, skip_existing, file_filter)
    total_size = sum(f["size_bytes"] for f in files)
    return {
        "bulk_id":     bulk_id,
        "status":      "processing",
        "total_files": len(files),
        "total_size":  upload_worker._fmt_size(total_size),
        "directory":   directory,
        "message":     f"Import of {len(files)} files started in background.",
    }


@app.get("/api/directory/bulk-status/{bulk_id}")
def bulk_status(bulk_id: str):
    from upload_worker import get_bulk_job
    job = get_bulk_job(bulk_id)
    if not job:
        raise HTTPException(404, f"Bulk job not found: {bulk_id}")
    return {"bulk_id": bulk_id, **job}


@app.delete("/api/directory/bulk-status/{bulk_id}")
def clear_bulk_job(bulk_id: str):
    from upload_worker import _bulk_jobs
    if bulk_id in _bulk_jobs:
        del _bulk_jobs[bulk_id]
        return {"cleared": True}
    raise HTTPException(404, "Bulk job not found")


# ════════════════════════════════════════════════════════════════════════════
# AIRFLOW
# ════════════════════════════════════════════════════════════════════════════

@app.get("/api/airflow/status")
def airflow_status():
    try:
        r = requests.get(f"{AIRFLOW_URL}/health", timeout=5)
        return {"connected": r.status_code == 200}
    except:
        return {"connected": False}


@app.get("/api/airflow/dags")
def list_dags():
    r = requests.get(f"{AIRFLOW_URL}/api/v1/dags", auth=AIRFLOW_AUTH, timeout=10)
    return r.json()


@app.get("/api/airflow/dags/{dag_id}/runs")
def dag_runs(dag_id: str):
    r = requests.get(
        f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns?limit=10&order_by=-execution_date",
        auth=AIRFLOW_AUTH, timeout=10
    )
    return r.json()


@app.get("/api/airflow/dags/{dag_id}/runs/{run_id}/tasks")
def dag_task_instances(dag_id: str, run_id: str):
    r = requests.get(
        f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances",
        auth=AIRFLOW_AUTH, timeout=10
    )
    return r.json()


@app.post("/api/airflow/dags/{dag_id}/trigger")
def trigger_dag(dag_id: str, force: bool = False):
    r = requests.post(
        f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns",
        auth=AIRFLOW_AUTH,
        json={"conf": {"force": force}},
        timeout=10
    )
    return r.json()


# ════════════════════════════════════════════════════════════════════════════
# DATASETS
# ════════════════════════════════════════════════════════════════════════════

def ensure_datasets_table(cur, conn):
    cur.execute("CREATE SCHEMA IF NOT EXISTS meta")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS meta.datasets (
            id          SERIAL PRIMARY KEY,
            name        TEXT NOT NULL,
            type        TEXT NOT NULL,
            status      TEXT DEFAULT 'pending',
            row_count   INTEGER,
            col_count   INTEGER,
            file_size   TEXT,
            file_size_bytes BIGINT,
            table_name  TEXT,
            parquet_path TEXT,
            is_large    BOOLEAN DEFAULT FALSE,
            created_at  TIMESTAMP DEFAULT NOW(),
            updated_at  TIMESTAMP DEFAULT NOW()
        )
    """)
    for col, dtype in [
        ("col_count", "INTEGER"), ("file_size_bytes", "BIGINT"),
        ("parquet_path", "TEXT"), ("is_large", "BOOLEAN DEFAULT FALSE"),
    ]:
        try:
            cur.execute(f"ALTER TABLE meta.datasets ADD COLUMN IF NOT EXISTS {col} {dtype}")
        except:
            pass
    conn.commit()


@app.get("/api/datasets")
def list_datasets():
    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    ensure_datasets_table(cur, conn)
    cur.execute("SELECT * FROM meta.datasets ORDER BY created_at DESC")
    rows = cur.fetchall()
    cur.close(); conn.close()
    return [dict(r) for r in rows]


@app.delete("/api/datasets/{dataset_id}")
def delete_dataset(dataset_id: int):
    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT table_name, parquet_path FROM meta.datasets WHERE id = %s", (dataset_id,))
    row = cur.fetchone()
    if row and row["table_name"]:
        try: cur.execute(f'DROP TABLE IF EXISTS staging."{row["table_name"]}"')
        except: pass
    if row and row.get("parquet_path"):
        try: Path(row["parquet_path"]).unlink(missing_ok=True)
        except: pass
    cur.execute("DELETE FROM meta.datasets WHERE id = %s", (dataset_id,))
    conn.commit()
    cur.close(); conn.close()
    return {"deleted": True}


@app.post("/api/datasets/upload")
async def upload_dataset(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    name: Optional[str] = Form(None),
):
    filename = name or file.filename or "upload"
    job_id   = str(uuid.uuid4())
    tmp_dir  = "/tmp/etlflow_uploads"
    os.makedirs(tmp_dir, exist_ok=True)
    ext      = filename.rsplit(".", 1)[-1].lower() if "." in filename else "csv"
    tmp_path = f"{tmp_dir}/{job_id}.{ext}"
    file_size_bytes = 0
    try:
        with open(tmp_path, "wb") as f:
            while True:
                chunk = await file.read(8 * 1024 * 1024)
                if not chunk: break
                f.write(chunk)
                file_size_bytes += len(chunk)
    except Exception as e:
        raise HTTPException(500, f"Failed to save file: {e}")
    upload_worker._set(job_id,
        status="queued", pct=2,
        message=f"File received ({file_size_bytes/1024/1024:.1f} MB), processing…",
        filename=filename, file_size_bytes=file_size_bytes,
    )
    background_tasks.add_task(upload_worker.process_upload, job_id, tmp_path, filename, file_size_bytes)
    return {"job_id": job_id, "status": "processing", "filename": filename,
            "size_bytes": file_size_bytes, "message": "Upload received"}


@app.get("/api/datasets/upload/status/{job_id}")
def upload_status(job_id: str):
    job = upload_worker.get_job(job_id)
    if not job: raise HTTPException(404, "Job not found")
    return job


@app.post("/api/datasets/connect-db")
def connect_db(payload: dict):
    try:
        test = psycopg2.connect(
            host=payload["host"], port=payload.get("port", 5432),
            database=payload["database"], user=payload["username"],
            password=payload["password"], connect_timeout=5
        )
        test.close()
    except Exception as e:
        raise HTTPException(400, f"Connection failed: {e}")
    conn = get_conn()
    cur  = conn.cursor()
    ensure_datasets_table(cur, conn)
    cur.execute(
        "INSERT INTO meta.datasets (name, type, status) VALUES (%s, %s, 'connected') RETURNING id",
        (f"{payload['database']}@{payload['host']}", payload.get("db_type","PostgreSQL").upper())
    )
    conn.commit(); cur.close(); conn.close()
    return {"connected": True}


@app.get("/api/datasets/{dataset_id}/preview")
def preview_dataset(dataset_id: int, limit: int = 100):
    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM meta.datasets WHERE id = %s", (dataset_id,))
    ds = cur.fetchone()
    if not ds or not ds["table_name"]: raise HTTPException(404, "Dataset not found")
    cur.execute(f'SELECT * FROM staging."{ds["table_name"]}" LIMIT %s', (limit,))
    rows    = cur.fetchall()
    columns = [d[0] for d in cur.description]
    cur.close(); conn.close()
    return {"columns": columns, "rows": [dict(r) for r in rows], "total": ds["row_count"]}


@app.get("/api/datasets/{dataset_id}/download")
def download_dataset(dataset_id: int, format: str = "csv"):
    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM meta.datasets WHERE id = %s", (dataset_id,))
    ds = cur.fetchone()
    if not ds or not ds["table_name"]: raise HTTPException(404, "Dataset not found")
    cur.execute(f'SELECT * FROM staging."{ds["table_name"]}"')
    rows    = cur.fetchall()
    columns = [d[0] for d in cur.description]
    cur.close(); conn.close()
    df      = pd.DataFrame([dict(r) for r in rows], columns=columns)
    out_dir = "/tmp/etlflow_exports"
    os.makedirs(out_dir, exist_ok=True)
    if format == "parquet":
        path = f"{out_dir}/{ds['table_name']}.parquet"
        df.to_parquet(path, index=False, compression="snappy")
        return FileResponse(path, filename=f"{ds['table_name']}.parquet", media_type="application/octet-stream")
    path = f"{out_dir}/{ds['table_name']}.csv"
    df.to_csv(path, index=False)
    return FileResponse(path, filename=f"{ds['table_name']}.csv", media_type="text/csv")


# ════════════════════════════════════════════════════════════════════════════
# WAREHOUSE
# ════════════════════════════════════════════════════════════════════════════

@app.get("/api/warehouse/tables")
def warehouse_tables():
    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("CREATE SCHEMA IF NOT EXISTS warehouse"); conn.commit()
        cur.execute("""
            SELECT t.table_name, COUNT(c.column_name) as col_count
            FROM information_schema.tables t
            LEFT JOIN information_schema.columns c
              ON c.table_schema=t.table_schema AND c.table_name=t.table_name
            WHERE t.table_schema='warehouse' AND t.table_type='BASE TABLE'
            GROUP BY t.table_name ORDER BY t.table_name
        """)
        return [dict(r) for r in cur.fetchall()]
    except: return []
    finally: cur.close(); conn.close()


@app.get("/api/warehouse/{table_name}/download")
def download_warehouse_table(table_name: str, format: str = "csv"):
    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute(f'SELECT * FROM warehouse."{table_name}"')
        rows    = cur.fetchall()
        columns = [d[0] for d in cur.description]
        df      = pd.DataFrame([dict(r) for r in rows], columns=columns)
        out_dir = "/tmp/etlflow_exports"
        os.makedirs(out_dir, exist_ok=True)
        if format == "parquet":
            path = f"{out_dir}/{table_name}.parquet"
            df.to_parquet(path, index=False, compression="snappy")
            return FileResponse(path, filename=f"{table_name}.parquet", media_type="application/octet-stream")
        path = f"{out_dir}/{table_name}.csv"
        df.to_csv(path, index=False)
        return FileResponse(path, filename=f"{table_name}.csv", media_type="text/csv")
    finally: cur.close(); conn.close()


@app.post("/api/directory/preview")
def preview_directory_file(payload: dict):
    file_path = payload.get("file_path", "")
    limit     = int(payload.get("limit", 100))
    if not file_path: raise HTTPException(400, "file_path required")
    try:
        df = upload_worker.read_file_from_path(file_path)
        df_preview = df.head(limit).where(pd.notnull(df.head(limit)), None)
        return {"file_path": file_path, "columns": list(df_preview.columns),
                "rows": df_preview.to_dict(orient="records"),
                "total_rows": len(df), "total_cols": len(df.columns)}
    except FileNotFoundError as e: raise HTTPException(404, str(e))
    except Exception as e: raise HTTPException(400, f"Failed to read file: {e}")


@app.get("/api/parquet/list")
def list_parquet_files():
    files = []
    if os.path.exists(PARQUET_DIR):
        for f in sorted(Path(PARQUET_DIR).glob("*.parquet")):
            meta_path = f.with_suffix("").with_name(f.stem + ".meta.json")
            meta = {}
            if meta_path.exists():
                try:
                    with open(meta_path) as mf:
                        meta = json.load(mf)
                except: pass
            files.append({
                "name":       f.name,
                "path":       str(f),
                "size_bytes": f.stat().st_size,
                "size_str":   f"{f.stat().st_size/1024/1024:.1f}MB",
                **meta,
            })
    return {"parquet_dir": PARQUET_DIR, "total_files": len(files), "files": files}


@app.get("/api/parquet/download")
def download_parquet_file(file_path: str, format: str = "parquet"):
    resolved = os.path.realpath(file_path)
    if not resolved.startswith(os.path.realpath(PARQUET_DIR)):
        raise HTTPException(403, "File outside Parquet directory not allowed")
    p = Path(file_path)
    if not p.exists(): raise HTTPException(404, "File not found")
    if format == "csv":
        df       = pd.read_parquet(file_path, engine="pyarrow")
        out_dir  = "/tmp/etlflow_exports"
        os.makedirs(out_dir, exist_ok=True)
        out_path = f"{out_dir}/{p.stem}.csv"
        df.to_csv(out_path, index=False)
        return FileResponse(out_path, filename=f"{p.stem}.csv", media_type="text/csv")
    return FileResponse(file_path, filename=p.name, media_type="application/octet-stream")


# ════════════════════════════════════════════════════════════════════════════
# PIPELINE — Writes split DAG + Task files
# ════════════════════════════════════════════════════════════════════════════

def ensure_pipeline_runs_table(cur, conn):
    cur.execute("CREATE SCHEMA IF NOT EXISTS meta")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS meta.pipeline_runs (
            id            SERIAL PRIMARY KEY,
            dag_id        TEXT,
            task_id       TEXT,
            workflow_id   TEXT,
            workflow_name TEXT,
            input_table   TEXT,
            output_table  TEXT,
            row_count     INTEGER,
            status        TEXT DEFAULT 'pending',
            progress_pct  INTEGER DEFAULT 0,
            message       TEXT,
            ran_at        TIMESTAMP DEFAULT NOW(),
            finished_at   TIMESTAMP
        )
    """)
    for col, dtype in [
        ("task_id","TEXT"), ("progress_pct","INTEGER DEFAULT 0"), ("message","TEXT")
    ]:
        try: cur.execute(f"ALTER TABLE meta.pipeline_runs ADD COLUMN IF NOT EXISTS {col} {dtype}")
        except: pass
    conn.commit()


@app.post("/api/pipelines/run")
def run_pipeline(payload: dict):
    """
    Trigger a pipeline run.

    Struktur folder yang dihasilkan (per-workflow):
        DAGS_FOLDER/
        └── {dag_id}/
            ├── {dag_id}.py         ← workflow orchestrator (dibaca Airflow)
            └── tasks/
                ├── __init__.py     ← agar tasks/ jadi Python package
                └── task_{task_id}.py  ← satu file per output node
    """
    workflow_id   = payload.get("workflow_id",   f"wf_{int(time.time())}")
    workflow_name = payload.get("workflow_name", "Pipeline")
    input_table   = payload.get("input_table",  "")
    tasks         = payload.get("tasks",         [])
    description   = payload.get("description",  "")
    timeout_min   = int(payload.get("execution_timeout_minutes", 90))

    # Legacy single-task support
    if not tasks and payload.get("output_name"):
        tasks = [{
            "task_id":     "task_1",
            "output_name": payload.get("output_name", "output"),
            "transforms":  payload.get("transforms", []),
            "depends_on":  [],
        }]

    if not tasks:
        raise HTTPException(400, "No tasks defined. Add at least one Output Dataset node.")
    if not input_table:
        raise HTTPException(400, "input_table is required. Configure an Input Dataset node.")

    safe_input   = re.sub(r'[^a-zA-Z0-9_.]', '', input_table)
    safe_wf_name = re.sub(r'[^a-z0-9_]', '_', workflow_name.lower().strip())
    safe_wf_name = re.sub(r'_+', '_', safe_wf_name).strip('_')[:60]
    dag_id       = safe_wf_name or re.sub(r'[^a-z0-9_]', '_', workflow_id.lower())[:60]
    if not dag_id:
        dag_id = f"etl_pipeline_{int(time.time())}"

    # ── Buat folder khusus workflow ini + subfolder tasks/ ──────────────────
    workflow_dir = Path(DAGS_FOLDER) / dag_id
    tasks_dir    = workflow_dir / "tasks"
    try:
        os.makedirs(tasks_dir, exist_ok=True)
        init_file = tasks_dir / "__init__.py"
        if not init_file.exists():
            init_file.write_text("# Auto-generated — task modules package\n")
    except Exception as e:
        print(f"[DAG] Warning: could not create workflow folder {workflow_dir}: {e}")

    # ── Cek apakah DAG sudah terdaftar di Airflow ───────────────────────────
    dag_exists = False
    try:
        r = requests.get(f"{AIRFLOW_URL}/api/v1/dags/{dag_id}", auth=AIRFLOW_AUTH, timeout=5)
        dag_exists = r.status_code == 200
    except Exception as e:
        print(f"[DAG] Could not check Airflow: {e}")

    # ── Generate isi file DAG + file-file task ──────────────────────────────
    try:
        dag_content, task_files, task_outputs = generate_dag(
            dag_id=dag_id,
            workflow_id=workflow_id,
            workflow_name=workflow_name,
            input_table=safe_input,
            tasks=tasks,
            description=description,
            execution_timeout_minutes=timeout_min,
        )
    except Exception as e:
        raise HTTPException(500, f"File generation failed: {str(e)}\n{traceback.format_exc()}")

    # ── Tulis file task ke {dag_id}/tasks/ ──────────────────────────────────
    written_task_files = []
    write_failed = False
    for filename, content in task_files:
        task_path = tasks_dir / filename
        try:
            task_path.write_text(content, encoding="utf-8")
            written_task_files.append(str(task_path))
            print(f"[DAG] Task file written: {task_path}")
        except Exception as e:
            write_failed = True
            print(f"[DAG] Warning: could not write {task_path}: {e}")

    # ── Tulis file DAG utama ke {dag_id}/{dag_id}.py ────────────────────────
    dag_path = workflow_dir / f"{dag_id}.py"
    try:
        dag_path.write_text(dag_content, encoding="utf-8")
        print(f"[DAG] Written: {dag_path}")
    except Exception as e:
        # Fallback: tulis ke /tmp dengan struktur folder yang sama
        try:
            fallback_dir   = Path("/tmp") / dag_id
            fallback_tasks = fallback_dir / "tasks"
            os.makedirs(fallback_tasks, exist_ok=True)
            (fallback_tasks / "__init__.py").write_text("# Auto-generated\n")

            for filename, content in task_files:
                (fallback_tasks / filename).write_text(content, encoding="utf-8")

            fallback_dag = fallback_dir / f"{dag_id}.py"
            fallback_dag.write_text(dag_content, encoding="utf-8")
            print(f"[DAG] Fallback written to: {fallback_dag}")
            dag_path = fallback_dag
        except Exception as e2:
            raise HTTPException(500, f"Failed to write DAG file: {e}. Fallback also failed: {e2}")

    # ── Simpan record pipeline_runs ──────────────────────────────────────────
    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    run_ids = []
    try:
        ensure_pipeline_runs_table(cur, conn)
        for task in tasks:
            task_id    = task.get("task_id", "task_1")
            depends_on = task.get("depends_on", [])

            if task.get("input_table"):
                raw_task_input = task["input_table"]
            elif depends_on:
                parent_id = depends_on[0]
                raw_task_input = task_outputs.get(parent_id, input_table)
            else:
                raw_task_input = input_table

            safe_task_input = re.sub(r'[^a-zA-Z0-9_.]', '', raw_task_input) if raw_task_input else ""

            safe_out = re.sub(r'[^a-z0-9_]', '_', task.get("output_name", "output").lower())
            if safe_out and safe_out[0].isdigit():
                safe_out = 't_' + safe_out
            safe_out = safe_out or 'output'

            cur.execute("""
                INSERT INTO meta.pipeline_runs
                    (dag_id, task_id, workflow_id, workflow_name, input_table, output_table, status)
                VALUES (%s, %s, %s, %s, %s, %s, 'pending') RETURNING id
            """, (dag_id, task_id, workflow_id, workflow_name,
                  safe_task_input, f"warehouse.{safe_out}"))
            run_ids.append(cur.fetchone()["id"])
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, f"Failed to save run records: {str(e)}")
    finally:
        cur.close(); conn.close()

    # ── Tunggu Airflow mendeteksi DAG baru ──────────────────────────────────
    if not dag_exists:
        print(f"[DAG] Waiting for Airflow to detect {dag_id}…")
        detected = False
        for i in range(30):
            time.sleep(1)
            try:
                r = requests.get(f"{AIRFLOW_URL}/api/v1/dags/{dag_id}", auth=AIRFLOW_AUTH, timeout=5)
                if r.status_code == 200 and r.json().get("dag_id"):
                    print(f"[DAG] Detected after {i+1}s")
                    detected = True
                    break
            except:
                pass
        if not detected:
            print(f"[DAG] Warning: DAG not detected after 30s. Will try to trigger anyway.")
    else:
        time.sleep(1)

    # ── Unpause + trigger DAG ────────────────────────────────────────────────
    try:
        requests.patch(
            f"{AIRFLOW_URL}/api/v1/dags/{dag_id}", auth=AIRFLOW_AUTH,
            json={"is_paused": False}, timeout=5
        )
        time.sleep(1)
    except Exception as e:
        print(f"[DAG] Unpause warning: {e}")

    dag_run = {}
    try:
        r = requests.post(
            f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns",
            auth=AIRFLOW_AUTH,
            json={"conf": {"run_ids": run_ids}},
            timeout=10
        )
        dag_run = r.json()
        print(f"[DAG] Triggered: {dag_run}")
    except Exception as e:
        dag_run = {"error": str(e)}
        print(f"[DAG] Trigger warning: {e}")

    return {
        "run_ids":    run_ids,
        "run_id":     run_ids[0] if run_ids else None,
        "dag_id":     dag_id,
        "workflow_folder": str(workflow_dir),
        "dag_file":   f"{dag_id}/{dag_id}.py",
        "task_files": [f"{dag_id}/tasks/{fn}" for fn, _ in task_files],
        "dag_run":    dag_run,
        "status":     "triggered",
        "is_new":     not dag_exists,
    }


@app.get("/api/pipelines/runs")
def list_pipeline_runs():
    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        ensure_pipeline_runs_table(cur, conn)
        cur.execute("""
            SELECT id, dag_id, task_id, workflow_id, workflow_name,
                   input_table, output_table, row_count, status,
                   COALESCE(progress_pct,0) as progress_pct,
                   message, ran_at::text, finished_at::text
            FROM meta.pipeline_runs ORDER BY ran_at DESC LIMIT 100
        """)
        return [dict(r) for r in cur.fetchall()]
    except: return []
    finally: cur.close(); conn.close()


@app.patch("/api/pipelines/runs/{run_id}")
def update_pipeline_run(run_id: int, payload: dict):
    conn = get_conn()
    cur  = conn.cursor()
    try:
        ensure_pipeline_runs_table(cur, conn)
        cur.execute("""
            UPDATE meta.pipeline_runs SET
                status       = COALESCE(%s, status),
                row_count    = COALESCE(%s, row_count),
                progress_pct = COALESCE(%s, progress_pct),
                message      = COALESCE(%s, message),
                finished_at  = CASE WHEN %s IN ('success','failed') THEN NOW() ELSE finished_at END
            WHERE id = %s
        """, (payload.get("status"), payload.get("row_count"),
              payload.get("progress_pct"), payload.get("message"),
              payload.get("status",""), run_id))
        conn.commit()
        return {"updated": True}
    finally: cur.close(); conn.close()


@app.get("/api/pipelines/runs/{run_id}/preview")
def preview_run(run_id: int, limit: int = 100):
    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("SELECT output_table, status FROM meta.pipeline_runs WHERE id=%s", (run_id,))
        row = cur.fetchone()
        if not row: raise HTTPException(404, "Run not found")
        if row["status"] not in ("success","completed"):
            raise HTTPException(400, f"Pipeline not finished (status: {row['status']})")
        table = row["output_table"]
        if "." not in table: raise HTTPException(400, f"Invalid table: {table}")
        cur.execute(f'SELECT * FROM {table} LIMIT %s', (limit,))
        rows    = cur.fetchall()
        columns = [d[0] for d in cur.description]
        return {"columns": columns, "rows": [dict(r) for r in rows], "table": table}
    except HTTPException: raise
    except Exception as e: raise HTTPException(400, str(e))
    finally: cur.close(); conn.close()


@app.get("/api/pipelines/runs/{run_id}/download")
def download_run_output(run_id: int, format: str = "csv"):
    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("SELECT output_table, status FROM meta.pipeline_runs WHERE id=%s", (run_id,))
        row = cur.fetchone()
        if not row or row["status"] not in ("success","completed"):
            raise HTTPException(400, "Pipeline output not available")
        table = row["output_table"]
        cur.execute(f'SELECT * FROM {table}')
        rows    = cur.fetchall()
        columns = [d[0] for d in cur.description]
        df      = pd.DataFrame([dict(r) for r in rows], columns=columns)
        tname   = table.replace("warehouse.","").strip('"')
        out_dir = "/tmp/etlflow_exports"
        os.makedirs(out_dir, exist_ok=True)
        if format == "parquet":
            path = f"{out_dir}/{tname}.parquet"
            df.to_parquet(path, index=False, compression="snappy")
            return FileResponse(path, filename=f"{tname}.parquet", media_type="application/octet-stream")
        path = f"{out_dir}/{tname}.csv"
        df.to_csv(path, index=False)
        return FileResponse(path, filename=f"{tname}.csv", media_type="text/csv")
    finally: cur.close(); conn.close()


@app.get("/api/pipelines/runs/{run_id}/dag-status")
def get_dag_status(run_id: int):
    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("SELECT dag_id FROM meta.pipeline_runs WHERE id=%s", (run_id,))
        row = cur.fetchone()
        if not row: raise HTTPException(404, "Run not found")
        dag_id = row["dag_id"]
    finally: cur.close(); conn.close()
    try:
        r    = requests.get(
            f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns?limit=1&order_by=-execution_date",
            auth=AIRFLOW_AUTH, timeout=10
        )
        runs = r.json().get("dag_runs", [])
        run  = runs[0] if runs else {}
        tasks_state = {}
        if run.get("dag_run_id"):
            tr = requests.get(
                f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns/{run['dag_run_id']}/taskInstances",
                auth=AIRFLOW_AUTH, timeout=10
            )
            for t in tr.json().get("task_instances",[]):
                tasks_state[t["task_id"]] = t["state"]
        return {"dag_id": dag_id, "state": run.get("state","unknown"),
                "dag_run_id": run.get("dag_run_id"), "tasks": tasks_state}
    except Exception as e:
        return {"dag_id": dag_id, "state": "unknown", "error": str(e)}


# ════════════════════════════════════════════════════════════════════════════
# DAG FILES INTROSPECTION — list generated files for a workflow
# ════════════════════════════════════════════════════════════════════════════

@app.get("/api/pipelines/dag-files/{dag_id}")
def list_dag_files(dag_id: str):
    """Return the list of generated files for a given workflow (DAG + tasks)."""
    workflow_dir = Path(DAGS_FOLDER) / dag_id
    files = []

    dag_file = workflow_dir / f"{dag_id}.py"
    if dag_file.exists():
        files.append({
            "role":     "workflow_dag",
            "filename": dag_file.name,
            "path":     str(dag_file),
            "size":     dag_file.stat().st_size,
        })

    tasks_dir = workflow_dir / "tasks"
    if tasks_dir.exists():
        for tf in sorted(tasks_dir.glob("task_*.py")):
            files.append({
                "role":     "task",
                "filename": tf.name,
                "path":     str(tf),
                "size":     tf.stat().st_size,
            })

    if not files:
        raise HTTPException(404, f"No files found for workflow '{dag_id}'")

    return {"dag_id": dag_id, "workflow_folder": str(workflow_dir), "files": files}


@app.post("/api/preview/spark-pipeline")
def preview_spark_pipeline(payload: dict):
    """
    Preview hasil node (utility atau output) SEBELUM Run Pipeline dijalankan.
    Menggunakan Spark + shared-node caching + broadcast join + dynamic config.
    """
    nodes          = payload.get("nodes", [])
    edges          = payload.get("edges", [])
    target_node_id = payload.get("target_node_id")
    limit          = int(payload.get("limit", 100))

    if not target_node_id:
        raise HTTPException(400, "target_node_id required")

    input_node = next((n for n in nodes if n.get("data", {}).get("type") == "input_dataset"), None)
    if not input_node or not input_node.get("data", {}).get("config", {}).get("dataset"):
        raise HTTPException(400, "Configure the Input Dataset node first")

    ds = input_node["data"]["config"]["dataset"]
    table_name = ds.get("table_name") or re.sub(r'\.[^.]+$', '', ds.get("name", "")).lower().replace(" ", "_")
    input_table = f'staging."{table_name}"'

    conn = get_conn()
    cur  = conn.cursor()
    try:
        cur.execute("SELECT pg_total_relation_size(%s) / 1024.0 / 1024.0", (input_table,))
        size_mb = float(cur.fetchone()[0] or 0)
    except Exception:
        size_mb = (ds.get("file_size_bytes") or 0) / (1024 * 1024) or 100
    finally:
        cur.close(); conn.close()

    try:
        return spark_engine.preview_pipeline_spark(
            input_table=input_table, size_mb=size_mb,
            nodes=nodes, edges=edges,
            target_node_id=target_node_id, limit=limit,
        )
    except Exception as e:
        raise HTTPException(500, f"Spark preview failed: {e}\n{traceback.format_exc()}")

# ════════════════════════════════════════════════════════════════════════════
# TRANSFORM PREVIEW (DuckDB-powered, instant)
# ════════════════════════════════════════════════════════════════════════════

@app.post("/api/preview/transform")
def preview_transform(payload: dict):
    """Preview transform result — uses DuckDB if available (much faster)."""
    dataset_id = payload.get("dataset_id")
    transforms = payload.get("transforms", [])
    limit      = payload.get("limit", 50)
    if not dataset_id: raise HTTPException(400, "dataset_id required")

    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("SELECT * FROM meta.datasets WHERE id = %s", (dataset_id,))
        ds = cur.fetchone()
        if not ds: raise HTTPException(404, "Dataset not found")
        table_name = ds["table_name"]
        tbl        = f'staging."{table_name}"'

        if _duckdb_available():
            import duckdb
            sample_df = pd.read_sql(f"SELECT * FROM {tbl} LIMIT 5000", conn)
            dcon      = duckdb.connect(":memory:")
            dcon.register("_input", sample_df)

            # ── Baca & register tabel kanan untuk join_data ──────────────
            right_tables = {}
            for idx, tx in enumerate(transforms):
                if tx.get("type") == "join_data":
                    r_table = (tx.get("config") or {}).get("rightTable")
                    if r_table and r_table not in right_tables:
                        r_df = pd.read_sql(f"SELECT * FROM {r_table} LIMIT 5000", conn)
                        r_alias = f"_right_{idx}"
                        dcon.register(r_alias, r_df)
                        right_tables[r_table] = {"alias": r_alias, "columns": list(r_df.columns)}

            sql    = _build_duckdb_sql("_input", transforms, limit=limit, right_tables=right_tables)
            result = dcon.execute(sql).df()
            dcon.close()
            return {"columns": list(result.columns), "rows": result.to_dict(orient="records")}
            schema_cur = conn.cursor()
        schema_cur.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='staging' AND table_name='{table_name}'
            ORDER BY ordinal_position
        """)
        cur_cols   = [r[0] for r in schema_cur.fetchall()]
        current    = tbl
        cte_parts  = []
        step       = 0

        for tx in transforms:
            ntype  = tx.get("type",""); config = tx.get("config") or {}
            step  += 1; alias = f"step_{step}"
            prev   = f"step_{step-1}" if step > 1 else current
            try:
                if   ntype == "filter_rows":
                    cte_parts.append(f"{alias} AS (SELECT * FROM {prev} WHERE {config.get('formula','1=1')})")
                elif ntype == "select_col":
                    cols = [c for c in config.get("columns", cur_cols) if c in cur_cols]
                    if cols:
                        cte_parts.append(f"{alias} AS (SELECT {_q(cols)} FROM {prev})")
                        cur_cols = cols
                    else: step -= 1; continue
                elif ntype == "drop_col":
                    keep = [c for c in cur_cols if c not in set(config.get("columns",[]))]
                    cte_parts.append(f"{alias} AS (SELECT {_q(keep)} FROM {prev})")
                    cur_cols = keep
                elif ntype == "rename_col":
                    rn   = config.get("renames",{})
                    ex   = ", ".join(f'"{c}" AS "{rn.get(c,c)}"' for c in cur_cols)
                    cte_parts.append(f"{alias} AS (SELECT {ex} FROM {prev})")
                    cur_cols = [rn.get(c,c) for c in cur_cols]
                elif ntype == "group_agg":
                    gc=config.get("groupCols",[]); ac=config.get("aggCols",[])
                    if gc and ac:
                        ae=", ".join(f'{a["func"]}("{a["col"]}") AS "{a["alias"]}"' for a in ac)
                        cte_parts.append(f"{alias} AS (SELECT {_q(gc)}, {ae} FROM {prev} GROUP BY {_q(gc)})")
                        cur_cols = gc + [a["alias"] for a in ac]
                    else: step -= 1; continue

                elif ntype == "join_data":
                    right_table = config.get("rightTable", "")
                    left_col    = config.get("leftCol", "")
                    right_col   = config.get("rightCol", "")
                    if not (right_table and left_col):
                        step -= 1; continue

                    schema_r, tname_r = right_table.split(".", 1) if "." in right_table else ("staging", right_table)
                    tname_r_clean = tname_r.strip('"')
                    r_cur = conn.cursor()
                    r_cur.execute("""
                        SELECT column_name FROM information_schema.columns
                        WHERE table_schema = %s AND table_name = %s
                        ORDER BY ordinal_position
                    """, (schema_r, tname_r_clean))
                    r_cols = [r[0] for r in r_cur.fetchall()]
                    r_cur.close()

                    raw_type = config.get("joinType", "INNER JOIN").upper()
                    is_cross = "CROSS" in raw_type
                    sql_join = "CROSS JOIN" if is_cross else raw_type

                    dup = [c for c in r_cols if c in cur_cols and c != right_col]
                    left_select  = ", ".join(f'l."{c}"' for c in cur_cols)
                    right_select = ", ".join(
                        f'r."{c}" AS "{c}_right"' if c in dup else f'r."{c}"'
                        for c in r_cols
                    )

                    if is_cross:
                        cte_parts.append(
                            f"{alias} AS (SELECT {left_select}, {right_select} FROM {prev} l CROSS JOIN {right_table} r)"
                        )
                    elif right_col:
                        cte_parts.append(
                            f'{alias} AS (SELECT {left_select}, {right_select} '
                            f'FROM {prev} l {sql_join} {right_table} r ON l."{left_col}" = r."{right_col}")'
                        )
                    else:
                        step -= 1; continue

                    cur_cols = cur_cols + [f"{c}_right" if c in dup else c for c in r_cols]

                else: step -= 1; continue
            except Exception as e:
                cte_parts.append(f"{alias} AS (SELECT * FROM {prev})")

        last = f"step_{step}" if cte_parts else current
        sql  = (f"WITH {', '.join(cte_parts)} SELECT * FROM {last} LIMIT {limit}"
                if cte_parts else f"SELECT * FROM {current} LIMIT {limit}")
        cur.execute(sql)
        rows    = cur.fetchall()
        columns = [d[0] for d in cur.description]
        return {"columns": columns, "rows": [dict(r) for r in rows]}

    except HTTPException: raise
    except Exception as e: raise HTTPException(400, f"Preview failed: {e}")
    finally: cur.close(); conn.close()

# # ── DAG Generator (Spark-based) ──────────────────────────────────────
# def generate_spark_dag(dag_id, workflow_id, workflow_name, input_table, tasks, description=""):
#     """
#     Generate a multi-task Spark DAG.
#     tasks: list of {task_id, output_name, transforms, depends_on}
#     """
#     tasks_json = json.dumps(tasks, ensure_ascii=True)
#     safe_wf_id = workflow_id.replace("'", "")
#     safe_name  = workflow_name.replace("'", "").replace('"', '')
#     now_str    = datetime.now().isoformat()
#     safe_input = re.sub(r'[^a-zA-Z0-9_.]', '', input_table)

#     lines = []
#     lines.append(f"# Auto-generated Spark DAG: {dag_id}")
#     lines.append(f"# Workflow: {safe_name}")
#     lines.append(f"# Generated: {now_str}")
#     lines.append("")
#     lines.append("from airflow import DAG")
#     lines.append("from airflow.operators.python import PythonOperator")
#     lines.append("from airflow.providers.postgres.hooks.postgres import PostgresHook")
#     lines.append("from datetime import datetime")
#     lines.append("import json, requests, os, math")
#     lines.append("")
#     lines.append(f'DAG_ID      = {repr(dag_id)}')
#     lines.append(f'INPUT_TABLE = {repr(safe_input)}')
#     lines.append(f'WORKFLOW_ID = {repr(safe_wf_id)}')
#     lines.append(f'TASKS_DEF   = json.loads({repr(tasks_json)})')
#     lines.append(f'BACKEND_URL = "http://backend:8000"')
#     lines.append("")
#     lines.append('default_args = {"owner": "etlflow", "retries": 0}')
#     lines.append("")

#     # Helper functions
#     lines.append('''
# def get_schema(pg, table_name):
#     if "." not in table_name:
#         table_name = f"staging.{table_name}"
#     schema_name, tbl = table_name.split(".", 1)
#     rows = pg.get_records(f"""
#         SELECT column_name, data_type
#         FROM information_schema.columns
#         WHERE table_schema = '{schema_name}' AND table_name = '{tbl}'
#         AND column_name NOT IN ('_id','_date_partition','_processed_at','loaded_at','date_partition')
#         ORDER BY ordinal_position
#     """)
#     schema = {}
#     for col, dtype in rows:
#         if   "int"       in dtype: schema[col] = "BIGINT"
#         elif "numeric"   in dtype or "float" in dtype: schema[col] = "NUMERIC"
#         elif "timestamp" in dtype: schema[col] = "TIMESTAMP"
#         elif "date"      in dtype: schema[col] = "DATE"
#         elif "bool"      in dtype: schema[col] = "BOOLEAN"
#         else:                      schema[col] = "TEXT"
#     return schema

# def q(cols):
#     return ", ".join(f\'"{c}"\' for c in cols)

# def build_dynamic_spark_config(estimated_mb, cluster):
#     """
#     Hitung config berdasarkan:
#     - estimated_mb : ukuran data actual (dari sampling)
#     - cluster      : resource real dari Spark master API
    
#     Prinsip:
#     - Jangan pakai lebih dari 75% total cluster memory (sisakan untuk overhead)
#     - Jangan pakai lebih dari 80% total cores
#     - Kalau data kecil, pakai minimal (hemat resource untuk job lain)
#     - Kalau data besar, scale up proporsional
#     """
#     safe_mem_mb  = cluster["total_mem_mb"]  * 0.75
#     safe_cores   = max(1, int(cluster["total_cores"] * 0.80))
#     workers      = max(1, cluster["worker_count"])

#     # --- Tentukan "tier" berdasarkan ukuran data ---
#     if estimated_mb < 50:
#         # Tiny: 1 executor, memory minimal
#         # Tidak perlu Spark overhead → sarankan PostgreSQL
#         return {
#             "use_spark":        False,  # ← key flag
#             "reason":           f"Data only {estimated_mb:.1f}MB, PostgreSQL faster",
#             "executor_memory":  "512m",
#             "executor_cores":   1,
#             "num_executors":    1,
#             "dynamic":          False,
#             "partitions":       1,
#         }

#     elif estimated_mb < 500:
#         # Small: 1-2 executors, memory rendah
#         mem_per_exec = min(1024, int(safe_mem_mb / workers))
#         mem_per_exec = max(512, mem_per_exec)
#         return {
#             "use_spark":        True,
#             "executor_memory":  f"{mem_per_exec}m",
#             "executor_cores":   min(2, safe_cores),
#             "num_executors":    min(2, workers),
#             "dynamic":          False,
#             "partitions":       2,
#         }

#     elif estimated_mb < 5000:
#         # Medium: scale proporsional
#         needed_executors = max(2, min(workers, int(estimated_mb / 500)))
#         mem_per_exec     = min(4096, int(safe_mem_mb / needed_executors))
#         mem_per_exec     = max(1024, mem_per_exec)
#         cores_per_exec   = max(1, safe_cores // needed_executors)
#         partitions       = max(4, needed_executors * cores_per_exec * 2)

#         return {
#             "use_spark":        True,
#             "executor_memory":  f"{mem_per_exec}m",
#             "executor_cores":   cores_per_exec,
#             "num_executors":    needed_executors,
#             "dynamic":          True,
#             "partitions":       partitions,
#         }

#     else:
#         # Large: maksimalkan semua resource
#         mem_per_exec = min(8192, int(safe_mem_mb / workers))
#         mem_per_exec = max(2048, mem_per_exec)
#         partitions   = max(8, safe_cores * 3)  # 3x cores = good parallelism

#         return {
#             "use_spark":        True,
#             "executor_memory":  f"{mem_per_exec}m",
#             "executor_cores":   safe_cores // max(1, workers),
#             "num_executors":    workers,
#             "dynamic":          True,
#             "partitions":       partitions,
#             "extra_configs": {
#                 # Untuk dataset > 5GB, aktifkan optimasi tambahan
#                 "spark.sql.shuffle.partitions":          str(partitions),
#                 "spark.sql.adaptive.skewJoin.enabled":   "true",
#                 "spark.memory.fraction":                 "0.8",
#                 "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
#             }
#         }

# def run_task(task_def, **context):
#     import subprocess
#     pg = PostgresHook(postgres_conn_id="postgres_default")
#     conf = context.get("dag_run").conf or {}
#     run_ids = conf.get("run_ids", [])
#     task_id = task_def.get("task_id", "task_1")
#     output_name = task_def.get("output_name", "output")
#     transforms  = task_def.get("transforms", [])

#     safe_output = output_name.lower().replace(" ", "_")
#     import re as _re
#     safe_output = _re.sub(r\'[^a-z0-9_]\', \'_\', safe_output)
#     if safe_output and safe_output[0].isdigit():
#         safe_output = \'t_\' + safe_output
#     safe_output = safe_output or \'output\'

#     # Detect input size
#     tbl = INPUT_TABLE if "." in INPUT_TABLE else f"staging.{INPUT_TABLE}"
#     sch, tname = tbl.split(".", 1)
#     exists = pg.get_first(f"""
#         SELECT EXISTS (SELECT FROM information_schema.tables
#         WHERE table_schema = \'{sch}\' AND table_name = \'{tname}\')
#     """)[0]
#     if not exists:
#         raise ValueError(f"Table {tbl} not found")

#     row_count = pg.get_first(f"SELECT COUNT(*) FROM {tbl}")[0]
#     schema    = get_schema(pg, tbl)
#     col_count = len(schema)
#     spark_cfg = detect_spark_config(row_count, col_count)

#     print(f"[Spark] Task: {task_id} | Rows: {row_count} | Cols: {col_count}")
#     print(f"[Spark] Config: {spark_cfg}")

#     estimated_mb = estimate_real_size_mb(pg, tbl, row_count)
#     cluster      = get_available_spark_resources()
#     spark_cfg    = build_dynamic_spark_config(estimated_mb, cluster)

#     print(f"[Resource] Estimated size: {estimated_mb:.1f}MB")
#     print(f"[Resource] Cluster: {cluster}")
#     print(f"[Resource] Config selected: {spark_cfg}")

#     if not spark_cfg.get("use_spark", True):
#         print(f"[Route] → PostgreSQL (reason: {spark_cfg.get('reason')})")
#         run_with_postgres(pg, tbl, safe_output, transforms, task_id, row_count)
#     else:
#         # Cek apakah PySpark benar-benar tersedia
#         spark_available = False
#         try:
#             import importlib.util
#             spark_available = importlib.util.find_spec("pyspark") is not None
#         except:
#             pass

#         if spark_available:
#             print(f"[Route] → Spark ({estimated_mb:.1f}MB, {cluster['total_cores']} cores available)")
#             run_with_spark(pg, tbl, safe_output, transforms, row_count, spark_cfg, task_id)
#         else:
#             print("[Route] → PostgreSQL fallback (PySpark not installed)")
#             run_with_postgres(pg, tbl, safe_output, transforms, task_id, row_count)

#     # Try PySpark if available, else fallback to PostgreSQL transforms
#     spark_available = False
#     try:
#         import importlib.util
#         spark_available = importlib.util.find_spec("pyspark") is not None
#     except:
#         pass

#     if spark_available:
#         run_with_spark(pg, tbl, safe_output, transforms, row_count, spark_cfg, task_id)
#     else:
#         run_with_postgres(pg, tbl, safe_output, transforms, task_id, row_count)

#     # Update backend run status
#     out = f"warehouse.{safe_output}"
#     count = pg.get_first(f"SELECT COUNT(*) FROM {out}")[0]

#     for run_id in run_ids:
#         try:
#             requests.patch(f"{BACKEND_URL}/api/pipelines/runs/{run_id}",
#                 json={"status": "success", "row_count": count}, timeout=5)
#         except Exception as e:
#             print(f"[Task] Backend update failed: {e}")

#     print(f"[Done] Task {task_id} → {out} ({count} rows)")

# def estimate_real_size_mb(pg, table, row_count):
#     """Sample 1000 rows, ukur actual size, ekstrapolasi ke full dataset."""
#     sample = min(1000, row_count)
#     rows = pg.get_records(f"SELECT * FROM {table} LIMIT {sample}")
    
#     import sys
#     sample_bytes = sum(sys.getsizeof(str(r)) for r in rows)
#     avg_row_bytes = sample_bytes / max(sample, 1)
    
#     estimated_mb = (avg_row_bytes * row_count) / (1024 * 1024)
#     return estimated_mb

# def get_available_spark_resources():
#     """
#     Query Spark master API untuk tahu resource yang benar-benar tersedia.
#     Jangan hardcode asumsi cluster.
#     """
#     import requests
#     try:
#         r = requests.get("http://spark:8080/json/", timeout=3)
#         data = r.json()
        
#         alive_workers = [w for w in data.get("workers", []) if w["state"] == "ALIVE"]
#         total_cores   = sum(w["cores"] for w in alive_workers)
#         # Spark API return memory dalam MB
#         total_mem_mb  = sum(w["memory"] for w in alive_workers)
        
#         return {
#             "total_cores":  total_cores,
#             "total_mem_mb": total_mem_mb,
#             "worker_count": len(alive_workers),
#             "available":    total_cores > 0,
#         }
#     except Exception as e:
#         print(f"[Spark] Cannot reach master API: {e}")
#         # Fallback ke minimum safe config
#         return {
#             "total_cores":  2,
#             "total_mem_mb": 2048,
#             "worker_count": 1,
#             "available":    False,  # flag: pakai PostgreSQL fallback saja
#         }


# def run_with_spark(pg, input_table, output_name, transforms, row_count, spark_cfg, task_id):
#     from pyspark.sql import SparkSession
#     from pyspark.sql import functions as F
#     from pyspark.sql.types import StringType, LongType, DoubleType, BooleanType, DateType, TimestampType

#     # Build SparkSession with right-sized resources
#     builder = SparkSession.builder \
#         .appName(f"ETLFlow_{DAG_ID}_{task_id}") \
#         .config("spark.master", "spark://spark:7077") \
#         .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar") \
#         .config("spark.executor.memory",  spark_cfg["executor_memory"]) \
#         .config("spark.executor.cores",   str(spark_cfg["executor_cores"])) \
#         .config("spark.sql.adaptive.enabled", "true") \
#         .config("spark.sql.adaptive.coalescePartitions.enabled", "true")

#     if spark_cfg.get("dynamic"):
#         builder = builder \
#             .config("spark.dynamicAllocation.enabled",    "true") \
#             .config("spark.dynamicAllocation.minExecutors", "1") \
#             .config("spark.dynamicAllocation.maxExecutors", str(spark_cfg["num_executors"]))
#     else:
#         # Static allocation: lebih predictable untuk small jobs
#         builder = builder \
#             .config("spark.dynamicAllocation.enabled", "false") \
#             .config("spark.executor.instances", str(spark_cfg["num_executors"]))

#     for k, v in spark_cfg.get("extra_configs", {}).items():
#         builder = builder.config(k, v)

#     spark = builder.getOrCreate()

#     # Read from PostgreSQL
#     jdbc_url = "jdbc:postgresql://postgres:5432/airflow"
#     jdbc_props = {"user": "airflow", "password": "airflow", "driver": "org.postgresql.Driver"}

#     # Determine optimal partitions
#     num_partitions = spark_cfg.get("partitions", 4)

#     jdbc_url   = "jdbc:postgresql://postgres:5432/airflow"
#     jdbc_props = {"user": "airflow", "password": "airflow", "driver": "org.postgresql.Driver"}

#     df = spark.read.jdbc(
#         url=jdbc_url,
#         table=f"(SELECT * FROM {input_table}) AS t",
#         numPartitions=num_partitions,
#         properties=jdbc_props
#     )

#     df = apply_spark_transforms(spark, df, transforms)

#     # Repartition strategy berdasarkan config
#     if num_partitions > 4:
#         df = df.repartition(num_partitions)
#     elif num_partitions > 1:
#         df = df.coalesce(num_partitions)

#     if len(transforms) > 3:
#         df.cache()

#     df.write.jdbc(
#         url=jdbc_url,
#         table=f"warehouse.{output_name}",
#         mode="overwrite",
#         properties=jdbc_props
#     )

#     if row_count > 100_000:
#         parquet_path = f"/data_csv/parquet/{output_name}.parquet"
#         os.makedirs("/data_csv/parquet", exist_ok=True)
#         df.write.mode("overwrite").parquet(parquet_path)

#     spark.stop()


# def apply_spark_transforms(spark, df, transforms):
#     from pyspark.sql import functions as F

#     for tx in transforms:
#         ntype  = tx.get("type", "")
#         config = tx.get("config") or {}

#         try:
#             if ntype == "filter_rows":
#                 formula = config.get("formula", "1=1")
#                 df = df.filter(formula)

#             elif ntype == "select_col":
#                 cols = config.get("columns", [])
#                 valid = [c for c in cols if c in df.columns]
#                 if valid:
#                     df = df.select(valid)

#             elif ntype == "drop_col":
#                 drop = config.get("columns", [])
#                 keep = [c for c in df.columns if c not in drop]
#                 df = df.select(keep)

#             elif ntype == "rename_col":
#                 renames = config.get("renames", {})
#                 for old, new in renames.items():
#                     if old in df.columns:
#                         df = df.withColumnRenamed(old, new)

#             elif ntype == "add_const":
#                 name  = config.get("name", "new_col")
#                 val   = config.get("value", "NULL")
#                 df = df.withColumn(name, F.lit(val))

#             elif ntype == "fill_null":
#                 fill_cols = config.get("columns", [])
#                 fill_val  = config.get("fillValue", "")
#                 fill_type = config.get("fillType", "value")
#                 for c in fill_cols:
#                     if c not in df.columns:
#                         continue
#                     if fill_type == "value":
#                         df = df.fillna({c: fill_val})
#                     elif fill_type == "mean":
#                         mean_val = df.agg(F.mean(c)).collect()[0][0]
#                         df = df.fillna({c: mean_val})

#             elif ntype == "order_table":
#                 orders = config.get("orders", [])
#                 sort_cols = []
#                 for o in orders:
#                     col = o.get("col")
#                     if col and col in df.columns:
#                         sort_cols.append(F.col(col).asc() if o.get("dir","ASC") == "ASC" else F.col(col).desc())
#                 if sort_cols:
#                     df = df.orderBy(sort_cols)

#             elif ntype == "change_type":
#                 types = config.get("types", {})
#                 type_map = {"TEXT":"string","INTEGER":"integer","BIGINT":"long",
#                             "NUMERIC":"double","BOOLEAN":"boolean","DATE":"date","TIMESTAMP":"timestamp"}
#                 for c, t in types.items():
#                     if c in df.columns:
#                         spark_type = type_map.get(t, "string")
#                         df = df.withColumn(c, F.col(c).cast(spark_type))

#             elif ntype == "group_agg":
#                 gcols = [c for c in config.get("groupCols", []) if c in df.columns]
#                 acols = config.get("aggCols", [])
#                 if gcols and acols:
#                     agg_exprs = []
#                     func_map = {"COUNT": F.count, "SUM": F.sum, "AVG": F.avg,
#                                 "MIN": F.min, "MAX": F.max, "COUNT DISTINCT": F.countDistinct}
#                     for a in acols:
#                         fn = func_map.get(a["func"], F.count)
#                         agg_exprs.append(fn(a["col"]).alias(a["alias"]))
#                     df = df.groupBy(gcols).agg(*agg_exprs)

#         except Exception as e:
#             print(f"[Spark] Transform {ntype} failed: {e}, skipping")

#     return df


# def run_with_postgres(pg, input_table, output_name, transforms, task_id, row_count):
#     """Fallback: run transforms using PostgreSQL SQL."""
#     print(f"[PG] Running transforms for {task_id} via PostgreSQL")
#     schema = get_schema(pg, input_table)
#     cur_cols = list(schema.keys())
#     current = input_table
#     step = 0

#     pg.run("CREATE SCHEMA IF NOT EXISTS warehouse")
#     pg.run("CREATE SCHEMA IF NOT EXISTS staging")

#     # Clean up old temp tables
#     temps = pg.get_records(f"""
#         SELECT table_name FROM information_schema.tables
#         WHERE table_schema = \'staging\' AND table_name LIKE \'_{DAG_ID}_{task_id}_step_%\'
#     """)
#     for (t,) in temps:
#         pg.run(f\'DROP TABLE IF EXISTS staging."{t}"\')

#     for tx in transforms:
#         ntype  = tx.get("type", "")
#         config = tx.get("config") or {}
#         step  += 1
#         tmp    = f"staging._{DAG_ID}_{task_id}_step_{step}"

#         cur_schema = get_schema(pg, current)
#         cur_cols   = list(cur_schema.keys())
#         all_q      = q(cur_cols)

#         try:
#             if ntype == "filter_rows":
#                 formula = config.get("formula", "1=1")
#                 pg.run(f"CREATE TABLE {tmp} AS SELECT * FROM {current} WHERE {formula}")
#             elif ntype == "select_col":
#                 cols = [c for c in config.get("columns", cur_cols) if c in cur_cols]
#                 if cols:
#                     pg.run(f"CREATE TABLE {tmp} AS SELECT {q(cols)} FROM {current}")
#                     cur_cols = cols
#                 else:
#                     tmp = current
#             elif ntype == "drop_col":
#                 keep = [c for c in cur_cols if c not in set(config.get("columns", []))]
#                 pg.run(f"CREATE TABLE {tmp} AS SELECT {q(keep)} FROM {current}")
#             elif ntype == "rename_col":
#                 renames = config.get("renames", {})
#                 exprs = ", ".join(f\'"{c}" AS "{renames.get(c, c)}"\' for c in cur_cols)
#                 pg.run(f"CREATE TABLE {tmp} AS SELECT {exprs} FROM {current}")
#             elif ntype == "add_const":
#                 name  = config.get("name", "new_col")
#                 val   = config.get("value", "NULL")
#                 dtype = config.get("dtype", "TEXT")
#                 pg.run(f\'CREATE TABLE {tmp} AS SELECT {all_q}, CAST({repr(val)} AS {dtype}) AS "{name}" FROM {current}\')
#             elif ntype == "fill_null":
#                 fill_cols = config.get("columns", [])
#                 fill_val  = config.get("fillValue", "")
#                 exprs_list = []
#                 for c in cur_cols:
#                     if c in fill_cols:
#                         exprs_list.append(f\'COALESCE("{c}"::TEXT, {repr(str(fill_val))})::TEXT AS "{c}"\')
#                     else:
#                         exprs_list.append(f\'"{c}"\')
#                 pg.run(f"CREATE TABLE {tmp} AS SELECT {', '.join(exprs_list)} FROM {current}")
#             elif ntype == "order_table":
#                 orders = config.get("orders", [])
#                 oc = ", ".join(f\'"{o["col"]}" {o.get("dir","ASC")}\' for o in orders if o.get("col") in cur_cols) or "1"
#                 pg.run(f"CREATE TABLE {tmp} AS SELECT {all_q} FROM {current} ORDER BY {oc}")
#             elif ntype == "change_type":
#                 types = config.get("types", {})
#                 exprs = ", ".join(
#                     f\'"{c}"::TEXT::{types[c]} AS "{c}"\' if c in types else f\'"{c}"\'
#                     for c in cur_cols
#                 )
#                 pg.run(f"CREATE TABLE {tmp} AS SELECT {exprs} FROM {current}")
#             elif ntype == "group_agg":
#                 gcols = [c for c in config.get("groupCols", []) if c in cur_cols]
#                 acols = config.get("aggCols", [])
#                 if gcols and acols:
#                     g = q(gcols)
#                     a = ", ".join(f\'{x["func"]}("{x["col"]}") AS "{x["alias"]}"\' for x in acols)
#                     pg.run(f"CREATE TABLE {tmp} AS SELECT {g}, {a} FROM {current} GROUP BY {g}")
#                 else:
#                     tmp = current
#             else:
#                 tmp = current
#         except Exception as e:
#             print(f"[PG] Step {step} ({ntype}) error: {e}")
#             tmp = current

#         if tmp != current:
#             current = tmp

#     # Load to warehouse
#     final_schema = get_schema(pg, current)
#     out = f"warehouse.{output_name}"
#     pg.run(f"DROP TABLE IF EXISTS {out}")
#     col_defs = ", ".join(f\'"{c}" {dt}\' for c, dt in final_schema.items())
#     pg.run(f"""CREATE TABLE {out} ({col_defs}, date_partition DATE DEFAULT CURRENT_DATE, loaded_at TIMESTAMP DEFAULT NOW())""")
#     col_names = q(final_schema.keys())
#     pg.run(f"""INSERT INTO {out} ({col_names}, date_partition, loaded_at) SELECT {col_names}, CURRENT_DATE, NOW() FROM {current}""")

#     # Cleanup temp tables
#     temps2 = pg.get_records(f"""
#         SELECT table_name FROM information_schema.tables
#         WHERE table_schema = \'staging\' AND table_name LIKE \'_{DAG_ID}_{task_id}_step_%\'
#     """)
#     for (t,) in temps2:
#         pg.run(f\'DROP TABLE IF EXISTS staging."{t}"\')

# ''')

#     # Generate task functions and DAG definition
#     lines.append(f"""
# with DAG(
#     dag_id={repr(dag_id)},
#     default_args=default_args,
#     schedule_interval=None,
#     start_date=datetime(2024, 1, 1),
#     catchup=False,
#     tags=["etl", "spark", "generated", {repr(safe_wf_id)}],
#     description={repr(description)},
# ) as dag:
#     airflow_tasks = {{}}
#     for task_def in TASKS_DEF:
#         tid = task_def["task_id"]
#         t = PythonOperator(
#             task_id=tid,
#             python_callable=run_task,
#             op_kwargs={{"task_def": task_def}},
#         )
#         airflow_tasks[tid] = t

#     # Set up task dependencies (multi-branch)
#     for task_def in TASKS_DEF:
#         tid = task_def["task_id"]
#         for dep_tid in task_def.get("depends_on", []):
#             if dep_tid in airflow_tasks:
#                 airflow_tasks[dep_tid] >> airflow_tasks[tid]
# """)

#     return "\n".join(lines)