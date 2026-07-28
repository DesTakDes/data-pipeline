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
import spark_config

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

def _detect_resources_and_tune(row_count):
    import os
    try:
        cpu_count = len(os.sched_getaffinity(0))
    except AttributeError:
        cpu_count = os.cpu_count() or 2

    mem_limit_bytes = None
    for path in ("/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"):
        try:
            with open(path) as f:
                val = f.read().strip()
                if val.isdigit() and int(val) < (1 << 62):
                    mem_limit_bytes = int(val)
                    break
        except (FileNotFoundError, PermissionError):
            continue
    if mem_limit_bytes is None:
        import psutil
        mem_limit_bytes = psutil.virtual_memory().total

    usable_mb = (mem_limit_bytes / 1024 / 1024) * 0.6
    usable_cores = max(1, cpu_count - 1)

    if row_count < 500_000:
        want_gb, want_cores, want_execs = 1, 1, 1
    elif row_count < 3_000_000:
        want_gb, want_cores, want_execs = 2, 2, 2
    else:
        want_gb, want_cores, want_execs = 4, 2, 3

    exec_mem_gb   = max(1, min(want_gb, int(usable_mb / 1024 / max(1, want_execs))))
    exec_cores    = min(want_cores, usable_cores)
    num_executors = min(want_execs, max(1, usable_cores // exec_cores))

    print(f"[Spark] Auto-tuned: {exec_mem_gb}g mem, {exec_cores} cores, {num_executors} executors "
          f"(host: {usable_cores+1} cores, {mem_limit_bytes/1024/1024:.0f}MB mem limit)")

    return {
        "executor_memory": f"{exec_mem_gb}g",
        "executor_memoryOverhead": f"{max(256, int(exec_mem_gb*1024*0.25))}m",
        "executor_cores": str(exec_cores),
        "max_executors": str(num_executors),
        "shuffle_partitions": max(8, num_executors * exec_cores * 2),
    }

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
    if safe_out and safe_out[0].isdigit():
        safe_out = 't_' + safe_out
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
        return f"WHEN TRY_CAST({col_ref} AS DOUBLE PRECISION) {condition} {num} THEN {result_lit}"
    op = condition if condition in (\'=\', \'!=\') else \'=\'
    return f"WHEN {col_ref} {op} {repr(value)} THEN {result_lit}"

# ── Estimate ─────────────────────────────────────────────────────────────
def _estimate_mb(pg_conn, table):
    try:
        cur = pg_conn.cursor()
        cur.execute("SELECT pg_total_relation_size(%s) / 1024.0 / 1024.0", (table,))
        val = float(cur.fetchone()[0] or 0)
        cur.close()
        return val
    except Exception:
        return 0.0


def _estimate_avg_row_bytes(pg_conn, table_name, schema="staging"):
    TYPE_BYTE_ESTIMATES = {
        "smallint": 16, "integer": 16, "bigint": 24,
        "real": 16, "double precision": 24, "numeric": 32,
        "boolean": 16,
        "date": 24, "timestamp without time zone": 32, "timestamp with time zone": 32,
        "character varying": 60,
        "text": 100,
        "character": 40,
        "json": 200, "jsonb": 200,
        "uuid": 40,
    }
    DEFAULT_BYTES = 40
    try:
        cur = pg_conn.cursor()
        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
        """, (schema, table_name))
        cols = cur.fetchall()
        cur.close()
        if not cols:
            print(f"[Spark] Could not find columns for {schema}.{table_name}, using default row size")
            return 200
        total_bytes = sum(TYPE_BYTE_ESTIMATES.get(dt, DEFAULT_BYTES) for _, dt in cols)
        total_bytes += 50
        print(f"[Spark] Estimated avg row size: {total_bytes} bytes across {len(cols)} columns for {schema}.{table_name}")
        return total_bytes
    except Exception as e:
        print(f"[Spark] Could not estimate row size ({e}), using default 200 bytes")
        return 200


def _compute_read_partitions(row_count, exec_mem_gb, exec_cores, avg_row_bytes=200):
    safe_partition_bytes = (exec_mem_gb * 1024 * 1024 * 1024) * 0.15
    rows_per_partition_by_memory = max(1, int(safe_partition_bytes / avg_row_bytes))
    partitions_by_memory = max(1, row_count // rows_per_partition_by_memory + 1)
    partitions_by_cores = exec_cores * 6
    read_partitions = max(partitions_by_memory, partitions_by_cores)
    read_partitions = max(8, min(300, read_partitions))
    return read_partitions

# ── Spark Cluster ──────────────────────────────────────────────────────────────
def _query_spark_cluster_capacity():
    """
    Ask the Spark Master directly how much memory/cores the cluster
    actually has right now (total and currently free across all workers).
    This reflects real capacity — set via SPARK_WORKER_MEMORY/CORES in .env —
    rather than guessing from container cgroups that aren't actually capped.
    """
    import requests
    try:
        r = requests.get("http://spark:8080/json/", timeout=5)
        data = r.json()
        total_mem_mb  = data.get("memory", 2048)
        used_mem_mb   = data.get("memoryused", 0)
        total_cores   = data.get("cores", 2)
        used_cores    = data.get("coresused", 0)
        return {
            "free_memory_mb": max(512, total_mem_mb - used_mem_mb),
            "total_memory_mb": total_mem_mb,
            "free_cores": max(1, total_cores - used_cores),
            "total_cores": total_cores,
        }
    except Exception as e:
        print(f"[Spark] Could not query cluster capacity ({e}), using conservative defaults")
        return {"free_memory_mb": 1024, "total_memory_mb": 2048, "free_cores": 1, "total_cores": 2}


# ── Spark Runner ──────────────────────────────────────────────────────────────

def _run_spark(input_table, output_name, transforms, row_count):
    from pyspark.sql import SparkSession, functions as F

    import requests
    try:
        r = requests.get("http://spark:8080/json/", timeout=5)
        cluster = r.json()
        total_mem_mb = cluster.get("memory", 2048)
    except Exception:
        total_mem_mb = 2048

    exec_mem_gb = max(1, int((total_mem_mb * 0.7) / 1024))
    exec_mem_overhead_mb = max(256, int(exec_mem_gb * 1024 * 0.25))

    safe_out = re.sub(r'[^a-z0-9_]','_',output_name.lower()).strip('_') or "output"
    if safe_out and safe_out[0].isdigit():
        safe_out = 't_' + safe_out

    # ── Estimate row size + compute partitions BEFORE creating the session ──
    try:
        schema_name, table_name_only = (input_table.split(".", 1) + [""])[:2] if "." in input_table else ("staging", input_table)
        pg_conn_for_estimate = psycopg2.connect(
            host="postgres", port=5432, database="airflow",
            user="airflow", password="airflow",
        )
        avg_row_bytes = _estimate_avg_row_bytes(pg_conn_for_estimate, table_name_only.strip('"'), schema_name)
        pg_conn_for_estimate.close()
    except Exception as e:
        print(f"[Spark] Row size estimation failed ({e}), using default 200 bytes")
        avg_row_bytes = 200

    read_partitions = _compute_read_partitions(
        row_count=row_count,
        exec_mem_gb=exec_mem_gb,
        exec_cores=2,   # match spark.executor.cores below
        avg_row_bytes=avg_row_bytes,
    )
    print(f"[Spark] Cluster: {total_mem_mb}MB. Using {exec_mem_gb}g. "
          f"Reading {row_count:,} rows using {read_partitions} partitions "
          f"(~{row_count // read_partitions:,} rows/partition, ~{avg_row_bytes} bytes/row)")

    # ── NOW create the SparkSession, using the values computed above ────────
    spark = (SparkSession.builder
        .appName(f"ETLFlow_{DAG_ID}_{TASK_ID}_{safe_out}")
        .config("spark.master","spark://spark:7077")
        .config("spark.jars","/opt/spark/jars/postgresql-42.6.0.jar")
        .config("spark.executor.memory", f"{exec_mem_gb}g")
        .config("spark.executor.memoryOverhead", f"{exec_mem_overhead_mb}m")
        .config("spark.executor.cores", "2")
        .config("spark.executor.instances", "1")
        .config("spark.dynamicAllocation.enabled","false")
        .config("spark.sql.adaptive.enabled","true")
        .config("spark.sql.adaptive.coalescePartitions.enabled","false")
        .config("spark.sql.shuffle.partitions", str(read_partitions))
        .getOrCreate())

    TYPE_MAP = _resolve_types_spark_map()

    # ── Read ONCE, using the partition count already computed above ────────
    df = _read_jdbc_table(spark, input_table, num_partitions=read_partitions)
    
    for tx in transforms:
        ntype = tx.get("type", "")
        cfg = tx.get("config") or {}
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
                name = cfg.get("name", "new_col")
                val = cfg.get("value", "")
                dtype = TYPE_MAP.get(cfg.get("dtype", "TEXT"), "string")
                df = df.withColumn(name, F.lit(val).cast(dtype))

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
                src = cfg.get("sourceCol")
                new_col = cfg.get("newColName", "mapped")
                whens = cfg.get("whens", [])
                else_v = cfg.get("elseValue", "")
                if src and src in df.columns:
                    expr = None
                    for w in whens:
                        condition = w.get("condition", "=")
                        value = w.get("value", "")
                        result = w.get("result", "")
                        if condition not in ("IS NULL", "IS NOT NULL") and not value:
                            continue
                        try:
                            cond_expr = _spark_when_condition(F, src, condition, value)
                        except Exception:
                            continue
                        if expr is None:
                            expr = F.when(cond_expr, F.lit(result))
                        else:
                            expr = expr.when(cond_expr, F.lit(result))
                    if expr is not None:
                        df = df.withColumn(new_col, expr.otherwise(F.lit(else_v)))
                    else:
                        df = df.withColumn(new_col, F.lit(else_v))

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
                        if stats:
                            df = df.fillna(stats)
                    elif ft == "median":
                        meds = {}
                        for c in fc:
                            q = df.approxQuantile(c, [0.5], 0.001)
                            if q:
                                meds[c] = q[0]
                        if meds:
                            df = df.fillna(meds)
                    elif ft == "mode":
                        modes = {}
                        for c in fc:
                            row = (df.filter(F.col(c).isNotNull()).groupBy(c).count()
                                     .orderBy(F.desc("count")).limit(1).collect())
                            if row:
                                modes[c] = row[0][c]
                        if modes:
                            df = df.fillna(modes)
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
                cols = [
                    F.col(o["col"]).asc() if o.get("dir", "ASC") == "ASC" else F.col(o["col"]).desc()
                    for o in orders if o.get("col") in df.columns
                ]
                if cols:
                    df = df.orderBy(*cols)

            elif ntype == "join_data":
                right_table = cfg.get("rightTable")
                left_col = cfg.get("leftCol")
                right_col = cfg.get("rightCol")
                if right_table and left_col:
                    right_df = _read_jdbc_table(spark, right_table, num_partitions=4)

                    raw_type = cfg.get("joinType", "INNER JOIN").upper()
                    is_cross = "CROSS" in raw_type
                    join_type = raw_type.replace(" JOIN", "").lower().replace("full outer", "outer")

                    # Rename any overlapping column names (including the right
                    # join key itself) so the result never has duplicate names.
                    right_join_col = right_col
                    dup_cols = [c for c in right_df.columns if c in df.columns]
                    for c in dup_cols:
                        new_name = f"{c}_right"
                        right_df = right_df.withColumnRenamed(c, new_name)
                        if c == right_col:
                            right_join_col = new_name

                    try:
                        right_size_mb = _estimate_mb(_get_conn(), right_table)
                    except Exception:
                        right_size_mb = 9999
                    if right_size_mb <= BROADCAST_MAX_MB:
                        right_df = F.broadcast(right_df)
                        print(f"[Spark] join_data: broadcast tabel kanan (~{right_size_mb:.1f} MB)")
                    else:
                        # Not broadcasting -> repartition both sides on the
                        # join key to avoid skew across the shuffle.
                        df = df.repartition(exec_cores * 2, F.col(left_col))
                        right_df = right_df.repartition(exec_cores * 2, F.col(right_join_col))

                    if is_cross:
                        df = df.crossJoin(right_df)
                    elif right_join_col:
                        df = df.join(right_df, df[left_col] == right_df[right_join_col], join_type)
                        if right_join_col != left_col:
                            df = df.drop(right_join_col)

            elif ntype == "calc":
                new_col = (cfg.get("newColName") or "result").strip()
                col_a = cfg.get("colA")
                col_b = cfg.get("colB")
                op = cfg.get("operation", "+")
                if new_col and col_a in df.columns and col_b in df.columns:
                    a = F.col(col_a).cast("double")
                    b = F.col(col_b).cast("double")
                    expr = {"+": a + b, "-": a - b, "*": a * b, "/": F.when(b != 0, a / b)}.get(op, a + b)
                    df = df.withColumn(new_col, expr)

            elif ntype == "adv_calculator":
                SCI = {
                    "sin": F.sin, "cos": F.cos, "sqrt": F.sqrt,
                    "radians": F.radians, "atan2": F.atan2, "power": F.pow,
                }
                for calc in cfg.get("calculations", []):
                    fn = SCI.get(calc.get("operation", "sin"), F.sin)
                    new_c = (calc.get("newColName") or "").strip()
                    col_a = calc.get("colA")
                    col_b = calc.get("colB")
                    if not new_c or col_a not in df.columns:
                        continue
                    if calc.get("operation") in ("atan2", "power") and col_b in df.columns:
                        df = df.withColumn(new_c, fn(F.col(col_a).cast("double"), F.col(col_b).cast("double")))
                    else:
                        df = df.withColumn(new_c, fn(F.col(col_a).cast("double")))

            elif ntype == "combine_cols":
                new_col = (cfg.get("newColName") or "combined").strip()
                sep = cfg.get("separator", " ")
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
                    fn_map = {
                        "COUNT": F.count, "SUM": F.sum, "AVG": F.avg,
                        "MIN": F.min, "MAX": F.max, "COUNT DISTINCT": F.countDistinct,
                    }
                    aggs = [
                        fn_map.get(a["func"], F.count)(a["col"]).alias(a["alias"])
                        for a in ac if a.get("col") in df.columns
                    ]
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

    # ── Repartition before writing so the JDBC writer sends manageable,
    # evenly-sized batches instead of one executor choking on one huge
    # partition. ────────────────────────────────────────────────────────
    JOIN_FANOUT_SAFETY = 3  # assume join could multiply rows up to 3x
    estimated_rows = row_count * JOIN_FANOUT_SAFETY


    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")

    df.write.jdbc(
        url="jdbc:postgresql://postgres:5432/airflow",
        table=f'warehouse."{safe_out}"',
        mode="overwrite",
        properties={
            "user": "airflow",
            "password": "airflow",
            "driver": "org.postgresql.Driver",
            "batchsize": "50000",
        },
    )

    if row_count > 10_000:
        os.makedirs(PARQUET_DIR, exist_ok=True)
        parquet_path = f"{PARQUET_DIR}/{safe_out}.parquet"
        (df.write.mode("overwrite").option("compression", "snappy").parquet(parquet_path))
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
    if safe_out and safe_out[0].isdigit():
        safe_out = \'t_\' + safe_out

    print(f"[Task:{TASK_ID}] engine=spark (single-engine architecture)")

    def _report(status, pct, msg, row_count=None):
        for run_id in run_ids:
            try:
                payload = {"status": status, "progress_pct": pct, "message": msg}
                if row_count is not None:
                    payload["row_count"] = row_count
                requests.patch(
                    f"{backend_url}/api/pipelines/runs/{run_id}",
                    json=payload,
                    timeout=5,
                )
            except Exception: pass

    from airflow.providers.postgres.hooks.postgres import PostgresHook
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")

    try:
        row_count = pg_hook.get_first(f"SELECT COUNT(*) FROM {tbl}")[0]
    except Exception as e:
        err = f"Failed to read input table {tbl}: {e}"
        print(f"[Task:{TASK_ID}] {err}")
        _report("failed", 0, err)
        raise

    try:
        _run_spark(tbl, safe_out, TRANSFORMS, row_count)
    except Exception as e:
        err = f"Spark execution failed: {e}"
        print(f"[Task:{TASK_ID}] {err}\\n{traceback.format_exc()}")
        _report("failed", 0, err)
        raise

    try:
        rows = pg_hook.get_first(f\'SELECT COUNT(*) FROM warehouse."{safe_out}"\')[0]
    except Exception:
        rows = 0

    _report("success", 100, f"Done: {rows:,} rows via spark", row_count=rows)
    print(f"[Task:{TASK_ID}] Done → warehouse.{safe_out} ({rows:,} rows via spark)")
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
sys.path.insert(0, os.path.dirname(__file__))


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
# HOST
# ════════════════════════════════════════════════════════════════════════════

@app.get("/api/spark/host-resources")
def spark_host_resources():
    return spark_config.detect_host_resources()

@app.get("/api/spark/resource-recommendation")
def resource_recommendation(file_size_bytes: int = 0, row_count: int = 0, col_count: int = 0):
    profile = spark_config.estimate_dataset_profile(file_size_bytes, row_count, col_count)
    host    = spark_config.detect_host_resources()
    rec     = spark_config.compute_auto_tuned_config(profile, host)
    return {"profile": profile, "host": host, "recommendation": rec}

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

def _spark_when_condition_main(F, col, condition, value):
    """Mirror of the Spark condition builder used in generated task files, for preview parity."""
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


def apply_spark_transforms(df, transforms, spark, get_right_df=None):
    """
    Apply the pipeline's transform list to a Spark DataFrame using the PySpark
    DataFrame API. This is the single, canonical transform implementation used
    by BOTH Preview Pipeline and Run Pipeline (via the generated task files),
    so results are always identical across the two.

    get_right_df(table_name) -> Spark DataFrame, used to resolve join_data's right table.
    Raises RuntimeError with a clear message on any transform error (no silent skips).
    """
    from pyspark.sql import functions as F, Window

    TYPE_MAP = {
        'TEXT': 'string', 'INTEGER': 'int', 'BIGINT': 'bigint',
        'NUMERIC': 'double', 'BOOLEAN': 'boolean',
        'DATE': 'date', 'TIMESTAMP': 'timestamp', 'VARCHAR(255)': 'string',
    }

    for i, tx in enumerate(transforms):
        ntype = tx.get("type", "")
        cfg   = tx.get("config") or {}
        try:
            if ntype == "filter_rows":
                formula = cfg.get("formula", "1=1")
                df = df.filter(F.expr(formula))

            elif ntype == "select_col":
                cols = [c for c in cfg.get("columns", []) if c in df.columns]
                missing = [c for c in cfg.get("columns", []) if c not in df.columns]
                if missing:
                    raise RuntimeError(f"Column(s) not found: {', '.join(missing)}")
                if cols:
                    df = df.select(*cols)

            elif ntype == "drop_col":
                drop = cfg.get("columns", [])
                df = df.drop(*[c for c in drop if c in df.columns])

            elif ntype == "rename_col":
                for o, n in cfg.get("renames", {}).items():
                    if o not in df.columns:
                        raise RuntimeError(f"Column not found for rename: {o}")
                    df = df.withColumnRenamed(o, n)

            elif ntype == "add_const":
                name  = cfg.get("name", "new_col")
                val   = cfg.get("value", "")
                dtype = TYPE_MAP.get(cfg.get("dtype", "TEXT"), "string")
                df = df.withColumn(name, F.lit(val).cast(dtype))

            elif ntype == "set_val":
                target = cfg.get("targetCol")
                if not target:
                    raise RuntimeError("Set Column Value: targetCol is required")
                if cfg.get("useExpr"):
                    df = df.withColumn(target, F.expr(cfg.get("expr", target)))
                else:
                    src = cfg.get("sourceCol", target)
                    if src not in df.columns:
                        raise RuntimeError(f"Set Column Value: source column not found: {src}")
                    df = df.withColumn(target, F.col(src))

            elif ntype == "val_mapper":
                src     = cfg.get("sourceCol")
                new_col = cfg.get("newColName", "mapped")
                whens   = cfg.get("whens", [])
                else_v  = cfg.get("elseValue", "")
                if not src or src not in df.columns:
                    raise RuntimeError(f"Value Mapper: source column not found: {src}")
                expr = None
                for w in whens:
                    condition = w.get("condition", "=")
                    value     = w.get("value", "")
                    result    = w.get("result", "")
                    if condition not in ("IS NULL", "IS NOT NULL") and value == "":
                        continue
                    cond_expr = _spark_when_condition_main(F, src, condition, value)
                    expr = F.when(cond_expr, F.lit(result)) if expr is None else expr.when(cond_expr, F.lit(result))
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

            elif ntype == "change_type":
                for col, dtype in (cfg.get("types") or {}).items():
                    if col not in df.columns:
                        raise RuntimeError(f"Change Type: column not found: {col}")
                    df = df.withColumn(col, F.col(col).cast(TYPE_MAP.get(dtype, "string")))

            elif ntype == "order_table":
                orders = cfg.get("orders", [])
                cols = [
                    F.col(o["col"]).asc() if o.get("dir", "ASC") == "ASC" else F.col(o["col"]).desc()
                    for o in orders if o.get("col") in df.columns
                ]
                if cols:
                    df = df.orderBy(*cols)

            elif ntype == "group_agg":
                gc = [c for c in cfg.get("groupCols", []) if c in df.columns]
                ac = cfg.get("aggCols", [])
                if not gc or not ac:
                    raise RuntimeError("Group By: groupCols and aggCols are required")
                fn_map = {
                    "COUNT": F.count, "SUM": F.sum, "AVG": F.avg,
                    "MIN": F.min, "MAX": F.max, "COUNT DISTINCT": F.countDistinct,
                }
                aggs = []
                for a in ac:
                    if a.get("col") not in df.columns:
                        raise RuntimeError(f"Group By: aggregate column not found: {a.get('col')}")
                    aggs.append(fn_map.get(a["func"], F.count)(a["col"]).alias(a.get("alias", f'{a["col"]}_{a["func"].lower()}')))
                df = df.groupBy(*gc).agg(*aggs)

            elif ntype == "calc":
                new_col = (cfg.get("newColName") or "result").strip()
                col_a, col_b, op = cfg.get("colA"), cfg.get("colB"), cfg.get("operation", "+")
                if col_a not in df.columns or col_b not in df.columns:
                    raise RuntimeError(f"Calculator: column not found ({col_a}, {col_b})")
                a, b = F.col(col_a).cast("double"), F.col(col_b).cast("double")
                expr = {"+": a + b, "-": a - b, "*": a * b, "/": F.when(b != 0, a / b)}.get(op, a + b)
                df = df.withColumn(new_col, expr)

            elif ntype == "adv_calculator":
                SCI = {"sin": F.sin, "cos": F.cos, "sqrt": F.sqrt,
                       "radians": F.radians, "atan2": F.atan2, "power": F.pow}
                for calc in cfg.get("calculations", []):
                    fn    = SCI.get(calc.get("operation", "sin"), F.sin)
                    new_c = (calc.get("newColName") or "").strip()
                    col_a, col_b = calc.get("colA"), calc.get("colB")
                    if not new_c or col_a not in df.columns:
                        continue
                    if calc.get("operation") in ("atan2", "power") and col_b in df.columns:
                        df = df.withColumn(new_c, fn(F.col(col_a).cast("double"), F.col(col_b).cast("double")))
                    else:
                        df = df.withColumn(new_c, fn(F.col(col_a).cast("double")))

            elif ntype == "combine_cols":
                new_col     = (cfg.get("newColName") or "combined").strip()
                sep         = cfg.get("separator", " ")
                selected    = [c for c in cfg.get("selectedCols", []) if c in df.columns]
                remove_orig = cfg.get("removeOriginal", False)
                if not selected:
                    raise RuntimeError("Combine Columns: no valid columns selected")
                parts = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in selected]
                combined = parts[0]
                for p in parts[1:]:
                    combined = F.concat(combined, F.lit(sep), p)
                df = df.withColumn(new_col, combined)
                if remove_orig:
                    df = df.drop(*selected)

            elif ntype == "join_data":
                right_table = cfg.get("rightTable")
                left_col    = cfg.get("leftCol")
                right_col   = cfg.get("rightCol")
                if not right_table or not left_col:
                    raise RuntimeError("Join: rightTable and leftCol are required")
                if get_right_df is None:
                    raise RuntimeError("Join: right table resolver not available")
                right_df = get_right_df(right_table)

                raw_type  = cfg.get("joinType", "INNER JOIN").upper()
                is_cross  = "CROSS" in raw_type
                join_type = raw_type.replace(" JOIN", "").lower().replace("full outer", "outer")

                if not right_col:
                    raise RuntimeError("Join: rightCol is required for non-cross joins") if not is_cross else None
                if not is_cross and (left_col not in df.columns or right_col not in right_df.columns):
                    raise RuntimeError(f"Join: column not found ({left_col} / {right_col})")

                right_join_col = right_col
                dup_cols = [c for c in right_df.columns if c in df.columns]
                for c in dup_cols:
                    new_name = f"{c}_right"
                    right_df = right_df.withColumnRenamed(c, new_name)
                    if c == right_col:
                        right_join_col = new_name

                if is_cross:
                    df = df.crossJoin(right_df)
                else:
                    df = df.join(right_df, df[left_col] == right_df[right_join_col], join_type)
                    if right_join_col != left_col:
                        df = df.drop(right_join_col)

            elif ntype == "pyspark":
                code = cfg.get("code", "")
                if code:
                    ns = {"df": df, "spark": spark, "F": F}
                    try:
                        exec(code, ns)
                    except Exception as e:
                        raise RuntimeError(f"PySpark node error: {e}")
                    df = ns.get("df", df)

            else:
                continue

        except RuntimeError:
            raise
        except Exception as e:
            raise RuntimeError(f"Step {i+1} ({ntype}) failed: {e}")

    return df


_preview_spark_session = None


def _get_preview_spark():
    """Reuse a single lightweight local Spark session for fast previews."""
    global _preview_spark_session
    if _preview_spark_session is None:
        from pyspark.sql import SparkSession
        _preview_spark_session = (
            SparkSession.builder
            .appName("ETLFlow_Preview")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate()
        )
    return _preview_spark_session


@app.post("/api/preview/transform")
def preview_transform(payload: dict):
    """
    Preview transform result using Apache Spark — the same engine and the same
    transform implementations (apply_spark_transforms) used by Run Pipeline,
    so preview results are always identical to the DAG's actual output.
    """
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

        try:
            sample_pdf = pd.read_sql(f"SELECT * FROM {tbl} LIMIT 5000", conn)
        except Exception as e:
            raise HTTPException(400, f"Failed to read dataset: {e}")

        spark = _get_preview_spark()
        try:
            df = spark.createDataFrame(sample_pdf.astype(object).where(pd.notnull(sample_pdf), None))
        except Exception as e:
            raise HTTPException(400, f"Failed to load data into Spark: {e}")

        right_df_cache = {}

        def get_right_df(right_table: str):
            if right_table in right_df_cache:
                return right_df_cache[right_table]
            try:
                r_pdf = pd.read_sql(f"SELECT * FROM {right_table} LIMIT 5000", conn)
            except Exception as e:
                raise RuntimeError(f"Join: could not read right table {right_table}: {e}")
            r_df = spark.createDataFrame(r_pdf.astype(object).where(pd.notnull(r_pdf), None))
            right_df_cache[right_table] = r_df
            return r_df

        try:
            result_df = apply_spark_transforms(df, transforms, spark, get_right_df=get_right_df)
            result_df = result_df.limit(limit)
            rows = [row.asDict(recursive=True) for row in result_df.collect()]
            columns = result_df.columns
        except RuntimeError as e:
            raise HTTPException(400, f"Preview failed: {e}")
        except Exception as e:
            raise HTTPException(400, f"Preview failed: {e}\n{traceback.format_exc()}")

        return {"columns": columns, "rows": rows}

    except HTTPException: raise
    except Exception as e: raise HTTPException(400, f"Preview failed: {e}")
    finally: cur.close(); conn.close()