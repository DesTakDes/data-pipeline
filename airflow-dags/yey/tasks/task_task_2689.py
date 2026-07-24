# Auto-generated TASK file
# Task ID   : task_2689
# DAG ID    : yey
# Output    : warehouse.plslah
# Generated : 2026-07-24T01:39:10.694636
# ─────────────────────────────────────────────────────────────────────────────
# This file contains ALL transform logic for this task.
# It is imported by the workflow DAG file (dag_yey.py).
# ─────────────────────────────────────────────────────────────────────────────

import os, re, time, json, requests, traceback
import psycopg2, psycopg2.extras
import pandas as pd

TASK_ID     = 'task_2689'
DAG_ID      = 'yey'
INPUT_TABLE = 'staging.market_stores_500'
OUTPUT_NAME = 'plslah'
TRANSFORMS  = json.loads('[{"type": "drop_col", "config": {"columns": ["phone_1", "phone_2", "email", "subscription_date", "website"]}}]')
BACKEND_URL = "http://backend:8000"
PARQUET_DIR = "/data_csv/parquet"
BATCH_INSERT_SIZE = 5000
CHUNK_ROWS  = 100_000
SHUFFLE_PARTITIONS = 200

PG_CONFIG = {
    'host': 'postgres', 'port': 5432,
    'database': 'airflow',
    'user': 'airflow', 'password': 'airflow',
}


# ── Helpers ──────────────────────────────────────────────────────────────────

def _get_conn():
    return psycopg2.connect(**PG_CONFIG)


def _q(cols):
    return ", ".join(f'"{c}"' for c in cols)

def _sql_when_fragment(col, condition, value, result):
    result_lit = repr(result)
    col_ref = f'"{col}"'
    if condition == 'IS NULL':
        return f"WHEN {col_ref} IS NULL THEN {result_lit}"
    if condition == 'IS NOT NULL':
        return f"WHEN {col_ref} IS NOT NULL THEN {result_lit}"
    if condition in ('IN', 'NOT IN'):
        vals = [v.strip() for v in str(value).split(',') if v.strip() != '']
        if not vals:
            return None
        vals_sql = ', '.join(repr(v) for v in vals)
        op = 'IN' if condition == 'IN' else 'NOT IN'
        return f"WHEN {col_ref} {op} ({vals_sql}) THEN {result_lit}"
    if condition == 'LIKE':
        return f"WHEN {col_ref} LIKE {repr(value)} THEN {result_lit}"
    if condition in ('>', '>=', '<', '<='):
        try:
            num = float(value)
        except (TypeError, ValueError):
            return None
        return f"WHEN TRY_CAST({col_ref} AS DOUBLE PRECISION) {condition} {num} THEN {result_lit}"
    op = condition if condition in ('=', '!=') else '='
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


# ── Spark Runner ──────────────────────────────────────────────────────────────

def _run_spark(input_table, output_name, transforms, row_count):
    from pyspark.sql import SparkSession, functions as F, Window

    safe_out = re.sub(r'[^a-z0-9_]','_',output_name.lower()).strip('_') or "output"
    if safe_out and safe_out[0].isdigit():
        safe_out = 't_' + safe_out
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

    df = _read_jdbc_table(spark, input_table, num_partitions=min(8, optimal_partitions))

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

                    if is_cross:
                        df = df.crossJoin(right_df)
                    elif right_col:
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

    df.write.jdbc(
        url="jdbc:postgresql://postgres:5432/airflow",
        table=f'warehouse."{safe_out}"',
        mode="overwrite",
        properties={"user": "airflow", "password": "airflow", "driver": "org.postgresql.Driver"},
    )

    if row_count > 10_000:
        os.makedirs(PARQUET_DIR, exist_ok=True)
        parquet_path = f"{PARQUET_DIR}/{safe_out}.parquet"
        output_parts = max(1, row_count // 500_000)
        (df.coalesce(output_parts).write.mode("overwrite")
           .option("compression", "snappy").parquet(parquet_path))
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
    safe_out = re.sub(r'[^a-z0-9_]','_',OUTPUT_NAME.lower()).strip('_') or "output"
    if safe_out and safe_out[0].isdigit():
        safe_out = 't_' + safe_out

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
        print(f"[Task:{TASK_ID}] {err}\n{traceback.format_exc()}")
        _report("failed", 0, err)
        raise

    try:
        rows = pg_hook.get_first(f'SELECT COUNT(*) FROM warehouse."{safe_out}"')[0]
    except Exception:
        rows = 0

    _report("success", 100, f"Done: {rows:,} rows via spark", row_count=rows)
    print(f"[Task:{TASK_ID}] Done → warehouse.{safe_out} ({rows:,} rows via spark)")
    return rows
