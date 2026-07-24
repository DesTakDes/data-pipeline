# Auto-generated SPARK TASK file
# Task ID   : task_3860
# DAG ID    : ibegyou
# Output    : warehouse.1
# Generated : 2026-07-23T12:23:44.071674
# ─────────────────────────────────────────────────────────────────────────────

import os, re, time, json, requests, traceback
from datetime import datetime
from pyspark.sql import SparkSession, functions as F, Window

TASK_ID     = 'task_3860'
DAG_ID      = 'ibegyou'
INPUT_TABLE = 'staging.market_stores_500'
OUTPUT_NAME = '1'
TRANSFORMS  = json.loads('[{"type": "join_data", "config": {"joinType": "INNER JOIN", "leftCol": "store_id", "rightCol": "store_id", "rightNodeId": "n1784809381606_2_6l3s", "rightTable": "staging.market_sales_500"}}]')
BACKEND_URL = "http://backend:8000"
PARQUET_DIR = "/data_csv/parquet"

PG_CONFIG = {
    'host': 'postgres', 'port': 5432,
    'database': 'airflow', 'user': 'airflow', 'password': 'airflow'
}

SPARK_TYPE_MAP = {
    "TEXT": "string", "VARCHAR": "string", "VARCHAR(255)": "string",
    "INTEGER": "int", "BIGINT": "bigint", "NUMERIC": "double",
    "DOUBLE": "double", "FLOAT": "double", "BOOLEAN": "boolean",
    "DATE": "date", "TIMESTAMP": "timestamp"
}

def _spark_when_condition(col, condition, value):
    c = F.col(col)
    cond = str(condition).upper().strip()
    if cond == "=": return c == value
    if cond == "!=": return c != value
    if cond in (">", ">=", "<", "<="):
        try:
            num = float(value)
            c_num = c.cast("double")
            if cond == ">": return c_num > num
            if cond == ">=": return c_num >= num
            if cond == "<": return c_num < num
            if cond == "<=": return c_num <= num
        except Exception: return F.lit(False)
    if cond == "LIKE": return c.like(str(value))
    if cond == "IS NULL": return c.isNull()
    if cond == "IS NOT NULL": return c.isNotNull()
    if cond == "IN": return c.isin(*[v.strip() for v in str(value).split(",") if v.strip()])
    if cond == "NOT IN": return ~c.isin(*[v.strip() for v in str(value).split(",") if v.strip()])
    return c == value

def run(run_ids, backend_url=BACKEND_URL):
    safe_out = re.sub(r'[^a-z0-9_]','_',OUTPUT_NAME.lower()).strip('_') or "output"
    target_table = f"warehouse.{safe_out}"

    spark = (SparkSession.builder
        .appName(f"ETLFlow_Spark_{DAG_ID}_{TASK_ID}")
        .config("spark.master", os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar")
        .config("spark.driver.extraClassPath", "/opt/spark/jars/postgresql-42.6.0.jar")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate())

    jdbc_url = f"jdbc:postgresql://{PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['database']}"
    jdbc_props = {"user": PG_CONFIG['user'], "password": PG_CONFIG['password'], "driver": "org.postgresql.Driver"}

    try:
        schema_tbl = INPUT_TABLE if "." in INPUT_TABLE else f"staging.{INPUT_TABLE}"
        schema_r, tname_r = schema_tbl.split(".", 1) if "." in schema_tbl else ("staging", schema_tbl)
        input_table_fmt = f'{schema_r}."{tname_r.strip(chr(34))}"'

        df = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", input_table_fmt).options(**jdbc_props).load()

        for idx, tx in enumerate(TRANSFORMS, start=1):
            ntype = tx.get("type", "")
            cfg   = tx.get("config") or {}

            if ntype == "filter_rows":
                df = df.filter(F.expr(cfg.get("formula", "1=1")))
            elif ntype == "select_col":
                cols = [c for c in cfg.get("columns", []) if c in df.columns]
                if cols: df = df.select(*cols)
            elif ntype == "drop_col":
                cols = [c for c in cfg.get("columns", []) if c in df.columns]
                if cols: df = df.drop(*cols)
            elif ntype == "rename_col":
                for old_c, new_c in (cfg.get("renames") or {}).items():
                    if old_c in df.columns and new_c: df = df.withColumnRenamed(old_c, new_c)
            elif ntype == "add_const":
                name = cfg.get("name", "new_col")
                val  = cfg.get("value", "")
                dtype = SPARK_TYPE_MAP.get(str(cfg.get("dtype","TEXT")).upper(), "string")
                if name: df = df.withColumn(name, F.lit(val).cast(dtype))
            elif ntype == "change_type":
                for col_name, dtype_str in (cfg.get("types") or {}).items():
                    if col_name in df.columns:
                        sp_type = SPARK_TYPE_MAP.get(str(dtype_str).upper(), "string")
                        df = df.withColumn(col_name, F.col(col_name).cast(sp_type))
            elif ntype == "set_val":
                target = cfg.get("targetCol")
                if target:
                    if cfg.get("useExpr"):
                        df = df.withColumn(target, F.expr(cfg.get("expr", f"`{target}`")))
                    else:
                        src = cfg.get("sourceCol", target)
                        if src in df.columns: df = df.withColumn(target, F.col(src))
            elif ntype == "val_mapper":
                src = cfg.get("sourceCol")
                new_col = cfg.get("newColName", "mapped")
                whens = cfg.get("whens", [])
                else_v = cfg.get("elseValue", "")
                if src and src in df.columns:
                    case_expr = None
                    for w in whens:
                        cond = w.get("condition", "=")
                        val  = w.get("value", "")
                        res  = w.get("result", "")
                        cond_expr = _spark_when_condition(src, cond, val)
                        if case_expr is None: case_expr = F.when(cond_expr, F.lit(res))
                        else: case_expr = case_expr.when(cond_expr, F.lit(res))
                    if case_expr is not None: df = df.withColumn(new_col, case_expr.otherwise(F.lit(else_v)))
                    else: df = df.withColumn(new_col, F.lit(else_v))
            elif ntype == "fill_null":
                fc    = [c for c in cfg.get("columns", []) if c in df.columns]
                ftype = cfg.get("fillType", "value")
                fval  = cfg.get("fillValue", "")
                if fc:
                    if ftype == "value": df = df.fillna(fval, subset=fc)
                    elif ftype == "mean":
                        stats = df.select([F.mean(F.col(c)).alias(c) for c in fc]).collect()[0].asDict()
                        df = df.fillna({k: float(v) for k, v in stats.items() if v is not None})
                    elif ftype in ("forward", "backward"):
                        df = df.withColumn("_mono_id", F.monotonically_increasing_id())
                        win = Window.orderBy("_mono_id").rowsBetween(Window.unboundedPreceding, 0) if ftype == "forward" else Window.orderBy("_mono_id").rowsBetween(0, Window.unboundedFollowing)
                        fn = F.last if ftype == "forward" else F.first
                        for c in fc: df = df.withColumn(c, fn(F.col(c), ignorenulls=True).over(win))
                        df = df.drop("_mono_id")
            elif ntype == "order_table":
                orders = [F.col(o["col"]).asc() if str(o.get("dir","ASC")).upper() == "ASC" else F.col(o["col"]).desc() for o in cfg.get("orders",[]) if o.get("col") in df.columns]
                if orders: df = df.orderBy(*orders)
            elif ntype == "group_agg":
                gcols = [c for c in cfg.get("groupCols", []) if c in df.columns]
                acols = cfg.get("aggCols", [])
                fn_map = {"SUM": F.sum, "COUNT": F.count, "AVG": F.avg, "MIN": F.min, "MAX": F.max, "COUNT DISTINCT": F.countDistinct}
                aggs = [fn_map.get(str(a.get("func","COUNT")).upper(), F.count)(a["col"]).alias(a.get("alias", a["col"])) for a in acols if a.get("col") in df.columns]
                if gcols and aggs: df = df.groupBy(*gcols).agg(*aggs)
            elif ntype == "join_data":
                right_table = cfg.get("rightTable", "")
                left_col    = cfg.get("leftCol", "")
                right_col   = cfg.get("rightCol", "")
                raw_type    = str(cfg.get("joinType", "INNER JOIN")).upper()
                is_cross    = "CROSS" in raw_type
                join_type   = raw_type.replace(" JOIN", "").lower().replace("full outer", "outer")
                if right_table and left_col:
                    r_schema, r_tname = right_table.split(".", 1) if "." in right_table else ("staging", right_table)
                    right_df = spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", f'{r_schema}."{r_tname.strip(chr(34))}"').options(**jdbc_props).load()
                    dup_cols = [c for c in right_df.columns if c in df.columns and c != right_col]
                    for c in dup_cols: right_df = right_df.withColumnRenamed(c, f"{c}_right")
                    if is_cross: df = df.crossJoin(right_df)
                    elif right_col: df = df.join(right_df, df[left_col] == right_df[right_col], join_type)
            elif ntype == "pyspark":
                code = cfg.get("code", "")
                if code and code.strip():
                    l_scope = {"df": df, "spark": spark, "F": F, "Window": Window}
                    exec(code, l_scope)
                    df = l_scope.get("df", df)

        df.write.format("jdbc").option("url", jdbc_url).option("dbtable", target_table).option("mode", "overwrite").options(**jdbc_props).mode("overwrite").save()

        row_count = df.count()

        for run_id in run_ids:
            try:
                requests.patch(f"{backend_url}/api/pipelines/runs/{run_id}", json={"status": "success", "row_count": row_count, "progress_pct": 100, "message": f"Done: {row_count:,} rows via Spark"}, timeout=5)
            except Exception: pass

        print(f"[SparkTask:{TASK_ID}] Done → {target_table} ({row_count:,} rows)")
        return row_count

    except Exception as e:
        err_msg = str(e)[:400]
        for run_id in run_ids:
            try:
                requests.patch(f"{backend_url}/api/pipelines/runs/{run_id}", json={"status": "failed", "message": err_msg}, timeout=5)
            except Exception: pass
        raise RuntimeError(f"Spark Task {TASK_ID} failed: {e}") from e
    finally:
        spark.stop()
