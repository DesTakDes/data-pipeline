# Auto-generated Spark DAG: pipeline_dag_dayakaya
# Workflow: Pipeline dayakaya
# Generated: 2026-05-19T08:12:19.329960

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import json, requests, os, math

DAG_ID      = 'pipeline_dag_dayakaya'
INPUT_TABLE = 'staging.mcdonald_s_reviews'
WORKFLOW_ID = 'dag_dayakaya'
TASKS_DEF   = json.loads('[{"task_id": "task_2", "output_name": "dayakaya", "transforms": [{"type": "select_col", "config": {}}], "depends_on": []}]')
BACKEND_URL = "http://backend:8000"

default_args = {"owner": "etlflow", "retries": 0}


def get_schema(pg, table_name):
    if "." not in table_name:
        table_name = f"staging.{table_name}"
    schema_name, tbl = table_name.split(".", 1)
    rows = pg.get_records(f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = '{schema_name}' AND table_name = '{tbl}'
        AND column_name NOT IN ('_id','_date_partition','_processed_at','loaded_at','date_partition')
        ORDER BY ordinal_position
    """)
    schema = {}
    for col, dtype in rows:
        if   "int"       in dtype: schema[col] = "BIGINT"
        elif "numeric"   in dtype or "float" in dtype: schema[col] = "NUMERIC"
        elif "timestamp" in dtype: schema[col] = "TIMESTAMP"
        elif "date"      in dtype: schema[col] = "DATE"
        elif "bool"      in dtype: schema[col] = "BOOLEAN"
        else:                      schema[col] = "TEXT"
    return schema

def q(cols):
    return ", ".join(f'"{c}"' for c in cols)

def detect_spark_config(row_count, col_count=10):
    """Heuristic-based Spark resource sizing."""
    estimated_mb = row_count * col_count * 50 / (1024 * 1024)
    if estimated_mb < 100:
        return {"executor_memory": "1g", "executor_cores": 1, "num_executors": 1, "dynamic": False}
    elif estimated_mb < 1000:
        return {"executor_memory": "2g", "executor_cores": 2, "num_executors": 2, "dynamic": False}
    elif estimated_mb < 10000:
        return {"executor_memory": "4g", "executor_cores": 4, "num_executors": 4, "dynamic": True}
    else:
        return {"executor_memory": "8g", "executor_cores": 8, "num_executors": 8, "dynamic": True}

def run_task(task_def, **context):
    import subprocess
    pg = PostgresHook(postgres_conn_id="postgres_default")
    conf = context.get("dag_run").conf or {}
    run_ids = conf.get("run_ids", [])
    task_id = task_def.get("task_id", "task_1")
    output_name = task_def.get("output_name", "output")
    transforms  = task_def.get("transforms", [])

    safe_output = output_name.lower().replace(" ", "_")
    import re as re
    safe_output = re.sub(r'[^a-z0-9_]', '_', safe_output)

    # Detect input size
    tbl = INPUT_TABLE if "." in INPUT_TABLE else f"staging.{INPUT_TABLE}"
    sch, tname = tbl.split(".", 1)
    exists = pg.get_first(f"""
        SELECT EXISTS (SELECT FROM information_schema.tables
        WHERE table_schema = '{sch}' AND table_name = '{tname}')
    """)[0]
    if not exists:
        raise ValueError(f"Table {tbl} not found")

    row_count = pg.get_first(f"SELECT COUNT(*) FROM {tbl}")[0]
    schema    = get_schema(pg, tbl)
    col_count = len(schema)
    spark_cfg = detect_spark_config(row_count, col_count)

    print(f"[Spark] Task: {task_id} | Rows: {row_count} | Cols: {col_count}")
    print(f"[Spark] Config: {spark_cfg}")

    # Try PySpark if available, else fallback to PostgreSQL transforms
    spark_available = False
    try:
        import importlib.util
        spark_available = importlib.util.find_spec("pyspark") is not None
    except:
        pass

    if spark_available:
        run_with_spark(pg, tbl, safe_output, transforms, row_count, spark_cfg, task_id)
    else:
        run_with_postgres(pg, tbl, safe_output, transforms, task_id, row_count)

    # Update backend run status
    out = f"warehouse.{safe_output}"
    count = pg.get_first(f"SELECT COUNT(*) FROM {out}")[0]

    for run_id in run_ids:
        try:
            requests.patch(f"{BACKEND_URL}/api/pipelines/runs/{run_id}",
                json={"status": "success", "row_count": count}, timeout=5)
        except Exception as e:
            print(f"[Task] Backend update failed: {e}")

    print(f"[Done] Task {task_id} → {out} ({count} rows)")


def run_with_spark(pg, input_table, output_name, transforms, row_count, spark_cfg, task_id):
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import *

    # Build SparkSession with right-sized resources
    builder = SparkSession.builder \
        .appName(f"ETLFlow_{DAG_ID}_{task_id}") \
        .config("spark.master", "spark://spark:7077") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar") \
        .config("spark.executor.memory", spark_cfg["executor_memory"]) \
        .config("spark.executor.cores", str(spark_cfg["executor_cores"])) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.broadcastTimeout", "300")

    if spark_cfg.get("dynamic"):
        builder = builder \
            .config("spark.dynamicAllocation.enabled", "true") \
            .config("spark.dynamicAllocation.minExecutors", "1") \
            .config("spark.dynamicAllocation.maxExecutors", str(spark_cfg["num_executors"]))

    spark = builder.getOrCreate()

    # Read from PostgreSQL
    jdbc_url = "jdbc:postgresql://postgres:5432/airflow"
    jdbc_props = {"user": "airflow", "password": "airflow", "driver": "org.postgresql.Driver"}

    # Determine optimal partitions
    num_partitions = max(1, min(8, row_count // 100000))

    df = spark.read.jdbc(
        url=jdbc_url, table=f"({'SELECT * FROM ' + input_table}) AS t",
        numPartitions=num_partitions, properties=jdbc_props
    )

    # Apply transforms
    df = apply_spark_transforms(spark, df, transforms)

    # Partitioning & format optimization
    if row_count > 1000000:
        df = df.repartition(max(4, num_partitions))
    elif row_count > 100000:
        df = df.coalesce(max(2, num_partitions // 2))

    # Cache if multiple operations needed
    if len(transforms) > 3:
        df.cache()

    # Write to warehouse via JDBC (columnar-optimized)
    df.write.jdbc(
        url=jdbc_url,
        table=f"warehouse.{output_name}",
        mode="overwrite",
        properties={**jdbc_props, "createTableOptions": "WITH (fillfactor=90)"}
    )

    # Also save as Parquet for large datasets
    if row_count > 100000:
        parquet_path = f"/data_csv/parquet/{output_name}.parquet"
        os.makedirs("/data_csv/parquet", exist_ok=True)
        df.write.mode("overwrite").parquet(parquet_path)
        print(f"[Spark] Saved Parquet: {parquet_path}")

    spark.stop()


def apply_spark_transforms(spark, df, transforms):
    from pyspark.sql import functions as F

    for tx in transforms:
        ntype  = tx.get("type", "")
        config = tx.get("config") or {}

        try:
            if ntype == "filter_rows":
                formula = config.get("formula", "1=1")
                df = df.filter(formula)

            elif ntype == "select_col":
                cols = config.get("columns", [])
                valid = [c for c in cols if c in df.columns]
                if valid:
                    df = df.select(valid)

            elif ntype == "drop_col":
                drop = config.get("columns", [])
                keep = [c for c in df.columns if c not in drop]
                df = df.select(keep)

            elif ntype == "rename_col":
                renames = config.get("renames", {})
                for old, new in renames.items():
                    if old in df.columns:
                        df = df.withColumnRenamed(old, new)

            elif ntype == "add_const":
                name  = config.get("name", "new_col")
                val   = config.get("value", "NULL")
                df = df.withColumn(name, F.lit(val))

            elif ntype == "fill_null":
                fill_cols = config.get("columns", [])
                fill_val  = config.get("fillValue", "")
                fill_type = config.get("fillType", "value")
                for c in fill_cols:
                    if c not in df.columns:
                        continue
                    if fill_type == "value":
                        df = df.fillna({c: fill_val})
                    elif fill_type == "mean":
                        mean_val = df.agg(F.mean(c)).collect()[0][0]
                        df = df.fillna({c: mean_val})

            elif ntype == "order_table":
                orders = config.get("orders", [])
                sort_cols = []
                for o in orders:
                    col = o.get("col")
                    if col and col in df.columns:
                        sort_cols.append(F.col(col).asc() if o.get("dir","ASC") == "ASC" else F.col(col).desc())
                if sort_cols:
                    df = df.orderBy(sort_cols)

            elif ntype == "change_type":
                types = config.get("types", {})
                type_map = {"TEXT":"string","INTEGER":"integer","BIGINT":"long",
                            "NUMERIC":"double","BOOLEAN":"boolean","DATE":"date","TIMESTAMP":"timestamp"}
                for c, t in types.items():
                    if c in df.columns:
                        spark_type = type_map.get(t, "string")
                        df = df.withColumn(c, F.col(c).cast(spark_type))

            elif ntype == "group_agg":
                gcols = [c for c in config.get("groupCols", []) if c in df.columns]
                acols = config.get("aggCols", [])
                if gcols and acols:
                    agg_exprs = []
                    func_map = {"COUNT": F.count, "SUM": F.sum, "AVG": F.avg,
                                "MIN": F.min, "MAX": F.max, "COUNT DISTINCT": F.countDistinct}
                    for a in acols:
                        fn = func_map.get(a["func"], F.count)
                        agg_exprs.append(fn(a["col"]).alias(a["alias"]))
                    df = df.groupBy(gcols).agg(*agg_exprs)

        except Exception as e:
            print(f"[Spark] Transform {ntype} failed: {e}, skipping")

    return df


def run_with_postgres(pg, input_table, output_name, transforms, task_id, row_count):
    """Fallback: run transforms using PostgreSQL SQL."""
    print(f"[PG] Running transforms for {task_id} via PostgreSQL")
    schema = get_schema(pg, input_table)
    cur_cols = list(schema.keys())
    current = input_table
    step = 0

    pg.run("CREATE SCHEMA IF NOT EXISTS warehouse")
    pg.run("CREATE SCHEMA IF NOT EXISTS staging")

    # Clean up old temp tables
    temps = pg.get_records(f"""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'staging' AND table_name LIKE '_{DAG_ID}_{task_id}_step_%'
    """)
    for (t,) in temps:
        pg.run(f'DROP TABLE IF EXISTS staging."{t}"')

    for tx in transforms:
        ntype  = tx.get("type", "")
        config = tx.get("config") or {}
        step  += 1
        tmp    = f"staging._{DAG_ID}_{task_id}_step_{step}"

        cur_schema = get_schema(pg, current)
        cur_cols   = list(cur_schema.keys())
        all_q      = q(cur_cols)

        try:
            if ntype == "filter_rows":
                formula = config.get("formula", "1=1")
                pg.run(f"CREATE TABLE {tmp} AS SELECT * FROM {current} WHERE {formula}")
            elif ntype == "select_col":
                cols = [c for c in config.get("columns", cur_cols) if c in cur_cols]
                if cols:
                    pg.run(f"CREATE TABLE {tmp} AS SELECT {q(cols)} FROM {current}")
                    cur_cols = cols
                else:
                    tmp = current
            elif ntype == "drop_col":
                keep = [c for c in cur_cols if c not in set(config.get("columns", []))]
                pg.run(f"CREATE TABLE {tmp} AS SELECT {q(keep)} FROM {current}")
            elif ntype == "rename_col":
                renames = config.get("renames", {})
                exprs = ", ".join(f'"{c}" AS "{renames.get(c, c)}"' for c in cur_cols)
                pg.run(f"CREATE TABLE {tmp} AS SELECT {exprs} FROM {current}")
            elif ntype == "add_const":
                name  = config.get("name", "new_col")
                val   = config.get("value", "NULL")
                dtype = config.get("dtype", "TEXT")
                pg.run(f'CREATE TABLE {tmp} AS SELECT {all_q}, CAST({repr(val)} AS {dtype}) AS "{name}" FROM {current}')
            elif ntype == "fill_null":
                fill_cols = config.get("columns", [])
                fill_val  = config.get("fillValue", "")
                exprs_list = []
                for c in cur_cols:
                    if c in fill_cols:
                        exprs_list.append(f'COALESCE("{c}"::TEXT, {repr(str(fill_val))})::TEXT AS "{c}"')
                    else:
                        exprs_list.append(f'"{c}"')
                pg.run(f"CREATE TABLE {tmp} AS SELECT {', '.join(exprs_list)} FROM {current}")
            elif ntype == "order_table":
                orders = config.get("orders", [])
                oc = ", ".join(f'"{o["col"]}" {o.get("dir","ASC")}' for o in orders if o.get("col") in cur_cols) or "1"
                pg.run(f"CREATE TABLE {tmp} AS SELECT {all_q} FROM {current} ORDER BY {oc}")
            elif ntype == "change_type":
                types = config.get("types", {})
                exprs = ", ".join(
                    f'"{c}"::TEXT::{types[c]} AS "{c}"' if c in types else f'"{c}"'
                    for c in cur_cols
                )
                pg.run(f"CREATE TABLE {tmp} AS SELECT {exprs} FROM {current}")
            elif ntype == "group_agg":
                gcols = [c for c in config.get("groupCols", []) if c in cur_cols]
                acols = config.get("aggCols", [])
                if gcols and acols:
                    g = q(gcols)
                    a = ", ".join(f'{x["func"]}("{x["col"]}") AS "{x["alias"]}"' for x in acols)
                    pg.run(f"CREATE TABLE {tmp} AS SELECT {g}, {a} FROM {current} GROUP BY {g}")
                else:
                    tmp = current
            else:
                tmp = current
        except Exception as e:
            print(f"[PG] Step {step} ({ntype}) error: {e}")
            tmp = current

        if tmp != current:
            current = tmp

    # Load to warehouse
    final_schema = get_schema(pg, current)
    out = f"warehouse.{output_name}"
    pg.run(f"DROP TABLE IF EXISTS {out}")
    col_defs = ", ".join(f'"{c}" {dt}' for c, dt in final_schema.items())
    pg.run(f"""CREATE TABLE {out} ({col_defs}, date_partition DATE DEFAULT CURRENT_DATE, loaded_at TIMESTAMP DEFAULT NOW())""")
    col_names = q(final_schema.keys())
    pg.run(f"""INSERT INTO {out} ({col_names}, date_partition, loaded_at) SELECT {col_names}, CURRENT_DATE, NOW() FROM {current}""")

    # Cleanup temp tables
    temps2 = pg.get_records(f"""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'staging' AND table_name LIKE '_{DAG_ID}_{task_id}_step_%'
    """)
    for (t,) in temps2:
        pg.run(f'DROP TABLE IF EXISTS staging."{t}"')



with DAG(
    dag_id='pipeline_dag_dayakaya',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "spark", "generated", 'dag_dayakaya'],
    description='',
) as dag:
    airflow_tasks = {}
    for task_def in TASKS_DEF:
        tid = task_def["task_id"]
        t = PythonOperator(
            task_id=tid,
            python_callable=run_task,
            op_kwargs={"task_def": task_def},
        )
        airflow_tasks[tid] = t

    # Set up task dependencies (multi-branch)
    for task_def in TASKS_DEF:
        tid = task_def["task_id"]
        for dep_tid in task_def.get("depends_on", []):
            if dep_tid in airflow_tasks:
                airflow_tasks[dep_tid] >> airflow_tasks[tid]
