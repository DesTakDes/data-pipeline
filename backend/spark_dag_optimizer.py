"""
spark_dag_optimizer.py
═══════════════════════════════════════════════════════════════════════════
Modul terpisah untuk generate Spark DAG yang dioptimalkan.
Fitur utama:
  1. Shared Node   — branch yang punya upstream sama → baca data 1x, cache bersama
  2. Materialize   — intermediate result berat disimpan ke Parquet sementara
  3. Hybrid Exec   — branch ringan paralel, branch berat antri sequential
  4. Columnar I/O  — semua output ke Parquet + partitionBy("date")
  5. Broadcast Join— tabel kecil otomatis broadcast, hindari shuffle besar
  6. Repartition   — otomatis repartition jika perlu
  7. Dynamic Alloc — spark.dynamicAllocation + sizing berdasarkan file size

Dipakai oleh main.py → generate_spark_dag() cukup import fungsi ini.
═══════════════════════════════════════════════════════════════════════════
"""
from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _safe(name: str) -> str:
    """Sanitize identifier for Python / SQL."""
    s = re.sub(r"[^a-z0-9_]", "_", name.lower())
    return re.sub(r"_+", "_", s).strip("_")


def _build_dependency_graph(tasks: list[dict]) -> dict[str, list[str]]:
    """
    Balikkan depends_on → {task_id: [children_task_ids]}
    """
    graph: dict[str, list[str]] = {t["task_id"]: [] for t in tasks}
    for t in tasks:
        for dep in t.get("depends_on", []):
            if dep in graph:
                graph[dep].append(t["task_id"])
    return graph


def _detect_shared_roots(tasks: list[dict]) -> dict[str, list[str]]:
    """
    Temukan tasks yang berbagi upstream yang sama (tidak punya depends_on).
    Return: { "shared_root_alias": [task_id, ...] }
    Jika ≥2 tasks membaca dari input_table yang sama tanpa depends_on,
    mereka bisa share 1 DataFrame yang di-cache.
    """
    root_tasks = [t for t in tasks if not t.get("depends_on")]
    # Semua root tasks membaca input_table yang sama → 1 shared node
    if len(root_tasks) >= 2:
        return {"_shared_input": [t["task_id"] for t in root_tasks]}
    return {}


def _classify_tasks(tasks: list[dict]) -> dict[str, str]:
    """
    Klasifikasikan setiap task sebagai 'light' atau 'heavy'
    berdasarkan jumlah dan tipe transform.

    light : ≤3 transform, tidak ada group_agg / join
    heavy : >3 transform, atau ada group_agg / join / pyspark
    """
    HEAVY_TYPES = {"group_agg", "join_data", "pyspark", "adv_calculator"}
    result = {}
    for t in tasks:
        transforms = t.get("transforms", [])
        has_heavy  = any(tx.get("type") in HEAVY_TYPES for tx in transforms)
        count      = len(transforms)
        result[t["task_id"]] = "heavy" if (has_heavy or count > 3) else "light"
    return result


def _execution_plan(
    tasks: list[dict],
    execution_mode: str = "hybrid",
) -> dict[str, Any]:
    """
    Buat execution plan:
      parallel   → semua task jalan paralel (ignores heavy)
      sequential → semua task jalan satu-satu
      hybrid     → light paralel, heavy antri
    """
    classification = _classify_tasks(tasks)
    lights  = [t for t in tasks if classification[t["task_id"]] == "light"]
    heavies = [t for t in tasks if classification[t["task_id"]] == "heavy"]

    if execution_mode == "parallel":
        return {"parallel": [t["task_id"] for t in tasks], "sequential": []}
    elif execution_mode == "sequential":
        return {"parallel": [], "sequential": [t["task_id"] for t in tasks]}
    else:  # hybrid
        return {
            "parallel":   [t["task_id"] for t in lights],
            "sequential": [t["task_id"] for t in heavies],
        }


# ─────────────────────────────────────────────────────────────────────────────
# Main code-gen
# ─────────────────────────────────────────────────────────────────────────────

def generate_optimized_spark_dag(
    dag_id:          str,
    workflow_id:     str,
    workflow_name:   str,
    input_table:     str,
    tasks:           list[dict],
    description:     str = "",
    execution_mode:  str = "hybrid",   # parallel | sequential | hybrid
) -> str:
    """
    Generate Python DAG file yang dioptimalkan.
    Semua fitur aktif secara otomatis berdasarkan analisis tasks.
    """
    safe_input  = re.sub(r"[^a-zA-Z0-9_.]", "", input_table)
    safe_wf_id  = workflow_id.replace("'", "")
    safe_name   = workflow_name.replace("'", "").replace('"', "")
    now_str     = datetime.now().isoformat()
    tasks_json  = json.dumps(tasks, ensure_ascii=True)

    shared_roots  = _detect_shared_roots(tasks)
    has_shared    = bool(shared_roots)
    exec_plan     = _execution_plan(tasks, execution_mode)
    classification= _classify_tasks(tasks)
    exec_plan_json= json.dumps(exec_plan)
    class_json    = json.dumps(classification)

    lines: list[str] = []

    # ── File header ────────────────────────────────────────────────────────
    lines += [
        f"# Auto-generated Optimized Spark DAG: {dag_id}",
        f"# Workflow: {safe_name}",
        f"# Generated: {now_str}",
        f"# Execution mode: {execution_mode}",
        f"# Shared nodes: {has_shared}",
        "",
        "from airflow import DAG",
        "from airflow.operators.python import PythonOperator",
        "from airflow.providers.postgres.hooks.postgres import PostgresHook",
        "from datetime import datetime",
        "import json, requests, os, sys, math",
        "",
        f"DAG_ID       = {repr(dag_id)}",
        f"INPUT_TABLE  = {repr(safe_input)}",
        f"WORKFLOW_ID  = {repr(safe_wf_id)}",
        f"TASKS_DEF    = json.loads({repr(tasks_json)})",
        f'BACKEND_URL  = "http://backend:8000"',
        f"EXEC_PLAN    = {exec_plan_json}",
        f"TASK_CLASS   = {class_json}",
        f"EXEC_MODE    = {repr(execution_mode)}",
        "",
        'default_args = {"owner": "etlflow", "retries": 1, "retry_delay": 5}',
        "",
    ]

    # ── Shared utilities ───────────────────────────────────────────────────
    lines.append(_SHARED_UTILS)

    # ── Shared node materializer ───────────────────────────────────────────
    if has_shared:
        lines.append(_SHARED_NODE_CODE)

    # ── Per-task runner ────────────────────────────────────────────────────
    lines.append(_TASK_RUNNER_CODE)

    # ── DAG definition ─────────────────────────────────────────────────────
    lines += [
        "",
        f"with DAG(",
        f"    dag_id={repr(dag_id)},",
        f"    default_args=default_args,",
        f"    schedule_interval=None,",
        f"    start_date=datetime(2024, 1, 1),",
        f"    catchup=False,",
        f"    max_active_tasks=4,",
        f"    tags=['etl', 'spark', 'optimized', {repr(safe_wf_id)}],",
        f"    description={repr(description)},",
        f") as dag:",
        "",
    ]

    # ── Shared input materializer task (jika ada shared root) ─────────────
    if has_shared:
        lines += [
            "    # ── Task: Materialize shared input (dibaca 1x, di-cache) ──────────",
            "    materialize_input = PythonOperator(",
            "        task_id='_materialize_shared_input',",
            "        python_callable=materialize_shared_input,",
            "    )",
            "",
        ]

    # ── Build individual task operators ───────────────────────────────────
    lines += [
        "    airflow_tasks = {}",
        "    for task_def in TASKS_DEF:",
        "        tid = task_def['task_id']",
        "        op = PythonOperator(",
        "            task_id=tid,",
        "            python_callable=run_task,",
        "            op_kwargs={'task_def': task_def},",
        "            pool=('spark_heavy_pool' if TASK_CLASS.get(tid) == 'heavy' else 'default_pool'),",
        "        )",
        "        airflow_tasks[tid] = op",
        "",
        "    # ── Wiring: dependency edges ─────────────────────────────────────",
        "    for task_def in TASKS_DEF:",
        "        tid = task_def['task_id']",
        "        deps = task_def.get('depends_on', [])",
        "        if deps:",
        "            for dep_tid in deps:",
        "                if dep_tid in airflow_tasks:",
        "                    airflow_tasks[dep_tid] >> airflow_tasks[tid]",
    ]

    if has_shared:
        lines += [
            "        else:",
            "            # Root tasks: tunggu materialization selesai dulu",
            "            materialize_input >> airflow_tasks[tid]",
        ]

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# Embedded code blocks (ditulis sebagai raw string, diinjeksi ke DAG file)
# ─────────────────────────────────────────────────────────────────────────────

_SHARED_UTILS = '''
# ══════════════════════════════════════════════════════════════════════════════
# SHARED UTILITIES
# ══════════════════════════════════════════════════════════════════════════════

PARQUET_TEMP_DIR = "/data_csv/parquet/_materialized"
PARQUET_OUT_DIR  = "/data_csv/parquet"

def get_schema(pg, table_name):
    if "." not in table_name:
        table_name = f"staging.{table_name}"
    schema_name, tbl = table_name.split(".", 1)
    rows = pg.get_records(f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = \'{schema_name}\' AND table_name = \'{tbl}\'
        AND column_name NOT IN (\'_id\',\'_date_partition\',\'_processed_at\',\'loaded_at\',\'date_partition\')
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
    return ", ".join(f\'"{c}"\' for c in cols)


def estimate_size_mb(pg, table, row_count):
    """Sample 1000 baris → ekstrapolasi ukuran dataset."""
    sample = min(1000, row_count)
    rows   = pg.get_records(f"SELECT * FROM {table} LIMIT {sample}")
    sample_bytes = sum(sys.getsizeof(str(r)) for r in rows)
    return (sample_bytes / max(sample, 1) * row_count) / (1024 * 1024)


def get_spark_resources():
    """Query Spark master API untuk tahu resource yang tersedia."""
    import requests as _req
    try:
        r = _req.get("http://spark:8080/json/", timeout=3)
        d = r.json()
        alive = [w for w in d.get("workers", []) if w["state"] == "ALIVE"]
        return {
            "total_cores": sum(w["cores"]  for w in alive),
            "total_mem_mb": sum(w["memory"] for w in alive),
            "worker_count": len(alive),
            "available": len(alive) > 0,
        }
    except:
        return {"total_cores": 2, "total_mem_mb": 2048, "worker_count": 1, "available": False}


def build_spark_config(estimated_mb, cluster):
    """
    Hitung config Spark berdasarkan ukuran data aktual + resource cluster.
    Tidak pernah pakai lebih dari 75% memory dan 80% core.
    """
    safe_mem   = cluster["total_mem_mb"] * 0.75
    safe_cores = max(1, int(cluster["total_cores"] * 0.80))
    workers    = max(1, cluster["worker_count"])

    if estimated_mb < 50:
        return {
            "use_spark": False,
            "reason": f"Data {estimated_mb:.1f}MB — PostgreSQL lebih cepat",
            "executor_memory": "512m", "executor_cores": 1, "num_executors": 1,
            "dynamic": False, "partitions": 1,
        }
    elif estimated_mb < 500:
        mem   = max(512, min(1024, int(safe_mem / workers)))
        parts = 2
        return {
            "use_spark": True,
            "executor_memory": f"{mem}m", "executor_cores": min(2, safe_cores),
            "num_executors": min(2, workers), "dynamic": False, "partitions": parts,
            "extra_configs": {
                "spark.sql.shuffle.partitions": str(parts),
            }
        }
    elif estimated_mb < 5000:
        n_exec = max(2, min(workers, int(estimated_mb / 500)))
        mem    = max(1024, min(4096, int(safe_mem / n_exec)))
        cores  = max(1, safe_cores // n_exec)
        parts  = max(4, n_exec * cores * 2)
        return {
            "use_spark": True,
            "executor_memory": f"{mem}m", "executor_cores": cores,
            "num_executors": n_exec, "dynamic": True, "partitions": parts,
            "extra_configs": {
                "spark.sql.shuffle.partitions": str(parts),
            }
        }
    else:
        mem   = max(2048, min(8192, int(safe_mem / workers)))
        parts = max(8, safe_cores * 3)
        return {
            "use_spark": True,
            "executor_memory": f"{mem}m",
            "executor_cores": max(1, safe_cores // workers),
            "num_executors": workers, "dynamic": True, "partitions": parts,
            "extra_configs": {
                "spark.sql.shuffle.partitions":        str(parts),
                "spark.sql.adaptive.skewJoin.enabled": "true",
                "spark.memory.fraction":               "0.8",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            },
        }


def create_spark_session(dag_id, task_id, cfg):
    """
    Buat SparkSession dengan config yang sudah dihitung.
    dynamicAllocation = True jika cfg["dynamic"] = True.
    """
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder
        .appName(f"ETLFlow_{dag_id}_{task_id}")
        .config("spark.master", "spark://spark:7077")
        .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar")
        .config("spark.executor.memory", cfg["executor_memory"])
        .config("spark.executor.cores",  str(cfg["executor_cores"]))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    )

    if cfg.get("dynamic"):
        builder = (
            builder
            .config("spark.dynamicAllocation.enabled",             "true")
            .config("spark.dynamicAllocation.shuffleTracking.enabled", "true")
            .config("spark.dynamicAllocation.minExecutors",         "1")
            .config("spark.dynamicAllocation.maxExecutors",  str(cfg["num_executors"]))
            .config("spark.dynamicAllocation.initialExecutors",     "1")
        )
    else:
        builder = (
            builder
            .config("spark.dynamicAllocation.enabled", "false")
            .config("spark.executor.instances",  str(cfg["num_executors"]))
        )

    for k, v in cfg.get("extra_configs", {}).items():
        builder = builder.config(k, v)

    return builder.getOrCreate()


# ── Columnar I/O ────────────────────────────────────────────────────────────

JDBC_URL   = "jdbc:postgresql://postgres:5432/airflow"
JDBC_PROPS = {"user": "airflow", "password": "airflow", "driver": "org.postgresql.Driver"}

def read_jdbc(spark, table, num_partitions=4):
    return spark.read.jdbc(
        url=JDBC_URL,
        table=f"(SELECT * FROM {table}) AS t",
        numPartitions=num_partitions,
        properties=JDBC_PROPS,
    )

def write_parquet_partitioned(df, output_name, num_partitions=4):
    """
    Simpan ke Parquet dengan partitionBy("date").
    Jika kolom tanggal tidak ada, simpan tanpa partisi.
    """
    import os
    out_path = f"{PARQUET_OUT_DIR}/{output_name}"
    os.makedirs(out_path, exist_ok=True)

    date_cols = [c for c in df.columns if "date" in c.lower() or "tgl" in c.lower()]

    if date_cols:
        part_col = date_cols[0]
        print(f"[Parquet] partitionBy(\'{part_col}\') → {out_path}")
        (
            df.repartition(num_partitions, part_col)
            .write.mode("overwrite")
            .partitionBy(part_col)
            .parquet(out_path)
        )
    else:
        print(f"[Parquet] no date column, unpartitioned → {out_path}")
        (
            df.coalesce(max(1, num_partitions // 2))
            .write.mode("overwrite")
            .parquet(out_path)
        )
    return out_path


def write_jdbc(df, output_name, num_partitions=4):
    """Tulis ke warehouse PostgreSQL."""
    (
        df.repartition(num_partitions)
        .write.jdbc(
            url=JDBC_URL,
            table=f"warehouse.{output_name}",
            mode="overwrite",
            properties=JDBC_PROPS,
        )
    )


# ── Join optimizer (broadcast kecil, shuffle besar) ────────────────────────

BROADCAST_THRESHOLD_MB = 128   # tabel < 128 MB → broadcast

def smart_join(df_left, df_right, left_col, right_col, join_type="inner",
               right_size_mb=None):
    """
    Otomatis pilih broadcast join (tabel kecil) atau shuffle join (tabel besar).
    """
    from pyspark.sql import functions as F

    right_size_mb = right_size_mb or 0
    if right_size_mb < BROADCAST_THRESHOLD_MB:
        print(f"[Join] broadcast join ({right_size_mb:.1f}MB < {BROADCAST_THRESHOLD_MB}MB threshold)")
        df_right_b = F.broadcast(df_right)
        return df_left.join(df_right_b, df_left[left_col] == df_right_b[right_col], join_type)
    else:
        print(f"[Join] shuffle join ({right_size_mb:.1f}MB ≥ threshold)")
        return df_left.join(df_right, df_left[left_col] == df_right[right_col], join_type)


# ── Transform engine (Spark) ────────────────────────────────────────────────

def apply_spark_transforms(spark, df, transforms):
    from pyspark.sql import functions as F

    for tx in transforms:
        ntype  = tx.get("type", "")
        config = tx.get("config") or {}
        try:
            if ntype == "filter_rows":
                df = df.filter(config.get("formula", "1=1"))

            elif ntype == "select_col":
                cols = [c for c in config.get("columns", []) if c in df.columns]
                if cols: df = df.select(cols)

            elif ntype == "drop_col":
                drop = set(config.get("columns", []))
                df   = df.select([c for c in df.columns if c not in drop])

            elif ntype == "rename_col":
                for old, new in config.get("renames", {}).items():
                    if old in df.columns:
                        df = df.withColumnRenamed(old, new)

            elif ntype == "add_const":
                df = df.withColumn(config.get("name", "new_col"), F.lit(config.get("value", "")))

            elif ntype == "fill_null":
                for c in config.get("columns", []):
                    if c not in df.columns: continue
                    ft = config.get("fillType", "value")
                    if ft == "value":
                        df = df.fillna({c: config.get("fillValue", "")})
                    elif ft == "mean":
                        mean_val = df.agg(F.mean(c)).collect()[0][0]
                        if mean_val is not None: df = df.fillna({c: mean_val})

            elif ntype == "order_table":
                sort_cols = []
                for o in config.get("orders", []):
                    col = o.get("col")
                    if col and col in df.columns:
                        sort_cols.append(F.col(col).asc() if o.get("dir","ASC")=="ASC" else F.col(col).desc())
                if sort_cols: df = df.orderBy(sort_cols)

            elif ntype == "change_type":
                type_map = {
                    "TEXT":"string","INTEGER":"integer","BIGINT":"long",
                    "NUMERIC":"double","BOOLEAN":"boolean",
                    "DATE":"date","TIMESTAMP":"timestamp",
                }
                for c, t in config.get("types", {}).items():
                    if c in df.columns:
                        df = df.withColumn(c, F.col(c).cast(type_map.get(t, "string")))

            elif ntype == "group_agg":
                gcols = [c for c in config.get("groupCols", []) if c in df.columns]
                acols = config.get("aggCols", [])
                if gcols and acols:
                    func_map = {
                        "COUNT": F.count, "SUM": F.sum, "AVG": F.avg,
                        "MIN": F.min, "MAX": F.max, "COUNT DISTINCT": F.countDistinct,
                    }
                    agg_exprs = [
                        func_map.get(a["func"], F.count)(a["col"]).alias(a["alias"])
                        for a in acols
                    ]
                    df = df.groupBy(gcols).agg(*agg_exprs)

            elif ntype == "join_data":
                # join_data memerlukan DataFrame kedua — diambil via JDBC
                right_table = config.get("rightTable", "")
                if right_table:
                    df_right    = spark.read.jdbc(url=JDBC_URL, table=right_table, properties=JDBC_PROPS)
                    right_rows  = df_right.count()
                    # estimasi kasar: 200 byte/row
                    right_mb    = (right_rows * 200) / (1024 * 1024)
                    df = smart_join(
                        df, df_right,
                        config.get("leftCol", "id"),
                        config.get("rightCol", "id"),
                        config.get("joinType", "inner").lower().replace(" join",""),
                        right_size_mb=right_mb,
                    )

            elif ntype == "pyspark":
                code = config.get("code", "")
                if code:
                    local_ns = {"df": df, "spark": spark, "F": F}
                    exec(code, local_ns)
                    df = local_ns["df"]

        except Exception as e:
            print(f"[Transform] {ntype} error: {e} — skipped")

    return df


# ── PostgreSQL fallback ─────────────────────────────────────────────────────

def run_with_postgres(pg, input_table, output_name, transforms, task_id, row_count):
    """Fallback ke SQL pure PostgreSQL jika Spark tidak tersedia / data kecil."""
    print(f"[PG] Fallback PostgreSQL: {task_id}")
    schema   = get_schema(pg, input_table)
    cur_cols = list(schema.keys())
    current  = input_table
    step     = 0

    pg.run("CREATE SCHEMA IF NOT EXISTS warehouse")
    pg.run("CREATE SCHEMA IF NOT EXISTS staging")

    # Bersihkan temp table lama
    for (t,) in pg.get_records(f"""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = \'staging\'
        AND table_name LIKE \'_{DAG_ID}_{task_id}_step_%\'
    """):
        pg.run(f\'DROP TABLE IF EXISTS staging."{t}"\')

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
                pg.run(f"CREATE TABLE {tmp} AS SELECT * FROM {current} WHERE {config.get(\'formula\',\'1=1\')}")
            elif ntype == "select_col":
                cols = [c for c in config.get("columns", cur_cols) if c in cur_cols]
                if cols: pg.run(f"CREATE TABLE {tmp} AS SELECT {q(cols)} FROM {current}")
                else: tmp = current
            elif ntype == "drop_col":
                keep = [c for c in cur_cols if c not in set(config.get("columns", []))]
                pg.run(f"CREATE TABLE {tmp} AS SELECT {q(keep)} FROM {current}")
            elif ntype == "rename_col":
                renames = config.get("renames", {})
                exprs   = ", ".join(f\'"{c}" AS "{renames.get(c,c)}"\' for c in cur_cols)
                pg.run(f"CREATE TABLE {tmp} AS SELECT {exprs} FROM {current}")
            elif ntype == "add_const":
                name  = config.get("name","new_col"); val = config.get("value","")
                dtype = config.get("dtype","TEXT")
                pg.run(f\'CREATE TABLE {tmp} AS SELECT {all_q}, CAST({repr(val)} AS {dtype}) AS "{name}" FROM {current}\')
            elif ntype == "fill_null":
                fill_cols = config.get("columns",[]); fill_val = config.get("fillValue","")
                exprs_list = []
                for c in cur_cols:
                    if c in fill_cols:
                        exprs_list.append(f\'COALESCE("{c}"::TEXT,{repr(str(fill_val))})::TEXT AS "{c}"\')
                    else:
                        exprs_list.append(f\'"{c}"\')
                pg.run(f"CREATE TABLE {tmp} AS SELECT {chr(44).join(exprs_list)} FROM {current}")
            elif ntype == "order_table":
                orders = config.get("orders",[])
                oc = ", ".join(f\'"{o["col"]}" {o.get("dir","ASC")}\' for o in orders if o.get("col") in cur_cols) or "1"
                pg.run(f"CREATE TABLE {tmp} AS SELECT {all_q} FROM {current} ORDER BY {oc}")
            elif ntype == "change_type":
                types = config.get("types",{})
                exprs = ", ".join(
                    f\'"{c}"::TEXT::{types[c]} AS "{c}"\' if c in types else f\'"{c}"\'
                    for c in cur_cols
                )
                pg.run(f"CREATE TABLE {tmp} AS SELECT {exprs} FROM {current}")
            elif ntype == "group_agg":
                gcols = [c for c in config.get("groupCols",[]) if c in cur_cols]
                acols = config.get("aggCols",[])
                if gcols and acols:
                    g = q(gcols)
                    a = ", ".join(f\'{x["func"]}("{x["col"]}") AS "{x["alias"]}"\' for x in acols)
                    pg.run(f"CREATE TABLE {tmp} AS SELECT {g}, {a} FROM {current} GROUP BY {g}")
                else:
                    tmp = current
            else:
                tmp = current
        except Exception as e:
            print(f"[PG] step {step} ({ntype}): {e}")
            tmp = current

        if tmp != current:
            current = tmp

    # Load ke warehouse
    final_schema = get_schema(pg, current)
    out  = f"warehouse.{output_name}"
    cols_str = q(final_schema.keys())
    col_defs = ", ".join(f\'"{c}" {dt}\' for c, dt in final_schema.items())
    pg.run(f"DROP TABLE IF EXISTS {out}")
    pg.run(f"""CREATE TABLE {out} (
        {col_defs},
        date_partition DATE DEFAULT CURRENT_DATE,
        loaded_at TIMESTAMP DEFAULT NOW()
    )""")
    pg.run(f"""INSERT INTO {out} ({cols_str}, date_partition, loaded_at)
               SELECT {cols_str}, CURRENT_DATE, NOW() FROM {current}""")

    # Cleanup temp
    for (t,) in pg.get_records(f"""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = \'staging\'
        AND table_name LIKE \'_{DAG_ID}_{task_id}_step_%\'
    """):
        pg.run(f\'DROP TABLE IF EXISTS staging."{t}"\')
'''

# ─────────────────────────────────────────────────────────────────────────────
# Shared Node Materializer
# ─────────────────────────────────────────────────────────────────────────────

_SHARED_NODE_CODE = '''
# ══════════════════════════════════════════════════════════════════════════════
# SHARED NODE — Materialize input 1x ke Parquet sementara
# Semua branch root (tanpa depends_on) membaca dari file ini
# ══════════════════════════════════════════════════════════════════════════════

def materialize_shared_input(**context):
    """
    Baca INPUT_TABLE 1x dari PostgreSQL → simpan ke Parquet temp.
    Branch root tasks akan membaca dari Parquet ini, bukan query ulang ke DB.
    Hemat I/O, hindari multiple full-table scans.
    """
    import os
    pg        = PostgresHook(postgres_conn_id="postgres_default")
    tbl       = INPUT_TABLE if "." in INPUT_TABLE else f"staging.{INPUT_TABLE}"
    sch, tname = tbl.split(".", 1)

    exists = pg.get_first(f"""
        SELECT EXISTS (SELECT FROM information_schema.tables
        WHERE table_schema = \'{sch}\' AND table_name = \'{tname}\')
    """)[0]
    if not exists:
        raise ValueError(f"[SharedNode] Tabel {tbl} tidak ditemukan")

    row_count = pg.get_first(f"SELECT COUNT(*) FROM {tbl}")[0]
    cluster   = get_spark_resources()
    est_mb    = estimate_size_mb(pg, tbl, row_count)
    cfg       = build_spark_config(est_mb, cluster)

    print(f"[SharedNode] {row_count:,} rows | {est_mb:.1f}MB | {cluster}")
    print(f"[SharedNode] Spark config: {cfg}")

    mat_path = f"{PARQUET_TEMP_DIR}/{DAG_ID}_shared_input"
    os.makedirs(PARQUET_TEMP_DIR, exist_ok=True)

    if not cfg.get("use_spark", True):
        # Kecil → baca via pandas, simpan parquet
        import pandas as pd
        print(f"[SharedNode] Data kecil → pandas → parquet")
        df_pd = pd.read_sql(f"SELECT * FROM {tbl}", pg.get_conn())
        df_pd.to_parquet(mat_path, index=False, engine="pyarrow", compression="snappy")
        print(f"[SharedNode] Parquet saved (pandas): {mat_path}")
        return

    # Besar → Spark
    spark = create_spark_session(DAG_ID, "_shared", cfg)
    try:
        df = read_jdbc(spark, tbl, num_partitions=cfg.get("partitions", 4))
        df.cache()                     # cache di memory Spark
        actual_count = df.count()     # force evaluation + populate cache

        # Simpan ke Parquet temp untuk dibaca branch-branch
        (
            df.write
            .mode("overwrite")
            .option("compression", "snappy")
            .parquet(mat_path)
        )
        print(f"[SharedNode] Parquet materialized: {mat_path} ({actual_count:,} rows)")
        df.unpersist()
    finally:
        spark.stop()

    # Simpan path ke XCom agar branch bisa baca
    return mat_path
'''

# ─────────────────────────────────────────────────────────────────────────────
# Per-task runner
# ─────────────────────────────────────────────────────────────────────────────

_TASK_RUNNER_CODE = '''
# ══════════════════════════════════════════════════════════════════════════════
# TASK RUNNER
# ══════════════════════════════════════════════════════════════════════════════

def run_task(task_def, **context):
    """
    Jalankan satu task (branch).
    - Cek apakah input tersedia sebagai Parquet materialized (shared node)
    - Atau baca langsung dari PostgreSQL
    - Terapkan transforms
    - Simpan ke warehouse (PostgreSQL) + Parquet columnar
    - Patch backend run status
    """
    import os, importlib.util
    pg       = PostgresHook(postgres_conn_id="postgres_default")
    conf     = context.get("dag_run").conf or {}
    run_ids  = conf.get("run_ids", [])

    task_id    = task_def.get("task_id", "task_1")
    output_name= task_def.get("output_name", "output")
    transforms = task_def.get("transforms", [])

    import re as _re
    safe_output = _re.sub(r\'[^a-z0-9_]\', \'_\', output_name.lower())
    if safe_output and safe_output[0].isdigit(): safe_output = "t_" + safe_output
    safe_output = safe_output or "output"

    tbl    = INPUT_TABLE if "." in INPUT_TABLE else f"staging.{INPUT_TABLE}"
    sch, tname = tbl.split(".", 1)

    # ── Cek materialized Parquet (dari shared node) ──────────────────────
    mat_path = f"{PARQUET_TEMP_DIR}/{DAG_ID}_shared_input"
    has_mat  = os.path.exists(mat_path)

    row_count = pg.get_first(f"SELECT COUNT(*) FROM {tbl}")[0]
    cluster   = get_spark_resources()
    est_mb    = estimate_size_mb(pg, tbl, row_count)
    cfg       = build_spark_config(est_mb, cluster)

    task_class = TASK_CLASS.get(task_id, "light")
    print(f"[Task] {task_id} | class={task_class} | {row_count:,} rows | {est_mb:.1f}MB")
    print(f"[Task] Materialized input: {has_mat} | Spark config: {cfg}")

    # ── Route: Spark vs PostgreSQL ────────────────────────────────────────
    spark_available = importlib.util.find_spec("pyspark") is not None

    if not cfg.get("use_spark", True) or not spark_available:
        print(f"[Route] → PostgreSQL fallback (Spark: {spark_available}, size ok: {cfg.get(\'use_spark\')})")
        run_with_postgres(pg, tbl, safe_output, transforms, task_id, row_count)
    else:
        print(f"[Route] → Spark ({est_mb:.1f}MB)")
        _run_with_spark_optimized(
            task_id, safe_output, transforms, row_count,
            cfg, tbl, mat_path, has_mat,
        )

    # ── Catat jumlah output ───────────────────────────────────────────────
    out_table = f"warehouse.{safe_output}"
    count = pg.get_first(f"SELECT COUNT(*) FROM {out_table}")[0]

    for run_id in run_ids:
        try:
            requests.patch(
                f"{BACKEND_URL}/api/pipelines/runs/{run_id}",
                json={"status": "success", "row_count": count},
                timeout=5,
            )
        except Exception as e:
            print(f"[Task] Backend update failed: {e}")

    print(f"[Done] {task_id} → {out_table} ({count:,} rows)")


def _run_with_spark_optimized(
    task_id, output_name, transforms, row_count,
    cfg, input_table, mat_path, has_mat,
):
    """
    Spark execution dengan optimasi penuh:
    - Baca dari Parquet materialized jika tersedia (hemat JDBC scan)
    - Cache DataFrame jika ada banyak transform
    - Repartition otomatis
    - Simpan ke warehouse + Parquet columnar + partitionBy(date)
    """
    import os
    spark = create_spark_session(DAG_ID, task_id, cfg)
    num_partitions = cfg.get("partitions", 4)

    try:
        # ── 1. Baca data ─────────────────────────────────────────────────
        if has_mat:
            print(f"[Spark] Baca dari materialized Parquet: {mat_path}")
            df = spark.read.parquet(mat_path)
            # Repartition agar sesuai jumlah executor
            df = df.repartition(num_partitions)
        else:
            print(f"[Spark] Baca langsung dari JDBC: {input_table}")
            df = read_jdbc(spark, input_table, num_partitions)

        # ── 2. Cache jika banyak transform ───────────────────────────────
        if len(transforms) > 2:
            df.cache()
            df.count()   # force evaluation
            print(f"[Spark] DataFrame di-cache ({len(transforms)} transforms)")

        # ── 3. Apply transforms ───────────────────────────────────────────
        df = apply_spark_transforms(spark, df, transforms)

        # ── 4. Repartition sebelum write (hindari small files) ────────────
        output_partitions = max(1, min(num_partitions, row_count // 100_000 + 1))
        df = df.repartition(output_partitions)

        # ── 5. Write ke warehouse PostgreSQL ─────────────────────────────
        write_jdbc(df, output_name, output_partitions)
        print(f"[Spark] Written to warehouse.{output_name}")

        # ── 6. Write ke Parquet columnar (partitionBy date jika ada) ─────
        parquet_path = write_parquet_partitioned(df, output_name, output_partitions)
        print(f"[Spark] Parquet output: {parquet_path}")

        if len(transforms) > 2:
            df.unpersist()

    finally:
        spark.stop()
'''


# ─────────────────────────────────────────────────────────────────────────────
# Public API — dipakai oleh main.py
# ─────────────────────────────────────────────────────────────────────────────

def generate_spark_dag(
    dag_id:         str,
    workflow_id:    str,
    workflow_name:  str,
    input_table:    str,
    tasks:          list[dict],
    description:    str = "",
    execution_mode: str = "hybrid",
) -> str:
    """
    Drop-in replacement untuk generate_spark_dag() di main.py.
    Signature sama, tapi output jauh lebih optimal.

    Cara pakai di main.py:
        from spark_dag_optimizer import generate_spark_dag
        # Hapus fungsi generate_spark_dag() yang lama dari main.py
    """
    return generate_optimized_spark_dag(
        dag_id=dag_id,
        workflow_id=workflow_id,
        workflow_name=workflow_name,
        input_table=input_table,
        tasks=tasks,
        description=description,
        execution_mode=execution_mode,
    )