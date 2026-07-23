"""
spark_engine.py
Spark-based preview & smart execution engine untuk ETLFlow.

Optimasi yang diimplementasikan:
  - SparkSession singleton (reuse across calls, hindari overhead start/stop)
  - Dynamic config berdasarkan ukuran input (measure size -> generate config)
  - Source dibaca SEKALI per preview call, DataFrame-nya dipakai ulang oleh semua branch
  - Shared node (fan-out > 1) dimaterialize ke Parquet sekali, dipakai ulang semua children
  - Materialize intermediate ke Parquet snappy (memutus lineage panjang, hemat I/O)
  - Broadcast join untuk tabel kanan yang kecil
  - Repartition sebelum operasi shuffle berat (group_agg, order_table)
"""

import os
import json
import hashlib
import shutil
from typing import Optional

import pandas as pd
import spark_config

PARQUET_DIR      = "/data_csv/parquet"
CACHE_DIR        = "/data_csv/spark_cache"   # materialized intermediate nodes
BROADCAST_MAX_MB = 200                        # tabel di bawah ukuran ini akan di-broadcast

_spark_session = None  # singleton per-proses, dipakai ulang antar preview call


# ════════════════════════════════════════════════════════════════════════════
# 1. DYNAMIC SPARK CONFIG — "measure size -> generate config -> submit"
# ════════════════════════════════════════════════════════════════════════════

SPARK_TYPE_MAP = {
    "TEXT": "string", "INTEGER": "int", "BIGINT": "bigint",
    "NUMERIC": "double", "BOOLEAN": "boolean",
    "DATE": "date", "TIMESTAMP": "timestamp", "VARCHAR(255)": "string",
}

def _pg_table_size_mb(table: str) -> float:
    """
    Estimasi ukuran tabel via katalog Postgres (pg_total_relation_size) —
    TIDAK memicu Spark action, jadi murah dan tidak jadi bottleneck.
    Dipakai untuk keputusan broadcast join.
    """
    import psycopg2
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=int(os.getenv("POSTGRES_PORT", 5432)),
            database=os.getenv("POSTGRES_DB", "airflow"),
            user=os.getenv("POSTGRES_USER", "airflow"),
            password=os.getenv("POSTGRES_PASSWORD", "airflow"),
        )
        cur = conn.cursor()
        cur.execute("SELECT pg_total_relation_size(%s) / 1024.0 / 1024.0", (table,))
        val = float(cur.fetchone()[0] or 0)
        cur.close(); conn.close()
        return val
    except Exception:
        return 9999.0  # tidak diketahui -> jangan broadcast, aman


def _build_when_condition(col: str, condition: str, value):
    """
    Bangun kondisi Spark untuk SEMUA jenis condition di val_mapper,
    bukan cuma '='. Cocok dengan daftar CONDITIONS di UtilityConfigs.jsx:
    ["=","!=",">",">=","<","<=","LIKE","IS NULL","IS NOT NULL","IN","NOT IN"]
    """
    from pyspark.sql import functions as F
    c = F.col(col)

    if condition == "=":
        return c == value
    if condition == "!=":
        return c != value
    if condition in (">", ">=", "<", "<="):
        num = float(value)
        c_num = c.cast("double")
        return {">": c_num > num, ">=": c_num >= num,
                "<": c_num < num, "<=": c_num <= num}[condition]
    if condition == "LIKE":
        return c.like(value)
    if condition == "IS NULL":
        return c.isNull()
    if condition == "IS NOT NULL":
        return c.isNotNull()
    if condition == "IN":
        vals = [v.strip() for v in str(value).split(",")]
        return c.isin(*vals)
    if condition == "NOT IN":
        vals = [v.strip() for v in str(value).split(",")]
        return ~c.isin(*vals)
    return c == value  # fallback aman

def compute_spark_config(size_mb: float) -> dict:
    """Smart-scaling: config Spark otomatis mengikuti ukuran data input."""
    if size_mb < 100:
        return {
            "executor_memory": "1g", "executor_cores": "1",
            "max_executors": "1", "shuffle_partitions": 8,
            "dynamic_allocation": "false",
        }
    elif size_mb < 1000:
        return {
            "executor_memory": "2g", "executor_cores": "2",
            "max_executors": "3", "shuffle_partitions": 50,
            "dynamic_allocation": "true",
        }
    elif size_mb < 10_000:
        return {
            "executor_memory": "4g", "executor_cores": "2",
            "max_executors": "6", "shuffle_partitions": 200,
            "dynamic_allocation": "true",
        }
    else:
        return {
            "executor_memory": "8g", "executor_cores": "4",
            "max_executors": "12", "shuffle_partitions": max(400, int(size_mb / 100)),
            "dynamic_allocation": "true",
        }


def get_spark_session(size_mb: float = 0, resource_config: Optional[dict] = None):
    """
    Reuse satu SparkSession sepanjang proses hidup.
    Config diterapkan HANYA saat sesi pertama kali dibuat.
    """
    global _spark_session
    if _spark_session is not None:
        try:
            _spark_session.sql("SELECT 1").collect()
            return _spark_session
        except Exception:
            _spark_session = None

    from pyspark.sql import SparkSession

    profile = spark_config.estimate_dataset_profile(file_size_bytes=max(int(size_mb * 1024 * 1024), 0), row_count=0, col_count=0)
    profile["size_mb"] = size_mb
    session_cfg = spark_config.get_runtime_spark_session_config(profile)
    if resource_config:
        session_cfg.update({k: str(v) if isinstance(v, (int, float)) else v for k, v in resource_config.items()})

    builder = SparkSession.builder.appName("ETLFlow_Preview_Engine")
    for key, value in session_cfg.items():
        builder = builder.config(key, str(value))
    _spark_session = builder.getOrCreate()
    print(f"[SparkEngine] SparkSession dibuat dengan config: {session_cfg}")
    return _spark_session


# ════════════════════════════════════════════════════════════════════════════
# 2. ANALISIS GRAPH — deteksi shared node (fan-out > 1)
# ════════════════════════════════════════════════════════════════════════════

def compute_fanout(nodes: list, edges: list) -> dict:
    """{node_id: jumlah edge keluar} — fan-out > 1 berarti node ini shared."""
    fanout = {n["id"]: 0 for n in nodes}
    for e in edges:
        if e["source"] in fanout:
            fanout[e["source"]] += 1
    return fanout


def topo_order(nodes: list, edges: list) -> list:
    """Kahn's algorithm — urutan topological node ids."""
    in_degree = {n["id"]: 0 for n in nodes}
    graph     = {n["id"]: [] for n in nodes}
    for e in edges:
        if e["source"] in graph:
            graph[e["source"]].append(e["target"])
        if e["target"] in in_degree:
            in_degree[e["target"]] += 1

    queue = [nid for nid, d in in_degree.items() if d == 0]
    order = []
    while queue:
        cur = queue.pop(0)
        order.append(cur)
        for nxt in graph.get(cur, []):
            in_degree[nxt] -= 1
            if in_degree[nxt] == 0:
                queue.append(nxt)
    return order


def node_signature(node_id: str, node_map: dict, edges: list) -> str:
    """
    Hash stabil dari config node + signature rantai upstream-nya.
    Dipakai sebagai cache key sehingga rantai upstream yang identik
    (shared node) hanya dihitung sekali, dan preview ulang dengan
    config yang tidak berubah bisa langsung pakai parquet yang sudah ada.
    """
    node   = node_map[node_id]
    config = node.get("data", {}).get("config") or {}
    ntype  = node.get("data", {}).get("type", "")

    parent_edge = next((e for e in edges if e["target"] == node_id), None)
    parent_sig  = node_signature(parent_edge["source"], node_map, edges) if parent_edge else "root"

    payload = json.dumps({"type": ntype, "config": config, "parent": parent_sig}, sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


# ════════════════════════════════════════════════════════════════════════════
# 3. MATERIALIZE / CACHE — "hemat I/O, hindari baca ulang"
# ════════════════════════════════════════════════════════════════════════════

def _cache_path(sig: str) -> str:
    return os.path.join(CACHE_DIR, f"{sig}.parquet")


def materialize(df, sig: str):
    """
    Tulis DataFrame ke Parquet (snappy) lalu baca ulang.
    Ini memutus lineage panjang (Spark tidak perlu recompute seluruh
    rantai upstream tiap kali node ini disentuh lagi) dan memberi
    setiap branch downstream sumber yang murah & sudah dihitung.
    """
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = _cache_path(sig)
    if os.path.exists(path):
        return df.sparkSession.read.parquet(path)  # sudah ada -> reuse, 0 recompute

    (df.write.mode("overwrite").option("compression", "snappy").parquet(path))
    return df.sparkSession.read.parquet(path)


def cached_or_none(spark, sig: str):
    path = _cache_path(sig)
    return spark.read.parquet(path) if os.path.exists(path) else None


def clear_cache():
    if os.path.exists(CACHE_DIR):
        shutil.rmtree(CACHE_DIR)


# ════════════════════════════════════════════════════════════════════════════
# 4. TRANSFORM PER NODE — Spark native
# ════════════════════════════════════════════════════════════════════════════

def apply_node_transform(spark, df, node: dict, node_map: dict, right_df_lookup, right_size_lookup=None):
    """
    Terapkan satu transform utility node ke Spark DataFrame.

    right_size_lookup: callable(right_node_id) -> float (MB), dipakai untuk
    keputusan broadcast join di node join_data. Opsional (default None)
    supaya caller lama yang hanya mengirim 5 argumen (mis. task file hasil
    generate_task_file di main.py) tetap kompatibel.
    """
    from pyspark.sql import functions as F

    ntype  = node["data"]["type"]
    config = node["data"].get("config") or {}

    if ntype == "select_col":
        cols = [c for c in config.get("columns", []) if c in df.columns]
        return df.select(*cols) if cols else df

    if ntype == "drop_col":
        drop = [c for c in config.get("columns", []) if c in df.columns]
        return df.drop(*drop) if drop else df

    if ntype == "rename_col":
        for old, new in (config.get("renames") or {}).items():
            if old in df.columns:
                df = df.withColumnRenamed(old, new)
        return df

    if ntype == "add_const":
        return df.withColumn(config.get("name", "new_col"), F.lit(config.get("value", "")))

    if ntype == "set_val":
        target = config.get("targetCol")
        if not target:
            return df
        if config.get("useExpr"):
            return df.withColumn(target, F.expr(config.get("expr", target)))
        src = config.get("sourceCol", target)
        return df.withColumn(target, F.col(src)) if src in df.columns else df

    if ntype == "val_mapper":
        src, new_col = config.get("sourceCol"), config.get("newColName", "mapped")
        whens, else_v = config.get("whens", []), config.get("elseValue", "")
        if not src or src not in df.columns:
            return df
        expr = None
        for w in whens:
            condition = w.get("condition", "=")
            # IS NULL / IS NOT NULL tidak butuh value; selain itu wajib ada value.
            if condition not in ("IS NULL", "IS NOT NULL") and not w.get("value"):
                continue
            try:
                cond = _build_when_condition(src, condition, w.get("value"))
            except Exception as e:
                print(f"[SparkEngine] val_mapper: kondisi tidak valid ({condition}): {e} — dilewati")
                continue
            expr = F.when(cond, F.lit(w.get("result"))) if expr is None else expr.when(cond, F.lit(w.get("result")))
        expr = expr.otherwise(F.lit(else_v)) if expr is not None else F.lit(else_v)
        return df.withColumn(new_col, expr)

    if ntype == "fill_null":
        fill_cols, fill_val = config.get("columns", []), config.get("fillValue", "")
        if config.get("fillType", "value") == "value" and fill_cols:
            return df.fillna(fill_val, subset=[c for c in fill_cols if c in df.columns])
        return df

    if ntype == "filter_rows":
        return df.filter(F.expr(config.get("formula", "1=1")))

    if ntype == "order_table":
        orders = config.get("orders", [])
        if not orders:
            return df
        cols = [F.col(o["col"]).asc() if o.get("dir", "ASC") == "ASC" else F.col(o["col"]).desc()
                for o in orders if o.get("col") in df.columns]
        # ORDER BY = full shuffle -> repartition dulu agar sort tidak
        # bottleneck di satu partisi yang skewed.
        df = df.repartition(int(spark.conf.get("spark.sql.shuffle.partitions")))
        return df.orderBy(*cols) if cols else df

    if ntype == "group_agg":
        gcols = [c for c in config.get("groupCols", []) if c in df.columns]
        acols = config.get("aggCols", [])
        if not gcols or not acols:
            return df
        fn_map = {"COUNT": F.count, "SUM": F.sum, "AVG": F.avg, "MIN": F.min,
                  "MAX": F.max, "COUNT DISTINCT": F.countDistinct}
        aggs = [fn_map.get(a["func"], F.count)(a["col"]).alias(a["alias"])
                for a in acols if a.get("col") in df.columns]
        if not aggs:
            return df
        # Repartition berdasarkan group-key SEBELUM aggregasi -> hindari
        # random shuffle raksasa, aggregasi jadi lebih lokal per partisi.
        df = df.repartition(*[F.col(c) for c in gcols])
        return df.groupBy(*gcols).agg(*aggs)

    if ntype == "join_data":
        right_df = right_df_lookup(config.get("rightNodeId"))
        if right_df is None:
            return df
        left_col, right_col = config.get("leftCol"), config.get("rightCol")
        if not left_col or not right_col:
            return df
        join_type = (config.get("joinType", "INNER JOIN")
                     .replace(" JOIN", "").lower().replace("full outer", "outer"))

        # Broadcast tabel kanan yang kecil -> hindari shuffle join sama sekali.
        # Ukuran diambil dari right_size_lookup (mis. pg_total_relation_size),
        # bukan dengan menghitung df secara langsung (mahal & bisa trigger job).
        right_size_mb = right_size_lookup(config.get("rightNodeId")) if right_size_lookup else BROADCAST_MAX_MB + 1
        if right_size_mb <= BROADCAST_MAX_MB:
            right_df = F.broadcast(right_df)
            print(f"[SparkEngine] join_data: broadcast tabel kanan (~{right_size_mb:.1f} MB)")

        return df.join(right_df, df[left_col] == right_df[right_col], join_type)

    if ntype == "calc":
        new_col = (config.get("newColName") or "result").strip()
        col_a, col_b, op = config.get("colA"), config.get("colB"), config.get("operation", "+")
        if not (new_col and col_a and col_b):
            return df
        a, b = F.col(col_a).cast("double"), F.col(col_b).cast("double")
        expr = {"+": a + b, "-": a - b, "*": a * b, "/": F.when(b != 0, a / b)}.get(op, a + b)
        return df.withColumn(new_col, expr)

    if ntype == "adv_calculator":
        SCI = {"sin": F.sin, "cos": F.cos, "sqrt": F.sqrt, "radians": F.radians,
               "atan2": F.atan2, "power": F.pow}
        for calc in config.get("calculations", []):
            fn    = SCI.get(calc.get("operation", "sin"), F.sin)
            new_c = (calc.get("newColName") or "").strip()
            col_a, col_b = calc.get("colA"), calc.get("colB")
            if not new_c or not col_a:
                continue
            if calc.get("operation") in ("atan2", "power") and col_b:
                df = df.withColumn(new_c, fn(F.col(col_a).cast("double"), F.col(col_b).cast("double")))
            else:
                df = df.withColumn(new_c, fn(F.col(col_a).cast("double")))
        return df

    if ntype == "combine_cols":
        new_col, sep = (config.get("newColName") or "combined").strip(), config.get("separator", " ")
        selected = [c for c in config.get("selectedCols", []) if c in df.columns]
        remove_orig = config.get("removeOriginal", False)
        if not new_col or not selected:
            return df
        parts = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in selected]
        combined = parts[0]
        for p in parts[1:]:
            combined = F.concat(combined, F.lit(sep), p)
        df = df.withColumn(new_col, combined)
        if remove_orig:
            df = df.drop(*selected)
        return df

    if ntype == "change_type":
        TYPE_MAP = {"TEXT": "string", "INTEGER": "int", "BIGINT": "bigint",
                    "NUMERIC": "double", "BOOLEAN": "boolean",
                    "DATE": "date", "TIMESTAMP": "timestamp", "VARCHAR(255)": "string"}
        for col, dtype in (config.get("types") or {}).items():
            if col in df.columns:
                df = df.withColumn(col, F.col(col).cast(TYPE_MAP.get(dtype, "string")))
        return df

    if ntype == "pyspark":
        code = config.get("code", "")
        if code:
            ns = {"df": df, "spark": spark, "F": F}
            try:
                exec(code, ns)
                return ns.get("df", df)
            except Exception as e:
                print(f"[SparkEngine] pyspark node error: {e} — pass-through")
        return df

    return df


# ════════════════════════════════════════════════════════════════════════════
# 5. ENTRY POINT PREVIEW
# ════════════════════════════════════════════════════════════════════════════

def read_source_once(spark, table: str):
    """Baca tabel staging via JDBC — dipanggil SEKALI, hasilnya dipakai ulang."""
    pg_host = os.getenv("POSTGRES_HOST", "postgres")
    pg_port = os.getenv("POSTGRES_PORT", "5432")
    pg_db   = os.getenv("POSTGRES_DB", "airflow")
    jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"
    return (spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", f"(SELECT * FROM {table}) AS t")
            .option("user", os.getenv("POSTGRES_USER", "airflow"))
            .option("password", os.getenv("POSTGRES_PASSWORD", "airflow"))
            .option("driver", "org.postgresql.Driver")
            .load())


def preview_pipeline_spark(
    input_table: str,
    size_mb: float,
    nodes: list,
    edges: list,
    target_node_id: str,
    limit: int = 100,
) -> dict:
    """
    Preview hasil `target_node_id` (utility node ATAU output node) TANPA
    menjalankan pipeline sungguhan / menulis ke warehouse.
    """
    spark = get_spark_session(size_mb)

    node_map = {n["id"]: n for n in nodes}
    fanout   = compute_fanout(nodes, edges)
    order    = topo_order(nodes, edges)

    materialized = {}
    right_sizes  = {} 
    cache_hits, cache_writes = [], []

    def resolve_right_df(right_node_id):
        if not right_node_id or right_node_id not in node_map:
            return None
        if right_node_id in materialized:
            return materialized[right_node_id]
        rnode = node_map[right_node_id]
        ds = rnode.get("data", {}).get("config", {}).get("dataset")
        if not ds:
            return None
        table_name = ds.get("table_name") or ds.get("name")
        r_table = f'staging."{table_name}"'
        r_df = read_source_once(spark, r_table)
        materialized[right_node_id] = r_df
        right_sizes[right_node_id]  = _pg_table_size_mb(r_table)  # NEW
        return r_df

    def resolve_right_size(right_node_id):                       # NEW
        return right_sizes.get(right_node_id, 9999.0)

    # ── Baca source utama SEKALI ─────────────────────────────────────────
    input_node = next((n for n in nodes if n["data"]["type"] == "input_dataset"), None)
    if input_node:
        materialized[input_node["id"]] = read_source_once(spark, input_table)

    # ── Jalan di urutan topological, materialize shared node ────────────
    for nid in order:
        node  = node_map[nid]
        ntype = node["data"]["type"]

        if ntype in ("input_dataset", "output_dataset"):
            continue

        parent_edge = next((e for e in edges if e["target"] == nid), None)
        if not parent_edge:
            continue
        parent_df = materialized.get(parent_edge["source"])
        if parent_df is None:
            continue

        sig = node_signature(nid, node_map, edges)
        cached = cached_or_none(spark, sig)
        if cached is not None:
            materialized[nid] = cached
            cache_hits.append(nid)
            continue

        result_df = apply_node_transform(
            spark, parent_df, node, node_map,
            resolve_right_df, resolve_right_size,
        )

        # Shared node (fan-out > 1) -> materialize ke Parquet sekali,
        # semua branch downstream reuse hasil ini.
        if fanout.get(nid, 0) > 1:
            result_df = materialize(result_df, sig)
            cache_writes.append(nid)

        materialized[nid] = result_df

    # ── Ambil DataFrame yang diminta ──────────────────────────────────────
    if target_node_id not in materialized:
        parent_edge = next((e for e in edges if e["target"] == target_node_id), None)
        if parent_edge and parent_edge["source"] in materialized:
            target_df = materialized[parent_edge["source"]]
        else:
            return {"columns": [], "rows": [], "info": "Node belum terhubung ke data"}
    else:
        target_df = materialized[target_node_id]

    preview_pd = target_df.limit(limit).toPandas()
    preview_pd = preview_pd.where(pd.notnull(preview_pd), None)

    return {
        "columns": list(target_df.columns),
        "rows":    preview_pd.to_dict(orient="records"),
        "stats": {
            "engine":              "spark",
            "partitions":          target_df.rdd.getNumPartitions(),
            "shared_nodes_cached": cache_writes,
            "cache_hits":          cache_hits,
            "spark_config":        compute_spark_config(size_mb),
        },
    }