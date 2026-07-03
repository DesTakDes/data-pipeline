# Auto-generated TASK file
# Task ID   : task_8969
# DAG ID    : cobaterus
# Output    : warehouse.ayo_1
# Generated : 2026-06-29T05:53:27.254593
# ─────────────────────────────────────────────────────────────────────────────
# This file contains ALL transform logic for this task.
# It is imported by the workflow DAG file (dag_cobaterus.py).
# ─────────────────────────────────────────────────────────────────────────────

import os, re, time, json, requests, traceback
import psycopg2, psycopg2.extras
import pandas as pd

TASK_ID     = 'task_8969'
DAG_ID      = 'cobaterus'
INPUT_TABLE = 'staging.customers_10000'
OUTPUT_NAME = 'ayo_1'
TRANSFORMS  = json.loads(r"""[{"type": "drop_col", "config": {"columns": ["index", "customer_id", "last_name", "phone_1", "phone_2", "email", "website"]}}]""")
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
    cols_quoted  = [f'"{c}"' for c in columns]
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
                    excl = ', '.join(f'"{c}"' for c in drop)
                    cte_parts.append(f'{alias} AS (SELECT * EXCLUDE ({excl}) FROM {cur_alias})')
            elif ntype == 'rename_col':
                renames = config.get('renames',{})
                if not renames: step -= 1; continue
                if cur_cols:
                    exprs = [f'"{c}" AS "{renames.get(c,c)}"' for c in cur_cols]
                    cte_parts.append(f"{alias} AS (SELECT {', '.join(exprs)} FROM {cur_alias})")
                    cur_cols = [renames.get(c,c) for c in cur_cols]
                else:
                    rs = ', '.join(f'"{o}" AS "{n}"' for o,n in renames.items())
                    cte_parts.append(f'{alias} AS (SELECT * RENAME ({rs}) FROM {cur_alias})')
            elif ntype == 'add_const':
                name  = config.get('name','new_col')
                val   = config.get('value','')
                dtype = DTYPE_MAP.get(config.get('dtype','TEXT'),'VARCHAR')
                cte_parts.append(f'{alias} AS (SELECT *, CAST({repr(val)} AS {dtype}) AS "{name}" FROM {cur_alias})')
                if cur_cols: cur_cols = cur_cols + [name]
            elif ntype == 'set_val':
                target = config.get('targetCol','')
                if not target: step -= 1; continue
                if config.get('useExpr'):
                    expr = config.get('expr', f'"{target}"')
                else:
                    src  = config.get('sourceCol', target)
                    expr = f'"{src}"'
                cte_parts.append(f'{alias} AS (SELECT * REPLACE ({expr} AS "{target}") FROM {cur_alias})')
            elif ntype == 'val_mapper':
                src     = config.get('sourceCol','')
                new_col = config.get('newColName','mapped')
                whens   = config.get('whens',[])
                else_v  = config.get('elseValue','')
                if not src or not whens: step -= 1; continue
                wc = ' '.join(
                    f'WHEN "{src}" {w["condition"]} {repr(w["value"])} THEN {repr(w["result"])}'
                    for w in whens if w.get('value') and w.get('result')
                )
                cte_parts.append(f'{alias} AS (SELECT *, CASE {wc} ELSE {repr(else_v)} END AS "{new_col}" FROM {cur_alias})')
                if cur_cols: cur_cols = cur_cols + [new_col]
            elif ntype == 'fill_null':
                fill_cols = config.get('columns',[])
                fill_val  = config.get('fillValue','')
                if fill_cols and config.get('fillType','value') == 'value':
                    rp = ', '.join(f'COALESCE("{c}", {repr(str(fill_val))}) AS "{c}"' for c in fill_cols)
                    cte_parts.append(f'{alias} AS (SELECT * REPLACE ({rp}) FROM {cur_alias})')
                else: step -= 1; continue
            elif ntype == 'change_type':
                types = config.get('types',{})
                if not types: step -= 1; continue
                rp = ', '.join(f'TRY_CAST("{c}" AS {DTYPE_MAP.get(t,"VARCHAR")}) AS "{c}"' for c,t in types.items())
                cte_parts.append(f'{alias} AS (SELECT * REPLACE ({rp}) FROM {cur_alias})')
            elif ntype == 'order_table':
                orders = config.get('orders',[])
                if not orders: step -= 1; continue
                oc = ', '.join(f'"{o["col"]}" {o.get("dir","ASC")}' for o in orders if o.get('col'))
                cte_parts.append(f'"{oc}"' if oc else "1")
            elif ntype == 'group_agg':
                gcols = config.get('groupCols',[])
                acols = config.get('aggCols',[])
                if not gcols or not acols: step -= 1; continue
                agg_exprs = []
                for a in acols:
                    fn  = a.get('func','COUNT')
                    col = a.get('col','')
                    aln = a.get('alias', f'{col}_{fn.lower()}')
                    if fn == 'COUNT DISTINCT':
                        agg_exprs.append(f'COUNT(DISTINCT "{col}") AS "{aln}"')
                    else:
                        agg_exprs.append(f'{fn}("{col}") AS "{aln}"')
                cte_parts.append(f"{alias} AS (SELECT {_q(gcols)}, {', '.join(agg_exprs)} FROM {cur_alias} GROUP BY {_q(gcols)})")
                cur_cols = gcols + [a.get('alias','') for a in acols]
            elif ntype == 'calc':
                new_col = (config.get('newColName') or 'result').strip()
                col_a   = config.get('colA','')
                col_b   = config.get('colB','')
                op      = config.get('operation','+')
                if not (new_col and col_a and col_b): step -= 1; continue
                cte_parts.append(f'{alias} AS (SELECT *, (TRY_CAST("{col_a}" AS DOUBLE) {op} TRY_CAST("{col_b}" AS DOUBLE)) AS "{new_col}" FROM {cur_alias})')
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
                        exprs.append(f'{fn}(TRY_CAST("{col_a}" AS DOUBLE), TRY_CAST("{col_b}" AS DOUBLE)) AS "{new_c}"')
                    else:
                        exprs.append(f'{fn}(TRY_CAST("{col_a}" AS DOUBLE)) AS "{new_c}"')
                if exprs:
                    cte_parts.append(f"{alias} AS (SELECT *, {', '.join(exprs)} FROM {cur_alias})")
                else: step -= 1; continue
            elif ntype == 'combine_cols':
                new_col     = (config.get('newColName') or 'combined').strip()
                sep         = config.get('separator',' ')
                selected    = config.get('selectedCols',[])
                remove_orig = config.get('removeOriginal',False)
                if not new_col or not selected: step -= 1; continue
                cp = f' || {repr(sep)} || '.join(f'COALESCE(CAST("{c}" AS VARCHAR), \'\')' for c in selected)
                if remove_orig:
                    excl = ', '.join(f'"{c}"' for c in selected)
                    cte_parts.append(f'{alias} AS (SELECT * EXCLUDE ({excl}), ({cp}) AS "{new_col}" FROM {cur_alias})')
                else:
                    cte_parts.append(f'{alias} AS (SELECT *, ({cp}) AS "{new_col}" FROM {cur_alias})')
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

        sql       = _build_transform_sql("_input", transforms)
        t_tx      = time.time()
        result_df = con.execute(sql).df()
        actual    = len(result_df)
        upd(75, f"Transforms done: {actual:,} rows ({time.time()-t_tx:.1f}s)")

        upd(78, "Saving rows as Parquet file (snappy)…")
        pq_path = save_parquet_snappy(result_df, output_name)
        upd(82, f"Parquet: {pq_path}")

        safe_out  = re.sub(r'[^a-z0-9_]','_',output_name.lower()).strip('_') or "output"
        out_table = f'warehouse."{safe_out}"'
        pg.rollback()
        wcur = pg.cursor()
        wcur.execute("CREATE SCHEMA IF NOT EXISTS warehouse")
        wcur.execute(f"DROP TABLE IF EXISTS {out_table}")
        PG_TYPE = {"int64":"BIGINT","int32":"INTEGER","float64":"NUMERIC","float32":"NUMERIC",
                    "bool":"BOOLEAN","object":"TEXT","datetime64[ns]":"TIMESTAMP"}
        col_defs = ", ".join(f'"{c}" {PG_TYPE.get(str(result_df[c].dtype),"TEXT")}' for c in result_df.columns)
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
    safe_out   = re.sub(r'[^a-z0-9_]','_',output_name.lower()).strip('_') or "output"
    out_table  = f"warehouse.{safe_out}"
    schema, tname = input_table.split(".",1) if "." in input_table else ("staging", input_table)

    cols = [r[0] for r in pg_hook.get_records(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema='{schema}' AND table_name='{tname}'
        AND column_name NOT IN ('loaded_at','date_partition')
        ORDER BY ordinal_position
    """)]

    pg_hook.run("CREATE SCHEMA IF NOT EXISTS warehouse")
    pg_hook.run(f"DROP TABLE IF EXISTS {out_table}")
    pg_hook.run(f"""
        CREATE TABLE {out_table} AS
        WITH _base AS (SELECT {", ".join(f'"{c}"' for c in cols)} FROM {input_table})
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
                pg_hook.run(f"CREATE TABLE {tmp} AS SELECT * FROM {cur_from} WHERE {config.get('formula','1=1')}")
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
                ex = ", ".join(f'"{c}" AS "{rn.get(c,c)}"' for c in cols)
                pg_hook.run(f"CREATE TABLE {tmp} AS SELECT {ex} FROM {cur_from}")
                cols = [rn.get(c,c) for c in cols]
            elif ntype == "group_agg":
                gc=config.get("groupCols",[]); ac=config.get("aggCols",[])
                if gc and ac:
                    ae=", ".join(f'{a["func"]}("{a["col"]}") AS "{a["alias"]}"' for a in ac)
                    pg_hook.run(f"CREATE TABLE {tmp} AS SELECT {_q(gc)}, {ae} FROM {cur_from} GROUP BY {_q(gc)}")
                    cols=gc+[a["alias"] for a in ac]
                else: tmp=cur_from; step-=1; continue
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
        WHERE table_schema='staging' AND table_name LIKE '_etl_{task_id}_s%'
    """):
        pg_hook.run(f'DROP TABLE IF EXISTS staging."{r[0]}"')


# ── Spark Runner ──────────────────────────────────────────────────────────────

def _run_spark(input_table, output_name, transforms, row_count):
    from pyspark.sql import SparkSession, functions as F
    safe_out = re.sub(r'[^a-z0-9_]','_',output_name.lower()).strip('_') or "output"
    optimal_partitions = max(SHUFFLE_PARTITIONS, row_count // 100_000 + 1)

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
        .getOrCreate())

    JDBC = {"url":"jdbc:postgresql://postgres:5432/airflow",
             "properties":{"user":"airflow","password":"airflow","driver":"org.postgresql.Driver"}}
    df = spark.read.jdbc(url=JDBC["url"], table=f"(SELECT * FROM {input_table}) AS t",
                         numPartitions=min(8, optimal_partitions), properties=JDBC["properties"])
    for tx in transforms:
        ntype = tx.get("type",""); cfg = tx.get("config") or {}
        try:
            if   ntype == "filter_rows": df=df.filter(cfg.get("formula","1=1"))
            elif ntype == "select_col":
                c=[x for x in cfg.get("columns",[]) if x in df.columns]
                if c: df=df.select(c)
            elif ntype == "drop_col": df=df.drop(*[c for c in cfg.get("columns",[]) if c in df.columns])
            elif ntype == "rename_col":
                for o,n in cfg.get("renames",{}).items():
                    if o in df.columns: df=df.withColumnRenamed(o,n)
            elif ntype == "add_const": df=df.withColumn(cfg.get("name","c"),F.lit(cfg.get("value","")))
            elif ntype == "group_agg":
                gc=cfg.get("groupCols",[]); ac=cfg.get("aggCols",[])
                if gc and ac:
                    fn_map={"COUNT":F.count,"SUM":F.sum,"AVG":F.avg,"MIN":F.min,"MAX":F.max,"COUNT DISTINCT":F.countDistinct}
                    df=df.groupBy(gc).agg(*[fn_map.get(a["func"],F.count)(a["col"]).alias(a["alias"]) for a in ac])
            elif ntype == "pyspark":
                code=cfg.get("code","")
                if code:
                    ns={"df":df,"spark":spark,"F":F}; exec(code,ns); df=ns["df"]
        except Exception as e:
            print(f"[Spark] {ntype}: {e}")

    df.write.jdbc(url=JDBC["url"], table=f"warehouse.{safe_out}", mode="overwrite", properties=JDBC["properties"])

    if row_count > 10_000:
        os.makedirs(PARQUET_DIR, exist_ok=True)
        parquet_path = f"{PARQUET_DIR}/{safe_out}.parquet"
        output_parts = max(1, row_count // 500_000)
        (df.coalesce(output_parts).write.mode("overwrite").option("compression","snappy").parquet(parquet_path))
        print(f"[Spark] Snappy Parquet saved: {parquet_path}")

    spark.stop()


# ── Main Entry Point (called by the workflow DAG) ─────────────────────────────

def run(run_ids, backend_url=BACKEND_URL):
    """
    Entry point called by the workflow DAG.
    run_ids: list of pipeline run IDs for progress reporting.
    """
    tbl      = INPUT_TABLE if "." in INPUT_TABLE else f"staging.{INPUT_TABLE}"
    safe_out = re.sub(r'[^a-z0-9_]','_',OUTPUT_NAME.lower()).strip('_') or "output"

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
        rows = pg_hook.get_first(f'SELECT COUNT(*) FROM warehouse."{safe_out}"')[0]
    else:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        pg_hook   = PostgresHook(postgres_conn_id="postgres_default")
        row_count = pg_hook.get_first(f"SELECT COUNT(*) FROM {tbl}")[0]
        _run_postgres(pg_hook, tbl, safe_out, TRANSFORMS, TASK_ID)
        try: rows = pg_hook.get_first(f'SELECT COUNT(*) FROM warehouse."{safe_out}"')[0]
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
