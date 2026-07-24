# Auto-generated TASK file
# Task ID   : task_0904
# DAG ID    : ala1
# Output    : warehouse.plsslah
# Generated : 2026-07-23T11:41:21.524106
# ─────────────────────────────────────────────────────────────────────────────
# This file contains ALL transform logic for this task.
# It is imported by the workflow DAG file (dag_ala1.py).
# ─────────────────────────────────────────────────────────────────────────────

import os, re, time, json, requests, traceback
import psycopg2, psycopg2.extras
import pandas as pd

TASK_ID     = 'task_0904'
DAG_ID      = 'ala1'
INPUT_TABLE = 'staging.market_stores_500'
OUTPUT_NAME = 'plsslah'
TRANSFORMS  = json.loads('[{"type": "join_data", "config": {"joinType": "INNER JOIN", "leftCol": "store_id", "rightCol": "store_id", "rightNodeId": "n1784806831802_4_y3jk", "rightTable": "staging.market_sales_500"}}]')
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

def _cond_sql_fragment(col, condition, value, dialect='duckdb'):
    col_ref = f'"{col}"'
    if condition == 'IS NULL':
        return f"{col_ref} IS NULL"
    if condition == 'IS NOT NULL':
        return f"{col_ref} IS NOT NULL"
    if condition in ('IN', 'NOT IN'):
        vals = [v.strip() for v in str(value).split(',') if v.strip() != '']
        if not vals:
            return None
        vals_sql = ', '.join(repr(v) for v in vals)
        op = 'IN' if condition == 'IN' else 'NOT IN'
        return f"{col_ref} {op} ({vals_sql})"
    if condition == 'LIKE':
        return f"{col_ref} LIKE {repr(value)}"
    if condition in ('>', '>=', '<', '<='):
        try:
            num = float(value)
        except (TypeError, ValueError):
            return None
        if dialect == 'postgres':
            numeric_expr = _pg_safe_cast_expr(col_ref, 'NUMERIC')
        else:
            numeric_expr = f"TRY_CAST({col_ref} AS DOUBLE PRECISION)"
        return f"{numeric_expr} {condition} {num}"
    op = condition if condition in ('=', '!=') else '='
    return f"{col_ref} {op} {repr(value)}"


def _sql_when_fragment(col, condition, value, result, conditions=None, logic='AND', dialect='duckdb'):
    result_lit = repr(result)
    if conditions:
        parts = []
        for c in conditions:
            frag = _cond_sql_fragment(c.get('col', col), c.get('condition', '='), c.get('value', ''), dialect=dialect)
            if frag:
                parts.append(f"({frag})")
        if not parts:
            return None
        joiner = ' OR ' if str(logic).upper() == 'OR' else ' AND '
        return f"WHEN {joiner.join(parts)} THEN {result_lit}"
    frag = _cond_sql_fragment(col, condition, value, dialect=dialect)
    if frag is None:
        return None
    return f"WHEN {frag} THEN {result_lit}"


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
                fragments = []
                for w in whens:
                    condition  = w.get('condition', '=')
                    value      = w.get('value', '')
                    result     = w.get('result', '')
                    conditions = w.get('conditions')
                    logic      = w.get('logic', 'AND')
                    if not conditions and condition not in ('IS NULL', 'IS NOT NULL') and value == '':
                        continue
                    frag = _sql_when_fragment(src, condition, value, result, conditions=conditions, logic=logic)
                    if frag:
                        fragments.append(frag)
                if not fragments:
                    step -= 1; continue
                wc = ' '.join(fragments)
                cte_parts.append(f'{alias} AS (SELECT *, CASE {wc} ELSE {repr(else_v)} END AS "{new_col}" FROM {cur_alias})')
                if cur_cols: cur_cols = cur_cols + [new_col]
            elif ntype == 'fill_null':
                fill_cols = config.get('columns',[])
                fill_type = config.get('fillType','value')
                fill_val  = config.get('fillValue','')
                if not fill_cols:
                    step -= 1; continue
                elif fill_type == 'value':
                    rp = ', '.join(f'COALESCE("{c}", {repr(str(fill_val))}) AS "{c}"' for c in fill_cols)
                    cte_parts.append(f'{alias} AS (SELECT * REPLACE ({rp}) FROM {cur_alias})')
                elif fill_type == 'mean':
                    rp = ', '.join(f'COALESCE("{c}", (SELECT AVG("{c}") FROM {cur_alias})) AS "{c}"' for c in fill_cols)
                    cte_parts.append(f'{alias} AS (SELECT * REPLACE ({rp}) FROM {cur_alias})')
                elif fill_type == 'median':
                    rp = ', '.join(f'COALESCE("{c}", (SELECT MEDIAN("{c}") FROM {cur_alias})) AS "{c}"' for c in fill_cols)
                    cte_parts.append(f'{alias} AS (SELECT * REPLACE ({rp}) FROM {cur_alias})')
                elif fill_type == 'mode':
                    rp = ', '.join(f'COALESCE("{c}", (SELECT MODE("{c}") FROM {cur_alias})) AS "{c}"' for c in fill_cols)
                    cte_parts.append(f'{alias} AS (SELECT * REPLACE ({rp}) FROM {cur_alias})')
                elif fill_type in ('forward', 'backward'):
                    rn_alias = f'{alias}_rn'
                    cte_parts.append(f'{rn_alias} AS (SELECT *, ROW_NUMBER() OVER () AS _rn FROM {cur_alias})')
                    if fill_type == 'forward':
                        fe = ', '.join(
                            f'COALESCE("{c}", LAST_VALUE("{c}" IGNORE NULLS) OVER '
                            f'(ORDER BY _rn ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS "{c}"'
                            for c in fill_cols
                        )
                    else:
                        fe = ', '.join(
                            f'COALESCE("{c}", FIRST_VALUE("{c}" IGNORE NULLS) OVER '
                            f'(ORDER BY _rn ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)) AS "{c}"'
                            for c in fill_cols
                        )
                    excl = ', '.join([f'"{c}"' for c in fill_cols] + ['_rn'])
                    cte_parts.append(f'{alias} AS (SELECT * EXCLUDE ({excl}), {fe} FROM {rn_alias})')
                else:
                    step -= 1; continue
            elif ntype == 'change_type':
                types = config.get('types',{})
                if not types: step -= 1; continue
                rp = ', '.join(f'TRY_CAST("{c}" AS {DTYPE_MAP.get(t,"VARCHAR")}) AS "{c}"' for c,t in types.items())
                cte_parts.append(f'{alias} AS (SELECT * REPLACE ({rp}) FROM {cur_alias})')
            elif ntype == 'order_table':
                orders = config.get('orders',[])
                if not orders: step -= 1; continue
                oc = ', '.join(f'"{o["col"]}" {o.get("dir","ASC")}' for o in orders if o.get('col'))
                cte_parts.append(f'{alias} AS (SELECT * FROM {cur_alias} ORDER BY {oc})')
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
                if op == '/':
                    oe = (f'CASE WHEN TRY_CAST("{col_b}" AS DOUBLE) != 0 '
                          f'THEN TRY_CAST("{col_a}" AS DOUBLE) / TRY_CAST("{col_b}" AS DOUBLE) '
                          f'ELSE NULL END')
                else:
                    oe = f'TRY_CAST("{col_a}" AS DOUBLE) {op} TRY_CAST("{col_b}" AS DOUBLE)'
                cte_parts.append(f'{alias} AS (SELECT *, ({oe}) AS "{new_col}" FROM {cur_alias})')
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
        try:
            result_df = con.execute(sql).df()
        except Exception as e:
            raise RuntimeError(f"Transform SQL failed (DuckDB engine): {e}") from e
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

def _pg_cast_type():
    return {
        "TEXT": "TEXT", "INTEGER": "INTEGER", "BIGINT": "BIGINT",
        "NUMERIC": "NUMERIC", "BOOLEAN": "BOOLEAN",
        "DATE": "DATE", "TIMESTAMP": "TIMESTAMP", "VARCHAR(255)": "VARCHAR(255)",
    }


# PostgreSQL has no built-in TRY_CAST. These small PL/pgSQL helpers give the
# same "safe cast -> NULL on failure" behaviour DuckDB gets for free via
# TRY_CAST, so Change Column Data Type (and numeric comparisons in Value
# Mapper) behave identically on both engines instead of erroring the batch.
_PG_SAFE_CAST_FUNCTIONS_SQL = '''
CREATE OR REPLACE FUNCTION meta._safe_cast_double(v TEXT) RETURNS DOUBLE PRECISION AS $$
BEGIN RETURN v::DOUBLE PRECISION; EXCEPTION WHEN OTHERS THEN RETURN NULL; END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION meta._safe_cast_bigint(v TEXT) RETURNS BIGINT AS $$
BEGIN RETURN ROUND(v::DOUBLE PRECISION)::BIGINT; EXCEPTION WHEN OTHERS THEN RETURN NULL; END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION meta._safe_cast_integer(v TEXT) RETURNS INTEGER AS $$
BEGIN RETURN ROUND(v::DOUBLE PRECISION)::INTEGER; EXCEPTION WHEN OTHERS THEN RETURN NULL; END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION meta._safe_cast_numeric(v TEXT) RETURNS NUMERIC AS $$
BEGIN RETURN v::NUMERIC; EXCEPTION WHEN OTHERS THEN RETURN NULL; END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION meta._safe_cast_boolean(v TEXT) RETURNS BOOLEAN AS $$
BEGIN RETURN v::BOOLEAN; EXCEPTION WHEN OTHERS THEN RETURN NULL; END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION meta._safe_cast_date(v TEXT) RETURNS DATE AS $$
BEGIN RETURN v::DATE; EXCEPTION WHEN OTHERS THEN RETURN NULL; END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION meta._safe_cast_timestamp(v TEXT) RETURNS TIMESTAMP AS $$
BEGIN RETURN v::TIMESTAMP; EXCEPTION WHEN OTHERS THEN RETURN NULL; END;
$$ LANGUAGE plpgsql IMMUTABLE;
'''


def _pg_safe_cast_expr(col_ref, dtype):
    """Return a Postgres SQL expression that safely casts col_ref to dtype,
    yielding NULL instead of raising when the value can't be converted."""
    dtype = (dtype or "TEXT").upper()
    if dtype == "TEXT":
        return f"{col_ref}::TEXT"
    if dtype == "VARCHAR(255)":
        return f"LEFT({col_ref}::TEXT, 255)"
    if dtype == "INTEGER":
        return f"meta._safe_cast_integer({col_ref}::TEXT)"
    if dtype == "BIGINT":
        return f"meta._safe_cast_bigint({col_ref}::TEXT)"
    if dtype == "NUMERIC":
        return f"meta._safe_cast_numeric({col_ref}::TEXT)"
    if dtype == "BOOLEAN":
        return f"meta._safe_cast_boolean({col_ref}::TEXT)"
    if dtype == "DATE":
        return f"meta._safe_cast_date({col_ref}::TEXT)"
    if dtype == "TIMESTAMP":
        return f"meta._safe_cast_timestamp({col_ref}::TEXT)"
    return f"{col_ref}::TEXT"


def _run_postgres(pg_hook, input_table, output_name, transforms, task_id):
    """PostgreSQL native runner untuk dataset kecil (<50MB).
    Setiap transform ditulis sebagai CREATE TABLE AS (immutable step)."""
    safe_out  = re.sub(r'[^a-z0-9_]','_',output_name.lower()).strip('_') or "output"
    out_table = f"warehouse.{safe_out}"
    schema, tname = input_table.split(".",1) if "." in input_table else ("staging", input_table)
    PG_TYPE = _pg_cast_type()

    pg_hook.run("CREATE SCHEMA IF NOT EXISTS meta")
    pg_hook.run(_PG_SAFE_CAST_FUNCTIONS_SQL)

    cols = [r[0] for r in pg_hook.get_records(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema='{schema}' AND table_name='{tname.strip(chr(34))}'
        AND column_name NOT IN ('loaded_at','date_partition')
        ORDER BY ordinal_position
    """)]

    pg_hook.run("CREATE SCHEMA IF NOT EXISTS warehouse")

    cur_from = input_table
    step     = 0
    for tx in transforms:
        ntype  = tx.get("type","")
        config = tx.get("config") or {}
        step  += 1
        tmp    = f"staging._etl_{task_id}_s{step}"
        try:
            if ntype == "filter_rows":
                pg_hook.run(f"CREATE TABLE {tmp} AS SELECT * FROM {cur_from} WHERE {config.get('formula','1=1')}")

            elif ntype == "select_col":
                sc = [c for c in config.get("columns",[]) if c in cols]
                if sc:
                    pg_hook.run(f"CREATE TABLE {tmp} AS SELECT {_q(sc)} FROM {cur_from}")
                    cols = sc
                else:
                    tmp=cur_from; step-=1; continue

            elif ntype == "drop_col":
                kc = [c for c in cols if c not in set(config.get("columns",[]))]
                pg_hook.run(f"CREATE TABLE {tmp} AS SELECT {_q(kc)} FROM {cur_from}")
                cols = kc

            elif ntype == "rename_col":
                rn = config.get("renames",{})
                if rn:
                    ex = ", ".join(f'"{c}" AS "{rn.get(c,c)}"' for c in cols)
                    pg_hook.run(f"CREATE TABLE {tmp} AS SELECT {ex} FROM {cur_from}")
                    cols = [rn.get(c,c) for c in cols]
                else:
                    tmp=cur_from; step-=1; continue

            elif ntype == "add_const":
                name  = config.get("name","new_col")
                val   = config.get("value","")
                dtype = PG_TYPE.get(config.get("dtype","TEXT"), "TEXT")
                if name:
                    pg_hook.run(
                        f'CREATE TABLE {tmp} AS SELECT {_q(cols)}, CAST({repr(val)} AS {dtype}) AS "{name}" FROM {cur_from}'
                    )
                    if name not in cols:
                        cols = cols + [name]
                else:
                    tmp=cur_from; step-=1; continue

            elif ntype == "set_val":
                target = config.get("targetCol","")
                if target and target in cols:
                    if config.get("useExpr"):
                        expr = config.get("expr", f'"{target}"')
                    else:
                        src  = config.get("sourceCol", target)
                        expr = f'"{src}"' if src in cols else f'"{target}"'
                    sel = ", ".join(
                        f'({expr}) AS "{c}"' if c == target else f'"{c}"'
                        for c in cols
                    )
                    pg_hook.run(f"CREATE TABLE {tmp} AS SELECT {sel} FROM {cur_from}")
                else:
                    tmp=cur_from; step-=1; continue

            elif ntype == "val_mapper":
                src, new_col = config.get("sourceCol",""), config.get("newColName","mapped")
                whens, else_v = config.get("whens",[]), config.get("elseValue","")
                if src in cols and whens:
                    fragments = []
                    for w in whens:
                        condition  = w.get("condition","=")
                        value      = w.get("value","")
                        result     = w.get("result","")
                        conditions = w.get("conditions")
                        logic      = w.get("logic","AND")
                        if not conditions and condition not in ("IS NULL","IS NOT NULL") and value == "":
                            continue
                        frag = _sql_when_fragment(src, condition, value, result, conditions=conditions, logic=logic)
                        if frag:
                            fragments.append(frag)
                    if fragments:
                        case_expr = f'CASE {" ".join(fragments)} ELSE {repr(else_v)} END AS "{new_col}"'
                        pg_hook.run(f"CREATE TABLE {tmp} AS SELECT {_q(cols)}, {case_expr} FROM {cur_from}")
                        if new_col not in cols:
                            cols = cols + [new_col]
                    else:
                        tmp=cur_from; step-=1; continue
                else:
                    tmp=cur_from; step-=1; continue

            elif ntype == "fill_null":
                fc = [c for c in config.get("columns",[]) if c in cols]
                ft = config.get("fillType","value")
                fv = config.get("fillValue","")
                if not fc:
                    tmp=cur_from; step-=1; continue
                elif ft == "value":
                    sel = ", ".join(
                        f'COALESCE("{c}"::TEXT,{repr(str(fv))})::TEXT AS "{c}"' if c in fc else f'"{c}"'
                        for c in cols
                    )
                    pg_hook.run(f"CREATE TABLE {tmp} AS SELECT {sel} FROM {cur_from}")
                elif ft == "mean":
                    sel = ", ".join(
                        f'COALESCE("{c}", (SELECT AVG("{c}") FROM {cur_from})) AS "{c}"' if c in fc else f'"{c}"'
                        for c in cols
                    )
                    pg_hook.run(f"CREATE TABLE {tmp} AS SELECT {sel} FROM {cur_from}")
                elif ft == "median":
                    sel = ", ".join(
                        f'COALESCE("{c}", (SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY "{c}") FROM {cur_from})) AS "{c}"'
                        if c in fc else f'"{c}"'
                        for c in cols
                    )
                    pg_hook.run(f"CREATE TABLE {tmp} AS SELECT {sel} FROM {cur_from}")
                elif ft == "mode":
                    sel = ", ".join(
                        f'COALESCE("{c}", (SELECT "{c}" FROM {cur_from} WHERE "{c}" IS NOT NULL '
                        f'GROUP BY "{c}" ORDER BY COUNT(*) DESC LIMIT 1)) AS "{c}"'
                        if c in fc else f'"{c}"'
                        for c in cols
                    )
                    pg_hook.run(f"CREATE TABLE {tmp} AS SELECT {sel} FROM {cur_from}")
                elif ft in ("forward","backward"):
                    # Catatan: correlated subquery -> aman untuk data <50MB (target
                    # engine ini), tapi tidak scalable untuk data lebih besar.
                    cmp_op = "<" if ft == "forward" else ">"
                    order_dir = "DESC" if ft == "forward" else "ASC"
                    sel_parts = []
                    for c in cols:
                        if c in fc:
                            sel_parts.append(
                                f'COALESCE(t1."{c}", (SELECT t2."{c}" FROM {cur_from} t2 '
                                f'WHERE t2.ctid {cmp_op} t1.ctid AND t2."{c}" IS NOT NULL '
                                f'ORDER BY t2.ctid {order_dir} LIMIT 1)) AS "{c}"'
                            )
                        else:
                            sel_parts.append(f't1."{c}"')
                    pg_hook.run(f"CREATE TABLE {tmp} AS SELECT {', '.join(sel_parts)} FROM {cur_from} t1")
                else:
                    tmp=cur_from; step-=1; continue

            elif ntype == "change_type":
                types = config.get("types",{})
                if types:
                    sel = ", ".join(
                        f'CAST("{c}" AS {PG_TYPE.get(types[c],"TEXT")}) AS "{c}"' if c in types else f'"{c}"'
                        for c in cols
                    )
                    pg_hook.run(f"CREATE TABLE {tmp} AS SELECT {sel} FROM {cur_from}")
                else:
                    tmp=cur_from; step-=1; continue

            elif ntype == "order_table":
                orders = [o for o in config.get("orders",[]) if o.get("col") in cols]
                if orders:
                    oc = ", ".join(f'"{o["col"]}" {o.get("dir","ASC")}' for o in orders)
                    pg_hook.run(f"CREATE TABLE {tmp} AS SELECT {_q(cols)} FROM {cur_from} ORDER BY {oc}")
                else:
                    tmp=cur_from; step-=1; continue

            elif ntype == "group_agg":
                gc=config.get("groupCols",[]); ac=config.get("aggCols",[])
                if gc and ac:
                    ae=", ".join(f'{a["func"]}("{a["col"]}") AS "{a["alias"]}"' for a in ac)
                    pg_hook.run(f"CREATE TABLE {tmp} AS SELECT {_q(gc)}, {ae} FROM {cur_from} GROUP BY {_q(gc)}")
                    cols=gc+[a["alias"] for a in ac]
                else:
                    tmp=cur_from; step-=1; continue

            elif ntype == "join_data":
                right_table = config.get("rightTable","")
                left_col    = config.get("leftCol","")
                right_col   = config.get("rightCol","")
                if not (right_table and left_col):
                    tmp=cur_from; step-=1; continue
                else:
                    raw_type = config.get("joinType","INNER JOIN").upper()
                    is_cross = "CROSS" in raw_type
                    sql_join = "CROSS JOIN" if is_cross else raw_type

                    schema_r, tname_r = right_table.split(".",1) if "." in right_table else ("staging", right_table)
                    r_cols = [r[0] for r in pg_hook.get_records(f"""
                        SELECT column_name FROM information_schema.columns
                        WHERE table_schema='{schema_r}' AND table_name='{tname_r.strip(chr(34))}'
                        ORDER BY ordinal_position
                    """)]

                    dup = [c for c in r_cols if c in cols and c != right_col]
                    left_sel  = ", ".join(f'l."{c}"' for c in cols)
                    right_sel = ", ".join(
                        f'r."{c}" AS "{c}_right"' if c in dup else f'r."{c}"'
                        for c in r_cols
                    )

                    if is_cross:
                        pg_hook.run(f"CREATE TABLE {tmp} AS SELECT {left_sel}, {right_sel} FROM {cur_from} l CROSS JOIN {right_table} r")
                        cols = cols + [f"{c}_right" if c in dup else c for c in r_cols]
                    elif right_col:
                        pg_hook.run(
                            f"CREATE TABLE {tmp} AS SELECT {left_sel}, {right_sel} "
                            f'FROM {cur_from} l {sql_join} {right_table} r ON l."{left_col}" = r."{right_col}"'
                        )
                        cols = cols + [f"{c}_right" if c in dup else c for c in r_cols]
                    else:
                        tmp=cur_from; step-=1; continue

            elif ntype == "calc":
                new_col = (config.get("newColName") or "result").strip()
                col_a, col_b, op = config.get("colA",""), config.get("colB",""), config.get("operation","+")
                if new_col and col_a in cols and col_b in cols:
                    if op == "/":
                        expr = (f'CASE WHEN CAST("{col_b}" AS DOUBLE PRECISION) != 0 '
                                f'THEN CAST("{col_a}" AS DOUBLE PRECISION) / CAST("{col_b}" AS DOUBLE PRECISION) '
                                f'ELSE NULL END')
                    else:
                        expr = f'(CAST("{col_a}" AS DOUBLE PRECISION) {op} CAST("{col_b}" AS DOUBLE PRECISION))'
                    pg_hook.run(f'CREATE TABLE {tmp} AS SELECT {_q(cols)}, {expr} AS "{new_col}" FROM {cur_from}')
                    if new_col not in cols:
                        cols = cols + [new_col]
                else:
                    tmp=cur_from; step-=1; continue

            elif ntype == "adv_calculator":
                SCI = {"sin":"SIN","cos":"COS","sqrt":"SQRT","radians":"RADIANS","atan2":"ATAN2","power":"POWER"}
                exprs, new_cols = [], []
                for calc in config.get("calculations",[]):
                    fn    = SCI.get(calc.get("operation","sin"),"SIN")
                    col_a = calc.get("colA",""); col_b = calc.get("colB","")
                    new_c = (calc.get("newColName") or "").strip()
                    if not new_c or col_a not in cols:
                        continue
                    if fn in ("ATAN2","POWER") and col_b in cols:
                        exprs.append(f'{fn}(CAST("{col_a}" AS DOUBLE PRECISION), CAST("{col_b}" AS DOUBLE PRECISION)) AS "{new_c}"')
                    else:
                        exprs.append(f'{fn}(CAST("{col_a}" AS DOUBLE PRECISION)) AS "{new_c}"')
                    new_cols.append(new_c)
                if exprs:
                    pg_hook.run(f"CREATE TABLE {tmp} AS SELECT {_q(cols)}, {', '.join(exprs)} FROM {cur_from}")
                    cols = cols + new_cols
                else:
                    tmp=cur_from; step-=1; continue

            elif ntype == "combine_cols":
                new_col = (config.get("newColName") or "combined").strip()
                sep     = config.get("separator"," ")
                selected = [c for c in config.get("selectedCols",[]) if c in cols]
                remove_orig = config.get("removeOriginal", False)
                if new_col and selected:
                    concat_expr = f' || {repr(sep)} || '.join(
                        f'COALESCE(CAST("{c}" AS TEXT), \'\')' for c in selected
                    )
                    keep = [c for c in cols if not (remove_orig and c in selected)]
                    pg_hook.run(f'CREATE TABLE {tmp} AS SELECT {_q(keep)}, ({concat_expr}) AS "{new_col}" FROM {cur_from}')
                    cols = keep + [new_col]
                else:
                    tmp=cur_from; step-=1; continue

            else:
                tmp=cur_from; step-=1; continue

        except Exception as e:
            print(f"[PG] step {step} {ntype}: {e}")
            raise RuntimeError(f"Transform step {step} ('{ntype}') failed (PostgreSQL engine): {e}") from e

        if tmp != cur_from:
            cur_from = tmp

    try:
        pg_hook.run(f"DROP TABLE IF EXISTS {out_table}")
        pg_hook.run(f"CREATE TABLE {out_table} AS SELECT {_q(cols)}, NOW() AS loaded_at FROM {cur_from}")
    except Exception as e:
        raise RuntimeError(f"Failed to materialize output table {out_table} (PostgreSQL engine): {e}") from e

    for r in pg_hook.get_records(f"""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema='staging' AND table_name LIKE '_etl_{task_id}_s%'
    """):
        pg_hook.run(f'DROP TABLE IF EXISTS staging."{r[0]}"')

# ── Spark Runner ──────────────────────────────────────────────────────────────

def _run_spark(input_table, output_name, transforms, row_count):
    from pyspark.sql import SparkSession, functions as F, Window

    safe_out = re.sub(r'[^a-z0-9_]','_',output_name.lower()).strip('_') or "output"
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
                        condition  = w.get("condition", "=")
                        value      = w.get("value", "")
                        result     = w.get("result", "")
                        conditions = w.get("conditions")
                        logic      = w.get("logic", "AND")
                        if conditions:
                            cond_expr = _spark_combined_condition(F, src, conditions, logic)
                            if cond_expr is None:
                                continue
                        else:
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

                    dup_cols = [c for c in right_df.columns if c in df.columns and c != right_col]
                    for c in dup_cols:
                        right_df = right_df.withColumnRenamed(c, f"{c}_right")

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
                        df = df.join(right_df, df[left_col] == right_df[right_col], join_type)

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
                    try:
                        exec(code, ns)
                    except Exception as e:
                        raise RuntimeError(f"PySpark node code failed: {e}") from e
                    df = ns.get("df", df)

        except Exception as e:
            print(f"[Spark] {ntype} error: {e}")
            raise RuntimeError(f"Transform node '{ntype}' failed (Spark engine): {e}") from e

    df.write.jdbc(
        url="jdbc:postgresql://postgres:5432/airflow",
        table=f"warehouse.{safe_out}",
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


def _spark_combined_condition(F, default_col, conditions, logic="AND"):
    """Combine a list of {col?, condition, value} rules into one boolean Spark
    column expression using AND/OR, mirroring _sql_when_fragment's semantics."""
    expr = None
    for c in conditions or []:
        try:
            frag = _spark_when_condition(F, c.get("col", default_col), c.get("condition", "="), c.get("value", ""))
        except Exception:
            continue
        if frag is None:
            continue
        expr = frag if expr is None else (
            (expr | frag) if str(logic).upper() == "OR" else (expr & frag)
        )
    return expr


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
