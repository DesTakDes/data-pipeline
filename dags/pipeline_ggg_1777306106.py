# Auto-generated DAG: pipeline_ggg_1777306106
# Workflow: ggg
# Generated: 2026-04-27T16:08:26.806380

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import json
import requests

DAG_ID       = 'pipeline_ggg_1777306106'
INPUT_TABLE  = 'staging.customers_(1)'
OUTPUT_NAME  = 'hoii'
WORKFLOW_ID  = 'wf_1777305920484'
TRANSFORMS   = json.loads('[{"type": "select_col", "config": {"columns": ["gender", "customerid", "age", "annual_income_($)", "spending_score_(1_100)", "profession", "work_experience", "family_size"]}}, {"type": "add_const", "config": {"name": "lele", "value": "0000", "dtype": "INTEGER"}}]')
BACKEND_URL  = "http://backend:8000"

default_args = {"owner": "etlflow", "retries": 0}


def get_schema(pg, table_name):
    if "." not in table_name:
        table_name = f"staging.{table_name}"
    schema_name, tbl = table_name.split(".", 1)
    rows = pg.get_records("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = '""" + schema_name + """'
          AND table_name   = '""" + tbl + """'
          AND column_name NOT IN (
              '_id','_date_partition','_processed_at',
              'loaded_at','date_partition'
          )
        ORDER BY ordinal_position
    """)
    schema = {}
    for col, dtype in rows:
        if   "int"     in dtype:                          schema[col] = "BIGINT"
        elif "numeric" in dtype or "float" in dtype:      schema[col] = "NUMERIC"
        elif "timestamp" in dtype:                        schema[col] = "TIMESTAMP"
        elif "date"    in dtype:                          schema[col] = "DATE"
        elif "bool"    in dtype:                          schema[col] = "BOOLEAN"
        else:                                             schema[col] = "TEXT"
    return schema


def q(cols):
    """Quote column list."""
    return ", ".join(f'"{c}"' for c in cols)


def run_pipeline(**context):
    pg     = PostgresHook(postgres_conn_id="postgres_default")
    conf   = context.get("dag_run").conf or {}
    run_id = conf.get("run_id")

    # ── EXTRACT ───────────────────────────────────────────────────
    tbl = INPUT_TABLE if "." in INPUT_TABLE else f"staging.{INPUT_TABLE}"
    sch, tname = tbl.split(".", 1)

    exists = pg.get_first(f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema = '{sch}' AND table_name = '{tname}'
        )
    """)[0]
    if not exists:
        raise ValueError(f"Table {tbl} not found")

    schema    = get_schema(pg, tbl)
    row_count = pg.get_first(f"SELECT COUNT(*) FROM {tbl}")[0]
    print(f"[Extract] {row_count} rows, {len(schema)} cols from {tbl}")

    # ── TRANSFORM ─────────────────────────────────────────────────
    # Bersihkan temp tables lama
    temps = pg.get_records(f"""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'staging'
        AND table_name LIKE '_pipeline_ggg_1777306106_step_%'
    """)
    for (t,) in temps:
        pg.run(f'DROP TABLE IF EXISTS staging."{t}"')

    current = tbl
    step    = 0

    for node in TRANSFORMS:
        ntype  = node.get("type", "")
        config = node.get("config") or {}
        step  += 1
        tmp    = f"staging._{DAG_ID}_step_{step}"

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
                else:
                    tmp = current

            elif ntype == "drop_col":
                keep = [c for c in cur_cols if c not in set(config.get("columns", []))]
                pg.run(f"CREATE TABLE {tmp} AS SELECT {q(keep)} FROM {current}")

            elif ntype == "rename_col":
                renames = config.get("renames", {})
                exprs   = ", ".join(f'"{c}" AS "{renames.get(c, c)}"' for c in cur_cols)
                pg.run(f"CREATE TABLE {tmp} AS SELECT {exprs} FROM {current}")

            elif ntype == "add_const":
                name  = config.get("name", "new_col")
                val   = config.get("value", "NULL")
                dtype = config.get("dtype", "TEXT")
                pg.run(f"CREATE TABLE {tmp} AS SELECT {all_q}, CAST({repr(val)} AS {dtype}) AS \"{name}\" FROM {current}")

            elif ntype == "fill_null":
                fill_cols = config.get("columns", [])
                fill_val  = config.get("fillValue", "")
                fill_type = config.get("fillType", "value")
                exprs_list = []
                for c in cur_cols:
                    if c in fill_cols:
                        cdtype = cur_schema.get(c, "TEXT")
                        if fill_type == "mean":
                            exprs_list.append(f'COALESCE("{c}", AVG("{c}") OVER()) AS "{c}"')
                        elif fill_type == "forward":
                            exprs_list.append(f'COALESCE("{c}", LAG("{c}") OVER (ORDER BY 1)) AS "{c}"')
                        elif fill_type == "backward":
                            exprs_list.append(f'COALESCE("{c}", LEAD("{c}") OVER (ORDER BY 1)) AS "{c}"')
                        else:
                            if cdtype in ("BIGINT", "INTEGER", "NUMERIC"):
                                try:
                                    exprs_list.append(f'COALESCE("{c}", {float(fill_val)}) AS "{c}"')
                                except:
                                    exprs_list.append(f'"{c}"')
                            else:
                                exprs_list.append(f"COALESCE(\"{c}\"::TEXT, {repr(str(fill_val))}) AS \"{c}\"")
                    else:
                        exprs_list.append(f'"{c}"')
                pg.run(f"CREATE TABLE {tmp} AS SELECT {', '.join(exprs_list)} FROM {current}")

            elif ntype == "order_table":
                orders = [o for o in config.get("orders", []) if o.get("col") in cur_cols]
                oc = ", ".join(f'"{o["col"]}" {o.get("dir","ASC")}' for o in orders) if orders else "1"
                pg.run(f"CREATE TABLE {tmp} AS SELECT {all_q} FROM {current} ORDER BY {oc}")

            elif ntype == "change_type":
                types = config.get("types", {})
                exprs = ", ".join(
                    f'"{c}"::TEXT::{types[c]} AS "{c}"' if c in types else f'"{c}"'
                    for c in cur_cols
                )
                pg.run(f"CREATE TABLE {tmp} AS SELECT {exprs} FROM {current}")

            elif ntype == "set_val":
                target = config.get("targetCol", "")
                if config.get("useExpr"):
                    expr = config.get("expr", "NULL")
                else:
                    expr = f'"{config.get("sourceCol", "NULL")}"'
                exprs = ", ".join(
                    f'{expr} AS "{c}"' if c == target else f'"{c}"'
                    for c in cur_cols
                )
                pg.run(f"CREATE TABLE {tmp} AS SELECT {exprs} FROM {current}")

            elif ntype == "val_mapper":
                src      = config.get("sourceCol", "")
                new_col  = config.get("newColName", "mapped")
                else_val = config.get("elseValue", "NULL")
                whens    = config.get("whens", [])
                cases    = " ".join(
                    f'WHEN "{src}" {w.get("condition","=")} {repr(w.get("value",""))} THEN {repr(w.get("result",""))}'
                    for w in whens
                )
                pg.run(f"CREATE TABLE {tmp} AS SELECT {all_q}, CASE {cases} ELSE {repr(else_val)} END AS \"{new_col}\" FROM {current}")

            elif ntype == "group_agg":
                gcols = [c for c in config.get("groupCols", []) if c in cur_cols]
                acols = [a for a in config.get("aggCols", []) if a.get("col") in cur_cols]
                if gcols and acols:
                    g = q(gcols)
                    a = ", ".join(f'{x["func"]}("{x["col"]}") AS "{x["alias"]}"' for x in acols)
                    pg.run(f"CREATE TABLE {tmp} AS SELECT {g}, {a} FROM {current} GROUP BY {g}")
                else:
                    tmp = current

            else:
                print(f"[Task] Skip: {ntype}")
                tmp = current

        except Exception as e:
            print(f"[Task] Error step {step} ({ntype}): {e}")
            raise

        if tmp != current:
            current = tmp
            print(f"[Task] Step {step} ({ntype}) → {current}")

    print(f"[Task] Transform done → {current}")

    # ── LOAD ──────────────────────────────────────────────────────
    final_schema = get_schema(pg, current) or schema
    pg.run("CREATE SCHEMA IF NOT EXISTS warehouse")
    out = f"warehouse.{OUTPUT_NAME}"
    pg.run(f"DROP TABLE IF EXISTS {out}")

    col_defs = ", ".join(f'"{c}" {dt}' for c, dt in final_schema.items())
    pg.run(f"""
        CREATE TABLE {out} (
            {col_defs},
            date_partition DATE DEFAULT CURRENT_DATE,
            loaded_at TIMESTAMP DEFAULT NOW()
        )
    """)

    col_names = q(final_schema.keys())
    pg.run(f"""
        INSERT INTO {out} ({col_names}, date_partition, loaded_at)
        SELECT {col_names}, CURRENT_DATE, NOW()
        FROM {current}
    """)

    count = pg.get_first(f"SELECT COUNT(*) FROM {out}")[0]
    print(f"[Task] {count} rows → {out}")

    # Update backend
    if run_id:
        try:
            requests.patch(
                f"{BACKEND_URL}/api/pipelines/runs/{run_id}",
                json={"status": "success", "row_count": count},
                timeout=5
            )
        except Exception as e:
            print(f"[Task] Backend update failed: {e}")

    # Cleanup temp tables
    temps2 = pg.get_records(f"""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'staging'
        AND table_name LIKE '_pipeline_ggg_1777306106_step_%'
    """)
    for (t,) in temps2:
        pg.run(f'DROP TABLE IF EXISTS staging."{t}"')

    print(f"[Done] {DAG_ID} complete!")



with DAG(
    dag_id='pipeline_ggg_1777306106',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "generated", 'wf_1777305920484'],
    description='',
) as dag:
    task = PythonOperator(
        task_id="task",
        python_callable=run_pipeline,
    )
