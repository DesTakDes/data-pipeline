from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import psycopg2
import psycopg2.extras
import pandas as pd
import requests
import os
import io
import json
from datetime import datetime
from typing import Optional

app = FastAPI(title="ETLFlow API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Config ────────────────────────────────────────────────────────
AIRFLOW_URL  = os.getenv("AIRFLOW_URL", "http://airflow-webserver:8080")
AIRFLOW_AUTH = ("admin", "admin123")
PG_CONFIG    = {
    "host":     os.getenv("POSTGRES_HOST", "postgres"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "database": os.getenv("POSTGRES_DB", "airflow"),
    "user":     os.getenv("POSTGRES_USER", "airflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
}

def get_conn():
    return psycopg2.connect(**PG_CONFIG)

# ── Health ────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.now().isoformat()}

# ── Airflow Status ────────────────────────────────────────────────
@app.get("/api/airflow/status")
def airflow_status():
    try:
        r = requests.get(f"{AIRFLOW_URL}/health", timeout=5)
        return {"connected": r.status_code == 200}
    except:
        return {"connected": False}

@app.get("/api/airflow/dags")
def list_dags():
    r = requests.get(f"{AIRFLOW_URL}/api/v1/dags",
                     auth=AIRFLOW_AUTH, timeout=10)
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

@app.patch("/api/airflow/dags/{dag_id}/pause")
def pause_dag(dag_id: str, is_paused: bool = True):
    r = requests.patch(
        f"{AIRFLOW_URL}/api/v1/dags/{dag_id}",
        auth=AIRFLOW_AUTH,
        json={"is_paused": is_paused},
        timeout=10
    )
    return r.json()

# ── Datasets ──────────────────────────────────────────────────────
@app.get("/api/datasets")
def list_datasets():
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS meta.datasets (
            id          SERIAL PRIMARY KEY,
            name        TEXT NOT NULL,
            type        TEXT NOT NULL,
            status      TEXT DEFAULT 'pending',
            row_count   INTEGER,
            file_size   TEXT,
            table_name  TEXT,
            created_at  TIMESTAMP DEFAULT NOW(),
            updated_at  TIMESTAMP DEFAULT NOW()
        )
    """)
    conn.commit()
    cur.execute("SELECT * FROM meta.datasets ORDER BY created_at DESC")
    rows = cur.fetchall()
    cur.close(); conn.close()
    return [dict(r) for r in rows]

@app.delete("/api/datasets/{dataset_id}")
def delete_dataset(dataset_id: int):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT table_name FROM meta.datasets WHERE id = %s", (dataset_id,))
    row = cur.fetchone()
    if row and row["table_name"]:
        try:
            cur.execute(f'DROP TABLE IF EXISTS staging."{row["table_name"]}"')
        except:
            pass
    cur.execute("DELETE FROM meta.datasets WHERE id = %s", (dataset_id,))
    conn.commit()
    cur.close(); conn.close()
    return {"deleted": True}

@app.post("/api/datasets/upload")
async def upload_dataset(
    file: UploadFile = File(...),
    name: Optional[str] = Form(None),
):
    content = await file.read()
    filename = name or file.filename or "upload"
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else "csv"

    try:
        if ext == "csv":
            # Fix: hapus parameter 'errors', pakai encoding_errors
            try:
                df = pd.read_csv(io.BytesIO(content), encoding="utf-8")
            except UnicodeDecodeError:
                df = pd.read_csv(io.BytesIO(content), encoding="latin-1")
        elif ext in ("xlsx", "xls"):
            df = pd.read_excel(io.BytesIO(content))
        else:
            raise HTTPException(400, "Only CSV and Excel supported")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(400, f"Could not parse file: {e}")

    # Sanitize column names
    df.columns = [
        c.strip().lower()
         .replace(" ", "_")
         .replace("-", "_")
         .replace(".", "_")
        for c in df.columns
    ]
    
    # Ganti NaN dengan None
    df = df.where(pd.notnull(df), None)
    
    import re as _re
    base_name  = filename.rsplit(".", 1)[0]
    table_name = _re.sub(r'[^a-z0-9_]', '_', base_name.lower())
    table_name = _re.sub(r'_+', '_', table_name).strip('_')

    conn = get_conn()
    cur = conn.cursor()

    # Ensure schemas exist
    cur.execute("CREATE SCHEMA IF NOT EXISTS meta")
    cur.execute("CREATE SCHEMA IF NOT EXISTS staging")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS meta.datasets (
            id SERIAL PRIMARY KEY, name TEXT, type TEXT,
            status TEXT DEFAULT 'pending', row_count INTEGER,
            file_size TEXT, table_name TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """)

    # Create staging table dynamically
    type_map = {"int64": "BIGINT", "float64": "NUMERIC", "bool": "BOOLEAN"}
    col_defs = ", ".join([
        f'"{c}" {type_map.get(str(df[c].dtype), "TEXT")}'
        for c in df.columns
    ])
    cur.execute(f'DROP TABLE IF EXISTS staging."{table_name}"')
    cur.execute(f'CREATE TABLE staging."{table_name}" ({col_defs})')

    # Insert rows
    cols = [f'"{c}"' for c in df.columns]
    placeholders = ", ".join(["%s"] * len(df.columns))
    insert_sql = f'INSERT INTO staging."{table_name}" ({", ".join(cols)}) VALUES ({placeholders})'
    rows = [tuple(None if pd.isna(v) else v for v in row) for row in df.itertuples(index=False)]
    psycopg2.extras.execute_batch(cur, insert_sql, rows, page_size=500)

    # Save to meta
    size_kb = len(content) / 1024
    size_str = f"{size_kb:.1f} KB" if size_kb < 1024 else f"{size_kb/1024:.1f} MB"
    cur.execute("""
        INSERT INTO meta.datasets (name, type, status, row_count, file_size, table_name)
        VALUES (%s, %s, 'deployed', %s, %s, %s) RETURNING id
    """, (filename, ext.upper(), len(df), size_str, table_name))
    new_id = cur.fetchone()[0]
    conn.commit()
    cur.close(); conn.close()

    return {
        "id": new_id, "name": filename, "type": ext.upper(),
        "rows": len(df), "columns": list(df.columns),
        "size": size_str, "table_name": table_name,
    }

@app.post("/api/datasets/connect-db")
def connect_db(payload: dict):
    try:
        test_conn = psycopg2.connect(
            host=payload["host"], port=payload.get("port", 5432),
            database=payload["database"], user=payload["username"],
            password=payload["password"], connect_timeout=5
        )
        test_conn.close()
    except Exception as e:
        raise HTTPException(400, f"Connection failed: {e}")

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS meta")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS meta.datasets (
            id SERIAL PRIMARY KEY, name TEXT, type TEXT,
            status TEXT DEFAULT 'pending', row_count INTEGER,
            file_size TEXT, table_name TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.execute("""
        INSERT INTO meta.datasets (name, type, status)
        VALUES (%s, %s, 'connected') RETURNING id
    """, (f"{payload['database']}@{payload['host']}", payload.get("db_type", "PostgreSQL").upper()))
    conn.commit()
    cur.close(); conn.close()
    return {"connected": True}

@app.get("/api/datasets/{dataset_id}/preview")
def preview_dataset(dataset_id: int, limit: int = 100):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM meta.datasets WHERE id = %s", (dataset_id,))
    ds = cur.fetchone()
    if not ds or not ds["table_name"]:
        raise HTTPException(404, "Dataset not found")
    cur.execute(f'SELECT * FROM staging."{ds["table_name"]}" LIMIT %s', (limit,))
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    cur.close(); conn.close()
    return {"columns": columns, "rows": [dict(r) for r in rows]}

# ── Warehouse ─────────────────────────────────────────────────────
@app.get("/api/warehouse/tables")
def warehouse_tables():
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT table_name,
            (SELECT COUNT(*) FROM information_schema.columns c2
             WHERE c2.table_schema = 'warehouse' AND c2.table_name = t.table_name) as col_count
        FROM information_schema.tables t
        WHERE table_schema = 'warehouse' AND table_type = 'BASE TABLE'
    """)
    rows = cur.fetchall()
    cur.close(); conn.close()
    return [dict(r) for r in rows]

# ── Pipeline (Workflow) ───────────────────────────────────────────
import os
import re
import time
from pathlib import Path

DAGS_FOLDER = os.getenv("DAGS_FOLDER", "/opt/airflow/dags")

@app.post("/api/pipelines/run")
def run_pipeline(payload: dict):
    """
    1 workflow = 1 DAG.
    Kalau DAG sudah ada untuk workflow ini, trigger saja.
    Kalau belum ada, buat DAG baru.
    """
    workflow_id   = payload.get("workflow_id", f"wf_{int(time.time())}")
    workflow_name = payload.get("workflow_name", "Pipeline")
    input_table   = payload.get("input_table", "")
    output_name   = payload.get("output_name", "output")
    transforms    = payload.get("transforms", [])
    description   = payload.get("description", "")

    # Sanitize
    safe_input  = re.sub(r'[^a-zA-Z0-9_.]', '', input_table)
    safe_output = re.sub(r'[^a-z0-9_]', '_', output_name.lower())

    # ── DAG ID tetap sama untuk workflow yang sama ────────────────
    safe_wf = re.sub(r'[^a-z0-9_]', '_', workflow_id.lower())[:40]
    dag_id  = f"pipeline_{safe_wf}"

    airflow_url  = os.getenv("AIRFLOW_URL", "http://airflow-webserver:8080")
    airflow_auth = ("admin", "admin123")

    # ── Cek apakah DAG sudah ada di Airflow ──────────────────────
    dag_exists = False
    try:
        r = requests.get(
            f"{airflow_url}/api/v1/dags/{dag_id}",
            auth=airflow_auth, timeout=5
        )
        dag_exists = r.status_code == 200
    except:
        pass

    dag_path = Path(DAGS_FOLDER) / f"{dag_id}.py"

    # ── Buat atau update DAG file ─────────────────────────────────
    # Selalu update file DAG agar konfigurasi terbaru (transforms, input, output)
    dag_content = generate_dag(
        dag_id        = dag_id,
        workflow_id   = workflow_id,
        workflow_name = workflow_name,
        input_table   = safe_input,
        output_name   = safe_output,
        transforms    = transforms,
        description   = description,
    )

    try:
        dag_path.write_text(dag_content)
        print(f"DAG file written: {dag_path}")
    except Exception as e:
        raise HTTPException(500, f"Failed to write DAG file: {e}")

    # ── Simpan run ke database ────────────────────────────────────
    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("CREATE SCHEMA IF NOT EXISTS meta")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS meta.pipeline_runs (
                id            SERIAL PRIMARY KEY,
                dag_id        TEXT,
                workflow_id   TEXT,
                workflow_name TEXT,
                input_table   TEXT,
                output_table  TEXT,
                row_count     INTEGER,
                status        TEXT DEFAULT 'pending',
                ran_at        TIMESTAMP DEFAULT NOW(),
                finished_at   TIMESTAMP
            )
        """)
        cur.execute("""
            INSERT INTO meta.pipeline_runs
                (dag_id, workflow_id, workflow_name, input_table, output_table, status)
            VALUES (%s, %s, %s, %s, %s, 'pending')
            RETURNING id
        """, (dag_id, workflow_id, workflow_name, safe_input, f"warehouse.{safe_output}"))
        run_id = cur.fetchone()["id"]
        conn.commit()
    finally:
        cur.close(); conn.close()

    # ── Tunggu Airflow detect DAG (hanya kalau baru) ──────────────
    if not dag_exists:
        print(f"New DAG {dag_id} — waiting for Airflow to detect...")
        for i in range(20):
            time.sleep(2)
            try:
                r = requests.get(
                    f"{airflow_url}/api/v1/dags/{dag_id}",
                    auth=airflow_auth, timeout=5
                )
                if r.status_code == 200:
                    print(f"DAG {dag_id} detected after {(i+1)*2}s")
                    break
            except:
                pass
    else:
        # DAG sudah ada — unpause kalau ter-pause, tunggu sebentar agar file update terbaca
        time.sleep(3)
        try:
            requests.patch(
                f"{airflow_url}/api/v1/dags/{dag_id}",
                auth=airflow_auth,
                json={"is_paused": False},
                timeout=5
            )
        except:
            pass

    # ── Trigger DAG ───────────────────────────────────────────────
    try:
        r = requests.post(
            f"{airflow_url}/api/v1/dags/{dag_id}/dagRuns",
            auth=airflow_auth,
            json={"conf": {"run_id": run_id}},
            timeout=10
        )
        dag_run = r.json()
        print(f"DAG triggered: {dag_run}")
    except Exception as e:
        dag_run = {"error": str(e)}

    return {
        "run_id":  run_id,
        "dag_id":  dag_id,
        "dag_run": dag_run,
        "status":  "triggered",
        "is_new":  not dag_exists,
    }


@app.get("/api/pipelines/runs")
def list_pipeline_runs():
    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("CREATE SCHEMA IF NOT EXISTS meta")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS meta.pipeline_runs (
                id            SERIAL PRIMARY KEY,
                dag_id        TEXT,
                workflow_id   TEXT,
                workflow_name TEXT,
                input_table   TEXT,
                output_table  TEXT,
                row_count     INTEGER,
                status        TEXT DEFAULT 'pending',
                ran_at        TIMESTAMP DEFAULT NOW(),
                finished_at   TIMESTAMP
            )
        """)
        conn.commit()
        cur.execute("""
            SELECT id, dag_id, workflow_id, workflow_name,
                   input_table, output_table, row_count, status,
                   ran_at::text, finished_at::text
            FROM meta.pipeline_runs
            ORDER BY ran_at DESC
            LIMIT 50
        """)
        return [dict(r) for r in cur.fetchall()]
    except Exception as e:
        return []
    finally:
        cur.close(); conn.close()


@app.patch("/api/pipelines/runs/{run_id}")
def update_pipeline_run(run_id: int, payload: dict):
    """Update status dan row_count setelah DAG selesai."""
    conn = get_conn()
    cur  = conn.cursor()
    try:
        cur.execute("""
            UPDATE meta.pipeline_runs
            SET status      = %s,
                row_count   = %s,
                finished_at = NOW()
            WHERE id = %s
        """, (payload.get("status"), payload.get("row_count"), run_id))
        conn.commit()
        return {"updated": True}
    finally:
        cur.close(); conn.close()


@app.get("/api/pipelines/runs/{run_id}/preview")
def preview_run(run_id: int, limit: int = 100):
    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute(
            "SELECT output_table, status FROM meta.pipeline_runs WHERE id = %s",
            (run_id,)
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "Run not found")
        if row["status"] not in ("success", "completed"):
            raise HTTPException(400, f"Pipeline not completed yet (status: {row['status']})")

        table = row["output_table"]
        parts = table.split(".")
        if len(parts) != 2:
            raise HTTPException(400, f"Invalid table: {table}")

        cur.execute(f'SELECT * FROM {table} LIMIT %s', (limit,))
        rows    = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.execute(f'SELECT COUNT(*) as cnt FROM {table}')
        total = cur.fetchone()["cnt"]

        return {"columns": columns, "rows": [dict(r) for r in rows], "table": table, "total": total}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(400, str(e))
    finally:
        cur.close(); conn.close()


@app.get("/api/pipelines/runs/{run_id}/dag-status")
def get_dag_status(run_id: int):
    """Ambil status DAG dari Airflow untuk run tertentu."""
    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("SELECT dag_id FROM meta.pipeline_runs WHERE id = %s", (run_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "Run not found")
        dag_id = row["dag_id"]
    finally:
        cur.close(); conn.close()

    airflow_url  = os.getenv("AIRFLOW_URL", "http://airflow-webserver:8080")
    airflow_auth = ("admin", "admin123")

    try:
        r = requests.get(
            f"{airflow_url}/api/v1/dags/{dag_id}/dagRuns?limit=1&order_by=-execution_date",
            auth=airflow_auth, timeout=10
        )
        runs = r.json().get("dag_runs", [])
        run  = runs[0] if runs else {}

        # Ambil task instances
        tasks = {}
        if run.get("dag_run_id"):
            tr = requests.get(
                f"{airflow_url}/api/v1/dags/{dag_id}/dagRuns/{run['dag_run_id']}/taskInstances",
                auth=airflow_auth, timeout=10
            )
            for t in tr.json().get("task_instances", []):
                tasks[t["task_id"]] = t["state"]

        return {
            "dag_id":    dag_id,
            "state":     run.get("state", "unknown"),
            "dag_run_id":run.get("dag_run_id"),
            "tasks":     tasks,
        }
    except Exception as e:
        return {"dag_id": dag_id, "state": "unknown", "error": str(e)}


def generate_dag(dag_id, workflow_id, workflow_name, input_table,
                 output_name, transforms, description=""):
    import json as _json

    # Serialize transforms sebagai JSON yang akan di-embed
    transforms_json = _json.dumps(transforms, ensure_ascii=True)

    safe_input = _re.sub(r'[^a-zA-Z0-9_.]', '', input_table)
    safe_output = re.sub(r'[^a-z0-9_]', '_', output_name.lower())
    safe_wf_id  = workflow_id.replace("'", "")
    safe_name   = workflow_name.replace("'", "").replace('"', '')
    now_str     = __import__("datetime").datetime.now().isoformat()

    # Tulis DAG sebagai file Python biasa (bukan f-string kompleks)
    lines = []
    lines.append(f"# Auto-generated DAG: {dag_id}")
    lines.append(f"# Workflow: {safe_name}")
    lines.append(f"# Generated: {now_str}")
    lines.append("")
    lines.append("from airflow import DAG")
    lines.append("from airflow.operators.python import PythonOperator")
    lines.append("from airflow.providers.postgres.hooks.postgres import PostgresHook")
    lines.append("from datetime import datetime")
    lines.append("import json")
    lines.append("import requests")
    lines.append("")
    lines.append(f'DAG_ID       = {repr(dag_id)}')
    lines.append(f'INPUT_TABLE  = {repr(safe_input)}')
    lines.append(f'OUTPUT_NAME  = {repr(safe_output)}')
    lines.append(f'WORKFLOW_ID  = {repr(safe_wf_id)}')
    lines.append(f'TRANSFORMS   = json.loads({repr(transforms_json)})')
    lines.append(f'BACKEND_URL  = "http://backend:8000"')
    lines.append("")
    lines.append('default_args = {"owner": "etlflow", "retries": 0}')
    lines.append("")

    # Helper dan task function sebagai raw string
    lines.append('''
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
        AND table_name LIKE '_''' + dag_id + '''_step_%'
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
                pg.run(f"CREATE TABLE {tmp} AS SELECT {all_q}, CAST({repr(val)} AS {dtype}) AS \\"{name}\\" FROM {current}")

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
                                exprs_list.append(f"COALESCE(\\"{c}\\"::TEXT, {repr(str(fill_val))}) AS \\"{c}\\"")
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
                pg.run(f"CREATE TABLE {tmp} AS SELECT {all_q}, CASE {cases} ELSE {repr(else_val)} END AS \\"{new_col}\\" FROM {current}")

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
        AND table_name LIKE '_''' + dag_id + '''_step_%'
    """)
    for (t,) in temps2:
        pg.run(f'DROP TABLE IF EXISTS staging."{t}"')

    print(f"[Done] {DAG_ID} complete!")

''')

    # DAG definition
    lines.append(f"""
with DAG(
    dag_id={repr(dag_id)},
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "generated", {repr(safe_wf_id)}],
    description={repr(description)},
) as dag:
    task = PythonOperator(
        task_id="task",
        python_callable=run_pipeline,
    )
""")

    return "\n".join(lines)


@app.get("/api/warehouse/tables")
def warehouse_tables():
    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("""
            SELECT
                t.table_name,
                COUNT(c.column_name) as col_count,
                (SELECT COUNT(*) FROM information_schema.tables t2
                 WHERE t2.table_schema = 'warehouse'
                 AND t2.table_name = t.table_name) as exists
            FROM information_schema.tables t
            LEFT JOIN information_schema.columns c
                ON c.table_schema = t.table_schema
                AND c.table_name  = t.table_name
            WHERE t.table_schema = 'warehouse'
            AND t.table_type = 'BASE TABLE'
            GROUP BY t.table_name
            ORDER BY t.table_name
        """)
        rows = cur.fetchall()
        return [dict(r) for r in rows]
    except Exception as e:
        return []
    finally:
        cur.close(); conn.close()