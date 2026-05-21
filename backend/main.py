from fastapi import FastAPI, UploadFile, File, HTTPException, Form
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
from datetime import datetime
from typing import Optional
from pathlib import Path

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
LARGE_FILE_THRESHOLD_GB = 10

def get_conn():
    return psycopg2.connect(**PG_CONFIG)

def ensure_schemas(cur, conn):
    cur.execute("CREATE SCHEMA IF NOT EXISTS meta")
    cur.execute("CREATE SCHEMA IF NOT EXISTS staging")
    cur.execute("CREATE SCHEMA IF NOT EXISTS warehouse")
    conn.commit()

# ── Health ──────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.now().isoformat()}

# ── Airflow ─────────────────────────────────────────────────────────
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

# ── Datasets ────────────────────────────────────────────────────────
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
    # Add missing columns if they don't exist
    for col, dtype in [
        ("col_count", "INTEGER"),
        ("file_size_bytes", "BIGINT"),
        ("parquet_path", "TEXT"),
        ("is_large", "BOOLEAN DEFAULT FALSE"),
    ]:
        try:
            cur.execute(f"ALTER TABLE meta.datasets ADD COLUMN IF NOT EXISTS {col} {dtype}")
        except:
            pass
    conn.commit()

@app.get("/api/datasets")
def list_datasets():
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    ensure_datasets_table(cur, conn)
    cur.execute("SELECT * FROM meta.datasets ORDER BY created_at DESC")
    rows = cur.fetchall()
    cur.close(); conn.close()
    return [dict(r) for r in rows]

@app.delete("/api/datasets/{dataset_id}")
def delete_dataset(dataset_id: int):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT table_name, parquet_path FROM meta.datasets WHERE id = %s", (dataset_id,))
    row = cur.fetchone()
    if row and row["table_name"]:
        try:
            cur.execute(f'DROP TABLE IF EXISTS staging."{row["table_name"]}"')
        except:
            pass
    if row and row.get("parquet_path"):
        try:
            Path(row["parquet_path"]).unlink(missing_ok=True)
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
    file_size_bytes = len(content)
    file_size_gb = file_size_bytes / (1024 ** 3)
    is_large = file_size_gb >= LARGE_FILE_THRESHOLD_GB

    try:
        if ext == "csv":
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
        c.strip().lower().replace(" ", "_").replace("-", "_").replace(".", "_")
        for c in df.columns
    ]
    df = df.where(pd.notnull(df), None)

    base_name  = filename.rsplit(".", 1)[0]
    table_name = re.sub(r'[^a-z0-9_]', '_', base_name.lower())
    table_name = re.sub(r'_+', '_', table_name).strip('_')

    conn = get_conn()
    cur = conn.cursor()
    ensure_schemas(cur, conn)
    ensure_datasets_table(cur, conn)

    # Save as Parquet if large
    parquet_path = None
    if is_large:
        os.makedirs(PARQUET_DIR, exist_ok=True)
        parquet_path = f"{PARQUET_DIR}/{table_name}.parquet"
        df.to_parquet(parquet_path, index=False, engine='pyarrow')

    # Create staging table
    type_map = {"int64": "BIGINT", "float64": "NUMERIC", "bool": "BOOLEAN"}
    col_defs = ", ".join([
        f'"{c}" {type_map.get(str(df[c].dtype), "TEXT")}'
        for c in df.columns
    ])
    cur.execute(f'DROP TABLE IF EXISTS staging."{table_name}"')
    cur.execute(f'CREATE TABLE staging."{table_name}" ({col_defs})')

    cols = [f'"{c}"' for c in df.columns]
    placeholders = ", ".join(["%s"] * len(df.columns))
    insert_sql = f'INSERT INTO staging."{table_name}" ({", ".join(cols)}) VALUES ({placeholders})'
    rows = [tuple(None if (v is None or (isinstance(v, float) and pd.isna(v))) else v for v in row) for row in df.itertuples(index=False)]
    psycopg2.extras.execute_batch(cur, insert_sql, rows, page_size=500)

    size_kb = file_size_bytes / 1024
    size_str = f"{size_kb:.1f} KB" if size_kb < 1024 else (f"{size_kb/1024:.1f} MB" if size_kb < 1024*1024 else f"{file_size_gb:.2f} GB")

    cur.execute("""
        INSERT INTO meta.datasets (name, type, status, row_count, col_count, file_size, file_size_bytes, table_name, parquet_path, is_large)
        VALUES (%s, %s, 'deployed', %s, %s, %s, %s, %s, %s, %s) RETURNING id
    """, (filename, ext.upper(), len(df), len(df.columns), size_str, file_size_bytes, table_name, parquet_path, is_large))
    new_id = cur.fetchone()[0]
    conn.commit()
    cur.close(); conn.close()

    return {
        "id": new_id, "name": filename, "type": ext.upper(),
        "rows": len(df), "columns": list(df.columns),
        "size": size_str, "table_name": table_name,
        "is_large": is_large, "parquet_path": parquet_path,
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
    ensure_datasets_table(cur, conn)
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
    return {"columns": columns, "rows": [dict(r) for r in rows], "total": ds["row_count"]}

@app.get("/api/datasets/{dataset_id}/download")
def download_dataset(dataset_id: int, format: str = "csv"):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM meta.datasets WHERE id = %s", (dataset_id,))
    ds = cur.fetchone()
    if not ds or not ds["table_name"]:
        raise HTTPException(404, "Dataset not found")
    cur.execute(f'SELECT * FROM staging."{ds["table_name"]}"')
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    cur.close(); conn.close()

    df = pd.DataFrame([dict(r) for r in rows], columns=columns)
    out_dir = "/tmp/etlflow_exports"
    os.makedirs(out_dir, exist_ok=True)
    if format == "parquet":
        path = f"{out_dir}/{ds['table_name']}.parquet"
        df.to_parquet(path, index=False)
        return FileResponse(path, filename=f"{ds['table_name']}.parquet", media_type="application/octet-stream")
    else:
        path = f"{out_dir}/{ds['table_name']}.csv"
        df.to_csv(path, index=False)
        return FileResponse(path, filename=f"{ds['table_name']}.csv", media_type="text/csv")

# ── Warehouse ────────────────────────────────────────────────────────
@app.get("/api/warehouse/tables")
def warehouse_tables():
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("CREATE SCHEMA IF NOT EXISTS warehouse")
        conn.commit()
        cur.execute("""
            SELECT t.table_name,
                COUNT(c.column_name) as col_count
            FROM information_schema.tables t
            LEFT JOIN information_schema.columns c
                ON c.table_schema = t.table_schema AND c.table_name = t.table_name
            WHERE t.table_schema = 'warehouse' AND t.table_type = 'BASE TABLE'
            GROUP BY t.table_name ORDER BY t.table_name
        """)
        rows = cur.fetchall()
        return [dict(r) for r in rows]
    except Exception as e:
        return []
    finally:
        cur.close(); conn.close()

@app.get("/api/warehouse/{table_name}/download")
def download_warehouse_table(table_name: str, format: str = "csv"):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute(f'SELECT * FROM warehouse."{table_name}"')
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame([dict(r) for r in rows], columns=columns)
        out_dir = "/tmp/etlflow_exports"
        os.makedirs(out_dir, exist_ok=True)
        if format == "parquet":
            path = f"{out_dir}/{table_name}.parquet"
            df.to_parquet(path, index=False)
            return FileResponse(path, filename=f"{table_name}.parquet", media_type="application/octet-stream")
        else:
            path = f"{out_dir}/{table_name}.csv"
            df.to_csv(path, index=False)
            return FileResponse(path, filename=f"{table_name}.csv", media_type="text/csv")
    finally:
        cur.close(); conn.close()

# ── Pipeline ─────────────────────────────────────────────────────────
@app.post("/api/pipelines/run")
def run_pipeline(payload: dict):
    workflow_id   = payload.get("workflow_id", f"wf_{int(time.time())}")
    workflow_name = payload.get("workflow_name", "Pipeline")
    input_table   = payload.get("input_table", "")
    tasks         = payload.get("tasks", [])  # Multi-branch tasks
    description   = payload.get("description", "")
    # Legacy single-task support
    if not tasks and payload.get("output_name"):
        tasks = [{
            "task_id": "task_1",
            "output_name": payload.get("output_name", "output"),
            "transforms": payload.get("transforms", []),
            "depends_on": [],
        }]

    safe_input  = re.sub(r'[^a-zA-Z0-9_.]', '', input_table)
    # DAG ID = nama workflow langsung (lowercase, tanpa prefix)
    safe_wf_name = re.sub(r'[^a-z0-9_]', '_', workflow_name.lower().strip())
    safe_wf_name = re.sub(r'_+', '_', safe_wf_name).strip('_')[:60]
    dag_id       = safe_wf_name if safe_wf_name else re.sub(r'[^a-z0-9_]', '_', workflow_id.lower())[:60]

    airflow_url  = os.getenv("AIRFLOW_URL", "http://airflow-webserver:8080")
    airflow_auth = ("admin", "admin123")

    # Check if DAG exists
    dag_exists = False
    try:
        r = requests.get(f"{airflow_url}/api/v1/dags/{dag_id}", auth=airflow_auth, timeout=5)
        dag_exists = r.status_code == 200
    except:
        pass

    dag_path = Path(DAGS_FOLDER) / f"{dag_id}.py"

    # Generate multi-task Spark DAG
    dag_content = generate_spark_dag(
        dag_id=dag_id,
        workflow_id=workflow_id,
        workflow_name=workflow_name,
        input_table=safe_input,
        tasks=tasks,
        description=description,
    )

    try:
        dag_path.write_text(dag_content)
    except Exception as e:
        raise HTTPException(500, f"Failed to write DAG file: {e}")

    # Save run records
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    run_ids = []
    try:
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
                ran_at        TIMESTAMP DEFAULT NOW(),
                finished_at   TIMESTAMP
            )
        """)
        # Add task_id column if missing
        try:
            cur.execute("ALTER TABLE meta.pipeline_runs ADD COLUMN IF NOT EXISTS task_id TEXT")
        except:
            pass
        conn.commit()

        for task in tasks:
            safe_out = re.sub(r'[^a-z0-9_]', '_', task.get("output_name","output").lower())
            cur.execute("""
                INSERT INTO meta.pipeline_runs
                    (dag_id, task_id, workflow_id, workflow_name, input_table, output_table, status)
                VALUES (%s, %s, %s, %s, %s, %s, 'pending')
                RETURNING id
            """, (dag_id, task.get("task_id","task_1"), workflow_id, workflow_name,
                  safe_input, f"warehouse.{safe_out}"))
            run_ids.append(cur.fetchone()["id"])
        conn.commit()
    finally:
        cur.close(); conn.close()

    # Wait for DAG detection
    if not dag_exists:
        for i in range(20):
            time.sleep(2)
            try:
                r = requests.get(f"{airflow_url}/api/v1/dags/{dag_id}", auth=airflow_auth, timeout=5)
                if r.status_code == 200:
                    break
            except:
                pass
    else:
        time.sleep(3)
        try:
            requests.patch(f"{airflow_url}/api/v1/dags/{dag_id}", auth=airflow_auth,
                           json={"is_paused": False}, timeout=5)
        except:
            pass

    # Trigger DAG
    try:
        r = requests.post(
            f"{airflow_url}/api/v1/dags/{dag_id}/dagRuns",
            auth=airflow_auth,
            json={"conf": {"run_ids": run_ids}},
            timeout=10
        )
        dag_run = r.json()
    except Exception as e:
        dag_run = {"error": str(e)}

    return {
        "run_ids": run_ids,
        "run_id": run_ids[0] if run_ids else None,
        "dag_id": dag_id,
        "dag_run": dag_run,
        "status": "triggered",
        "is_new": not dag_exists,
    }


@app.get("/api/pipelines/runs")
def list_pipeline_runs():
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("CREATE SCHEMA IF NOT EXISTS meta")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS meta.pipeline_runs (
                id SERIAL PRIMARY KEY, dag_id TEXT, task_id TEXT,
                workflow_id TEXT, workflow_name TEXT,
                input_table TEXT, output_table TEXT, row_count INTEGER,
                status TEXT DEFAULT 'pending',
                ran_at TIMESTAMP DEFAULT NOW(), finished_at TIMESTAMP
            )
        """)
        try:
            cur.execute("ALTER TABLE meta.pipeline_runs ADD COLUMN IF NOT EXISTS task_id TEXT")
        except:
            pass
        conn.commit()
        cur.execute("""
            SELECT id, dag_id, task_id, workflow_id, workflow_name,
                   input_table, output_table, row_count, status,
                   ran_at::text, finished_at::text
            FROM meta.pipeline_runs ORDER BY ran_at DESC LIMIT 100
        """)
        return [dict(r) for r in cur.fetchall()]
    except Exception as e:
        return []
    finally:
        cur.close(); conn.close()


@app.patch("/api/pipelines/runs/{run_id}")
def update_pipeline_run(run_id: int, payload: dict):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE meta.pipeline_runs
            SET status = %s, row_count = %s, finished_at = NOW()
            WHERE id = %s
        """, (payload.get("status"), payload.get("row_count"), run_id))
        conn.commit()
        return {"updated": True}
    finally:
        cur.close(); conn.close()


@app.get("/api/pipelines/runs/{run_id}/preview")
def preview_run(run_id: int, limit: int = 100):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("SELECT output_table, status FROM meta.pipeline_runs WHERE id = %s", (run_id,))
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
        rows = cur.fetchall()
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


@app.get("/api/pipelines/runs/{run_id}/download")
def download_run_output(run_id: int, format: str = "csv"):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("SELECT output_table, status FROM meta.pipeline_runs WHERE id = %s", (run_id,))
        row = cur.fetchone()
        if not row or row["status"] not in ("success", "completed"):
            raise HTTPException(400, "Pipeline output not available")
        table = row["output_table"]
        cur.execute(f'SELECT * FROM {table}')
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame([dict(r) for r in rows], columns=columns)
        tname = table.replace("warehouse.", "")
        out_dir = "/tmp/etlflow_exports"
        os.makedirs(out_dir, exist_ok=True)
        if format == "parquet":
            path = f"{out_dir}/{tname}.parquet"
            df.to_parquet(path, index=False)
            return FileResponse(path, filename=f"{tname}.parquet", media_type="application/octet-stream")
        else:
            path = f"{out_dir}/{tname}.csv"
            df.to_csv(path, index=False)
            return FileResponse(path, filename=f"{tname}.csv", media_type="text/csv")
    finally:
        cur.close(); conn.close()


@app.get("/api/pipelines/runs/{run_id}/dag-status")
def get_dag_status(run_id: int):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("SELECT dag_id FROM meta.pipeline_runs WHERE id = %s", (run_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "Run not found")
        dag_id = row["dag_id"]
    finally:
        cur.close(); conn.close()

    try:
        r = requests.get(
            f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns?limit=1&order_by=-execution_date",
            auth=AIRFLOW_AUTH, timeout=10
        )
        runs = r.json().get("dag_runs", [])
        run = runs[0] if runs else {}
        tasks = {}
        if run.get("dag_run_id"):
            tr = requests.get(
                f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns/{run['dag_run_id']}/taskInstances",
                auth=AIRFLOW_AUTH, timeout=10
            )
            for t in tr.json().get("task_instances", []):
                tasks[t["task_id"]] = t["state"]
        return {
            "dag_id": dag_id, "state": run.get("state", "unknown"),
            "dag_run_id": run.get("dag_run_id"), "tasks": tasks,
        }
    except Exception as e:
        return {"dag_id": dag_id, "state": "unknown", "error": str(e)}


# ── Node Transform Preview (run SQL in-memory on staging data) ──────
@app.post("/api/preview/transform")
def preview_transform(payload: dict):
    """
    Apply transforms to staging data and return a preview.
    Used by the frontend to preview node output without running the full DAG.
    """
    dataset_id = payload.get("dataset_id")
    transforms = payload.get("transforms", [])  # list of {type, config}
    limit = payload.get("limit", 50)

    if not dataset_id:
        raise HTTPException(400, "dataset_id required")

    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("SELECT * FROM meta.datasets WHERE id = %s", (dataset_id,))
        ds = cur.fetchone()
        if not ds:
            raise HTTPException(404, "Dataset not found")

        table_name = ds["table_name"]
        current_table = f'staging."{table_name}"'

        # Get schema
        schema_cur = conn.cursor()
        schema_cur.execute(f"""
            SELECT column_name, data_type FROM information_schema.columns
            WHERE table_schema = 'staging' AND table_name = '{table_name}'
            ORDER BY ordinal_position
        """)
        schema = {row[0]: row[1] for row in schema_cur.fetchall()}
        cur_cols = list(schema.keys())

        # Apply each transform in memory via SQL CTEs
        cte_parts = []
        step = 0

        def q(cols):
            return ", ".join(f'"{c}"' for c in cols)

        for tx in transforms:
            ntype = tx.get("type", "")
            config = tx.get("config") or {}
            step += 1
            alias = f"step_{step}"
            prev = f"step_{step-1}" if step > 1 else current_table

            try:
                if ntype == "filter_rows":
                    formula = config.get("formula", "1=1")
                    cte_parts.append(f'{alias} AS (SELECT * FROM {prev} WHERE {formula})')
                elif ntype == "select_col":
                    cols = [c for c in config.get("columns", cur_cols) if c in cur_cols]
                    if cols:
                        cte_parts.append(f'{alias} AS (SELECT {q(cols)} FROM {prev})')
                        cur_cols = cols
                    else:
                        cte_parts.append(f'{alias} AS (SELECT * FROM {prev})')
                elif ntype == "drop_col":
                    keep = [c for c in cur_cols if c not in set(config.get("columns", []))]
                    cte_parts.append(f'{alias} AS (SELECT {q(keep)} FROM {prev})')
                    cur_cols = keep
                elif ntype == "rename_col":
                    renames = config.get("renames", {})
                    exprs = ", ".join(f'"{c}" AS "{renames.get(c, c)}"' for c in cur_cols)
                    cte_parts.append(f'{alias} AS (SELECT {exprs} FROM {prev})')
                    cur_cols = [renames.get(c, c) for c in cur_cols]
                elif ntype == "add_const":
                    name = config.get("name", "new_col")
                    val = config.get("value", "NULL")
                    dtype = config.get("dtype", "TEXT")
                    cte_parts.append(f'{alias} AS (SELECT {q(cur_cols)}, CAST({repr(val)} AS {dtype}) AS "{name}" FROM {prev})')
                    cur_cols = cur_cols + [name]
                elif ntype == "fill_null":
                    fill_cols = config.get("columns", [])
                    fill_val = config.get("fillValue", "")
                    fill_type = config.get("fillType", "value")
                    exprs_list = []
                    for c in cur_cols:
                        if c in fill_cols:
                            if fill_type == "value":
                                exprs_list.append(f"COALESCE(\"{c}\"::TEXT, {repr(str(fill_val))})::TEXT AS \"{c}\"")
                            else:
                                exprs_list.append(f'"{c}"')
                        else:
                            exprs_list.append(f'"{c}"')
                    cte_parts.append(f'{alias} AS (SELECT {", ".join(exprs_list)} FROM {prev})')
                elif ntype == "order_table":
                    orders = config.get("orders", [])
                    oc = ", ".join(f'"{o["col"]}" {o.get("dir","ASC")}' for o in orders if o.get("col") in cur_cols) or "1"
                    cte_parts.append(f'{alias} AS (SELECT {q(cur_cols)} FROM {prev} ORDER BY {oc})')
                elif ntype == "change_type":
                    types = config.get("types", {})
                    exprs = ", ".join(
                        f'"{c}"::TEXT::{types[c]} AS "{c}"' if c in types else f'"{c}"'
                        for c in cur_cols
                    )
                    cte_parts.append(f'{alias} AS (SELECT {exprs} FROM {prev})')
                elif ntype == "group_agg":
                    gcols = [c for c in config.get("groupCols", []) if c in cur_cols]
                    acols = config.get("aggCols", [])
                    if gcols and acols:
                        g = q(gcols)
                        a = ", ".join(f'{x["func"]}("{x["col"]}") AS "{x["alias"]}"' for x in acols)
                        cte_parts.append(f'{alias} AS (SELECT {g}, {a} FROM {prev} GROUP BY {g})')
                        cur_cols = gcols + [x["alias"] for x in acols]
                    else:
                        cte_parts.append(f'{alias} AS (SELECT * FROM {prev})')
                else:
                    cte_parts.append(f'{alias} AS (SELECT * FROM {prev})')
            except Exception as e:
                cte_parts.append(f'{alias} AS (SELECT * FROM {prev})')

        if cte_parts:
            last_alias = f"step_{step}"
            sql = f"WITH {', '.join(cte_parts)} SELECT * FROM {last_alias} LIMIT {limit}"
        else:
            sql = f"SELECT * FROM {current_table} LIMIT {limit}"

        cur.execute(sql)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        return {"columns": columns, "rows": [dict(r) for r in rows]}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(400, f"Transform preview failed: {e}")
    finally:
        cur.close(); conn.close()


# ── DAG Generator (Spark-based) ──────────────────────────────────────
def generate_spark_dag(dag_id, workflow_id, workflow_name, input_table, tasks, description=""):
    """
    Generate a multi-task Spark DAG.
    tasks: list of {task_id, output_name, transforms, depends_on}
    """
    tasks_json = json.dumps(tasks, ensure_ascii=True)
    safe_wf_id = workflow_id.replace("'", "")
    safe_name  = workflow_name.replace("'", "").replace('"', '')
    now_str    = datetime.now().isoformat()
    safe_input = re.sub(r'[^a-zA-Z0-9_.]', '', input_table)

    lines = []
    lines.append(f"# Auto-generated Spark DAG: {dag_id}")
    lines.append(f"# Workflow: {safe_name}")
    lines.append(f"# Generated: {now_str}")
    lines.append("")
    lines.append("from airflow import DAG")
    lines.append("from airflow.operators.python import PythonOperator")
    lines.append("from airflow.providers.postgres.hooks.postgres import PostgresHook")
    lines.append("from datetime import datetime")
    lines.append("import json, requests, os, math")
    lines.append("")
    lines.append(f'DAG_ID      = {repr(dag_id)}')
    lines.append(f'INPUT_TABLE = {repr(safe_input)}')
    lines.append(f'WORKFLOW_ID = {repr(safe_wf_id)}')
    lines.append(f'TASKS_DEF   = json.loads({repr(tasks_json)})')
    lines.append(f'BACKEND_URL = "http://backend:8000"')
    lines.append("")
    lines.append('default_args = {"owner": "etlflow", "retries": 0}')
    lines.append("")

    # Helper functions
    lines.append('''
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
    return ", ".join(f\'"{c}"\' for c in cols)

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
    import re as _re
    safe_output = _re.sub(r\'[^a-z0-9_]\', \'_\', safe_output)

    # Detect input size
    tbl = INPUT_TABLE if "." in INPUT_TABLE else f"staging.{INPUT_TABLE}"
    sch, tname = tbl.split(".", 1)
    exists = pg.get_first(f"""
        SELECT EXISTS (SELECT FROM information_schema.tables
        WHERE table_schema = \'{sch}\' AND table_name = \'{tname}\')
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
    from pyspark.sql.types import StringType, LongType, DoubleType, BooleanType, DateType, TimestampType

    # Build SparkSession with right-sized resources
    builder = SparkSession.builder \\
        .appName(f"ETLFlow_{DAG_ID}_{task_id}") \\
        .config("spark.master", "spark://spark:7077") \\
        .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar") \\
        .config("spark.executor.memory", spark_cfg["executor_memory"]) \\
        .config("spark.executor.cores", str(spark_cfg["executor_cores"])) \\
        .config("spark.sql.adaptive.enabled", "true") \\
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \\
        .config("spark.sql.broadcastTimeout", "300")

    if spark_cfg.get("dynamic"):
        builder = builder \\
            .config("spark.dynamicAllocation.enabled", "true") \\
            .config("spark.dynamicAllocation.minExecutors", "1") \\
            .config("spark.dynamicAllocation.maxExecutors", str(spark_cfg["num_executors"]))

    spark = builder.getOrCreate()

    # Read from PostgreSQL
    jdbc_url = "jdbc:postgresql://postgres:5432/airflow"
    jdbc_props = {"user": "airflow", "password": "airflow", "driver": "org.postgresql.Driver"}

    # Determine optimal partitions
    num_partitions = max(1, min(8, row_count // 100000))

    df = spark.read.jdbc(
        url=jdbc_url, table=f"(SELECT * FROM {input_table}) AS t",
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

    # Write to warehouse via JDBC
    df.write.jdbc(
        url=jdbc_url,
        table=f"warehouse.{output_name}",
        mode="overwrite",
        properties=jdbc_props
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
        WHERE table_schema = \'staging\' AND table_name LIKE \'_{DAG_ID}_{task_id}_step_%\'
    """)
    for (t,) in temps:
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
                exprs = ", ".join(f\'"{c}" AS "{renames.get(c, c)}"\' for c in cur_cols)
                pg.run(f"CREATE TABLE {tmp} AS SELECT {exprs} FROM {current}")
            elif ntype == "add_const":
                name  = config.get("name", "new_col")
                val   = config.get("value", "NULL")
                dtype = config.get("dtype", "TEXT")
                pg.run(f\'CREATE TABLE {tmp} AS SELECT {all_q}, CAST({repr(val)} AS {dtype}) AS "{name}" FROM {current}\')
            elif ntype == "fill_null":
                fill_cols = config.get("columns", [])
                fill_val  = config.get("fillValue", "")
                exprs_list = []
                for c in cur_cols:
                    if c in fill_cols:
                        exprs_list.append(f\'COALESCE("{c}"::TEXT, {repr(str(fill_val))})::TEXT AS "{c}"\')
                    else:
                        exprs_list.append(f\'"{c}"\')
                pg.run(f"CREATE TABLE {tmp} AS SELECT {', '.join(exprs_list)} FROM {current}")
            elif ntype == "order_table":
                orders = config.get("orders", [])
                oc = ", ".join(f\'"{o["col"]}" {o.get("dir","ASC")}\' for o in orders if o.get("col") in cur_cols) or "1"
                pg.run(f"CREATE TABLE {tmp} AS SELECT {all_q} FROM {current} ORDER BY {oc}")
            elif ntype == "change_type":
                types = config.get("types", {})
                exprs = ", ".join(
                    f\'"{c}"::TEXT::{types[c]} AS "{c}"\' if c in types else f\'"{c}"\'
                    for c in cur_cols
                )
                pg.run(f"CREATE TABLE {tmp} AS SELECT {exprs} FROM {current}")
            elif ntype == "group_agg":
                gcols = [c for c in config.get("groupCols", []) if c in cur_cols]
                acols = config.get("aggCols", [])
                if gcols and acols:
                    g = q(gcols)
                    a = ", ".join(f\'{x["func"]}("{x["col"]}") AS "{x["alias"]}"\' for x in acols)
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
    col_defs = ", ".join(f\'"{c}" {dt}\' for c, dt in final_schema.items())
    pg.run(f"""CREATE TABLE {out} ({col_defs}, date_partition DATE DEFAULT CURRENT_DATE, loaded_at TIMESTAMP DEFAULT NOW())""")
    col_names = q(final_schema.keys())
    pg.run(f"""INSERT INTO {out} ({col_names}, date_partition, loaded_at) SELECT {col_names}, CURRENT_DATE, NOW() FROM {current}""")

    # Cleanup temp tables
    temps2 = pg.get_records(f"""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = \'staging\' AND table_name LIKE \'_{DAG_ID}_{task_id}_step_%\'
    """)
    for (t,) in temps2:
        pg.run(f\'DROP TABLE IF EXISTS staging."{t}"\')

''')

    # Generate task functions and DAG definition
    lines.append(f"""
with DAG(
    dag_id={repr(dag_id)},
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "spark", "generated", {repr(safe_wf_id)}],
    description={repr(description)},
) as dag:
    airflow_tasks = {{}}
    for task_def in TASKS_DEF:
        tid = task_def["task_id"]
        t = PythonOperator(
            task_id=tid,
            python_callable=run_task,
            op_kwargs={{"task_def": task_def}},
        )
        airflow_tasks[tid] = t

    # Set up task dependencies (multi-branch)
    for task_def in TASKS_DEF:
        tid = task_def["task_id"]
        for dep_tid in task_def.get("depends_on", []):
            if dep_tid in airflow_tasks:
                airflow_tasks[dep_tid] >> airflow_tasks[tid]
""")

    return "\n".join(lines)