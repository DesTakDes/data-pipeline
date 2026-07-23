"""
pipelines.dag_generator
──────────────────────────
Generates two kinds of files for a workflow, written to DAGS_FOLDER:

    {dag_id}/{dag_id}.py          ← workflow orchestrator (PythonOperator wiring only)
    {dag_id}/tasks/task_{id}.py   ← thin task file: imports pipeline_runtime +
                                      transform_lib, contains ZERO transform logic

This replaces the ~700-line string-template transform logic that used to be
embedded directly inside generate_task_file() in main.py. All 15 transform
types now live in exactly one place (transform_lib), imported identically
by this generated file and by preview/spark_executor.py.

Both `transform_lib` and `pipeline_runtime` must be installed in the Airflow
worker image (see architecture doc, section "distribusi transform_lib").
`pipeline_runtime` does NOT need to be installed in the backend/preview
image — the backend only ever writes it as a string here, never imports it.
"""
import json
import re
from datetime import datetime
from pathlib import Path


# ════════════════════════════════════════════════════════════════════════════
# TASK FILE — thin wrapper
# ════════════════════════════════════════════════════════════════════════════

_TASK_TEMPLATE = '''# Auto-generated TASK file — DO NOT EDIT BY HAND
# Task ID   : TASK_ID_PLACEHOLDER
# DAG ID    : DAG_ID_PLACEHOLDER
# Output    : warehouse.OUTPUT_NAME_PLACEHOLDER
# Generated : NOW_PLACEHOLDER
# ─────────────────────────────────────────────────────────────────────────────
# ALL transform logic lives in transform_lib (shared with the Preview Engine).
# ALL I/O orchestration (chunked read, progress, parquet, batch insert) lives
# in pipeline_runtime. This file only wires the two together for one task.
# ─────────────────────────────────────────────────────────────────────────────
import json
import psycopg2

from pipeline_runtime import (
    ProgressReporter, choose_engine, estimate_table_mb,
    run_duckdb_task, run_postgres_task, run_spark_task,
)

TASK_ID     = 'TASK_ID_PLACEHOLDER'
DAG_ID      = 'DAG_ID_PLACEHOLDER'
INPUT_TABLE = 'INPUT_TABLE_PLACEHOLDER'
OUTPUT_NAME = 'OUTPUT_NAME_PLACEHOLDER'
TRANSFORMS  = json.loads(TRANSFORMS_JSON_PLACEHOLDER)
PARQUET_DIR = "/data_csv/parquet"
BACKEND_URL = "http://backend:8000"

PG_CONFIG = {
    'host': 'PG_HOST_PLACEHOLDER', 'port': PG_PORT_PLACEHOLDER,
    'database': 'PG_DB_PLACEHOLDER',
    'user': 'PG_USER_PLACEHOLDER', 'password': 'PG_PASS_PLACEHOLDER',
}


def _pg_conn_factory():
    return psycopg2.connect(**PG_CONFIG)


def run(run_ids, backend_url=BACKEND_URL):
    """Entry point called by the workflow DAG's PythonOperator."""
    tbl = INPUT_TABLE if "." in INPUT_TABLE else f"staging.{INPUT_TABLE}"
    safe_out = __import__("re").sub(r"[^a-z0-9_]", "_", OUTPUT_NAME.lower()).strip("_") or "output"

    conn = _pg_conn_factory()
    size_mb = estimate_table_mb(conn, tbl)
    conn.close()

    engine = choose_engine(size_mb)
    progress = ProgressReporter(run_ids, backend_url)
    print(f"[Task:{TASK_ID}] {size_mb:.1f}MB | engine={engine}")

    if engine == "duckdb":
        result = run_duckdb_task(
            pg_config=PG_CONFIG, input_table=tbl, output_name=safe_out,
            transforms=TRANSFORMS, task_id=TASK_ID, parquet_dir=PARQUET_DIR,
            progress=progress,
        )
        rows = result.get("rows", 0)

    elif engine == "spark":
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        row_count = pg_hook.get_first(f"SELECT COUNT(*) FROM {tbl}")[0]
        progress.update(10, f"Starting Spark job ({row_count:,} rows)...", force=True)
        run_spark_task(
            input_table=tbl, output_name=safe_out, transforms=TRANSFORMS,
            row_count=row_count, task_id=TASK_ID, dag_id=DAG_ID,
            parquet_dir=PARQUET_DIR, pg_conn_factory=_pg_conn_factory,
        )
        rows = pg_hook.get_first(f'SELECT COUNT(*) FROM warehouse."{safe_out}"')[0]

    else:  # postgres
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        progress.update(10, "Running native Postgres transform...", force=True)
        result = run_postgres_task(
            pg_hook=pg_hook, input_table=tbl, output_name=safe_out,
            transforms=TRANSFORMS, task_id=TASK_ID,
        )
        rows = result.get("rows", 0)

    progress.finish_success(rows, engine)
    print(f"[Task:{TASK_ID}] Done -> warehouse.{safe_out} ({rows:,} rows via {engine})")
    return rows
'''


def generate_task_file(
    task_id: str,
    dag_id: str,
    workflow_id: str,
    input_table: str,
    output_name: str,
    transforms: list,
    execution_timeout_minutes: int = 90,
    pg_config: dict | None = None,
) -> str:
    """Generate a thin task file — no transform logic, only wiring."""
    from core.config import PG_CONFIG as DEFAULT_PG_CONFIG
    pg_config = pg_config or DEFAULT_PG_CONFIG

    tasks_json = json.dumps(transforms, ensure_ascii=True)
    safe_input = re.sub(r'[^a-zA-Z0-9_.]', '', input_table)
    safe_out = re.sub(r'[^a-z0-9_]', '_', output_name.lower()).strip('_') or "output"
    now_str = datetime.now().isoformat()

    code = _TASK_TEMPLATE
    code = code.replace("TASK_ID_PLACEHOLDER", task_id)
    code = code.replace("DAG_ID_PLACEHOLDER", dag_id)
    code = code.replace("NOW_PLACEHOLDER", now_str)
    code = code.replace("INPUT_TABLE_PLACEHOLDER", safe_input)
    code = code.replace("OUTPUT_NAME_PLACEHOLDER", safe_out)
    code = code.replace("TRANSFORMS_JSON_PLACEHOLDER", repr(tasks_json))
    code = code.replace("PG_HOST_PLACEHOLDER", pg_config["host"])
    code = code.replace("PG_PORT_PLACEHOLDER", str(pg_config["port"]))
    code = code.replace("PG_DB_PLACEHOLDER", pg_config["database"])
    code = code.replace("PG_USER_PLACEHOLDER", pg_config["user"])
    code = code.replace("PG_PASS_PLACEHOLDER", pg_config["password"])
    return code


# ════════════════════════════════════════════════════════════════════════════
# WORKFLOW DAG — orchestrator only (unchanged in spirit from the original;
# it never contained transform logic, only PythonOperator wiring, so it
# needed no refactor beyond moving it into this module).
# ════════════════════════════════════════════════════════════════════════════

def generate_workflow_dag(
    dag_id: str,
    workflow_id: str,
    workflow_name: str,
    tasks: list,
    description: str = "",
    execution_timeout_minutes: int = 90,
) -> str:
    safe_wf_id = workflow_id.replace("'", "")
    safe_name = workflow_name.replace("'", "").replace('"', '')
    now_str = datetime.now().isoformat()

    import_lines, task_defs, dep_lines = [], [], []

    for task in tasks:
        tid = task["task_id"]
        safe_tid = re.sub(r'[^a-z0-9_]', '_', tid.lower()).strip('_') or f"task_{len(import_lines)+1}"
        module_nm = f"task_{safe_tid}"
        import_lines.append(f"from tasks.{module_nm} import run as run_{safe_tid}")
        task_defs.append(f"""
    {safe_tid}_op = PythonOperator(
        task_id             = '{safe_tid}',
        python_callable     = lambda **ctx: run_{safe_tid}(
            run_ids     = (ctx.get('dag_run').conf or {{}}).get('run_ids', []),
            backend_url = BACKEND_URL,
        ),
        on_failure_callback = _on_failure,
        execution_timeout   = timedelta(minutes={execution_timeout_minutes}),
    )
    airflow_tasks['{safe_tid}'] = {safe_tid}_op""")

    for task in tasks:
        tid = task["task_id"]
        safe_tid = re.sub(r'[^a-z0-9_]', '_', tid.lower()).strip('_')
        for dep in task.get("depends_on", []):
            safe_dep = re.sub(r'[^a-z0-9_]', '_', dep.lower()).strip('_')
            dep_lines.append(f"    airflow_tasks['{safe_dep}'] >> airflow_tasks['{safe_tid}']")

    imports_block = "\n".join(import_lines)
    tasks_block = "\n".join(task_defs)
    deps_block = "\n".join(dep_lines) if dep_lines else "    pass  # no inter-task dependencies"

    return f'''# Auto-generated WORKFLOW DAG
# Workflow  : {safe_name}
# DAG ID    : {dag_id}
# Generated : {now_str}
# ─────────────────────────────────────────────────────────────────────────────
# ORCHESTRATOR ONLY. Imports task modules from tasks/ and wires them together.
# Transform logic lives in transform_lib; I/O orchestration in pipeline_runtime.
# ─────────────────────────────────────────────────────────────────────────────

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests

{imports_block}

DAG_ID      = {repr(dag_id)}
WORKFLOW_ID = {repr(safe_wf_id)}
BACKEND_URL = "http://backend:8000"

default_args = {{
    'owner'            : 'etlflow',
    'retries'          : 2,
    'retry_delay'      : timedelta(minutes=3),
    'execution_timeout': timedelta(minutes={execution_timeout_minutes}),
}}


def _on_failure(context):
    conf    = context.get("dag_run").conf or {{}}
    run_ids = conf.get("run_ids", [])
    err     = str(context.get("exception", "Unknown"))[:400]
    for run_id in run_ids:
        try:
            requests.patch(
                f"{{BACKEND_URL}}/api/pipelines/runs/{{run_id}}",
                json={{"status": "failed", "message": err}},
                timeout=5,
            )
        except Exception:
            pass


with DAG(
    dag_id           = DAG_ID,
    default_args     = default_args,
    schedule_interval= None,
    start_date       = datetime(2024, 1, 1),
    catchup          = False,
    max_active_tasks = 4,
    tags             = ["etl", "workflow", {repr(safe_wf_id[:50])}],
    description      = {repr(description)},
) as dag:

    airflow_tasks = {{}}

{tasks_block}

{deps_block}
'''


# ════════════════════════════════════════════════════════════════════════════
# PUBLIC ENTRYPOINT — backward-compatible signature/return type
# ════════════════════════════════════════════════════════════════════════════

def generate_dag(
    dag_id: str,
    workflow_id: str,
    workflow_name: str,
    input_table: str,
    tasks: list,
    description: str = "",
    execution_timeout_minutes: int = 90,
) -> tuple[str, list[tuple[str, str]], dict]:
    """Returns (dag_file_content, [(task_filename, task_file_content), ...], task_outputs)."""
    dag_content = generate_workflow_dag(
        dag_id=dag_id, workflow_id=workflow_id, workflow_name=workflow_name,
        tasks=tasks, description=description,
        execution_timeout_minutes=execution_timeout_minutes,
    )

    task_outputs = {}
    for t in tasks:
        tid = t["task_id"]
        out_name = t.get("output_name", "output")
        safe_out = re.sub(r'[^a-z0-9_]', '_', out_name.lower()).strip('_') or f"out_{tid}"
        if safe_out and safe_out[0].isdigit():
            safe_out = 't_' + safe_out
        task_outputs[tid] = f"warehouse.{safe_out}"

    task_files = []
    for task in tasks:
        task_id = re.sub(r'[^a-z0-9_]', '_', task["task_id"].lower()).strip('_')
        depends_on = task.get("depends_on", [])

        if task.get("input_table"):
            raw_task_input = task["input_table"]
        elif depends_on:
            raw_task_input = task_outputs.get(depends_on[0], input_table)
        else:
            raw_task_input = input_table

        safe_task_input = re.sub(r'[^a-zA-Z0-9_.]', '', raw_task_input) if raw_task_input else ""

        task_code = generate_task_file(
            task_id=task_id, dag_id=dag_id, workflow_id=workflow_id,
            input_table=safe_task_input, output_name=task.get("output_name", "output"),
            transforms=task.get("transforms", []),
            execution_timeout_minutes=execution_timeout_minutes,
        )
        task_files.append((f"task_{task_id}.py", task_code))

    return dag_content, task_files, task_outputs


# Backward-compatible alias (existing callers in main.py used this name)
generate_spark_dag = generate_dag


def write_workflow_files(dags_folder: str, dag_id: str, dag_content: str,
                          task_files: list[tuple[str, str]]) -> dict:
    """
    Writes the generated files to disk under DAGS_FOLDER/{dag_id}/, with the
    /tmp fallback behavior preserved from the original main.py endpoint.
    Separated out so routers/pipelines.py doesn't need to know about paths.
    """
    workflow_dir = Path(dags_folder) / dag_id
    tasks_dir = workflow_dir / "tasks"

    written_task_files = []
    try:
        workflow_dir.mkdir(parents=True, exist_ok=True)
        tasks_dir.mkdir(parents=True, exist_ok=True)
        init_file = tasks_dir / "__init__.py"
        if not init_file.exists():
            init_file.write_text("# Auto-generated — task modules package\n")

        for filename, content in task_files:
            path = tasks_dir / filename
            path.write_text(content, encoding="utf-8")
            written_task_files.append(str(path))

        dag_path = workflow_dir / f"{dag_id}.py"
        dag_path.write_text(dag_content, encoding="utf-8")
        return {"workflow_folder": str(workflow_dir), "dag_file": str(dag_path),
                "task_files": written_task_files, "fallback": False}

    except Exception as e:
        fallback_dir = Path("/tmp") / dag_id
        fallback_tasks = fallback_dir / "tasks"
        fallback_tasks.mkdir(parents=True, exist_ok=True)
        (fallback_tasks / "__init__.py").write_text("# Auto-generated\n")
        for filename, content in task_files:
            (fallback_tasks / filename).write_text(content, encoding="utf-8")
        fallback_dag = fallback_dir / f"{dag_id}.py"
        fallback_dag.write_text(dag_content, encoding="utf-8")
        print(f"[DAG] Primary write failed ({e}), used fallback: {fallback_dag}")
        return {"workflow_folder": str(fallback_dir), "dag_file": str(fallback_dag),
                "task_files": [str(fallback_tasks / fn) for fn, _ in task_files], "fallback": True}