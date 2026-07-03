# Auto-generated WORKFLOW DAG
# Workflow  : cobaterus
# DAG ID    : cobaterus
# Generated : 2026-06-29T05:53:27.254408
# ─────────────────────────────────────────────────────────────────────────────
# This file is the ORCHESTRATOR only.
# It imports task modules from tasks/ and wires them together.
# Transform logic lives in tasks/task_*.py files.
# ─────────────────────────────────────────────────────────────────────────────

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests

# ── Import task modules ───────────────────────────────────────────────────────
from tasks.task_task_8969 import run as run_task_8969
from tasks.task_task_6784 import run as run_task_6784

DAG_ID      = 'cobaterus'
WORKFLOW_ID = 'wf_1782712299552_s13e'
BACKEND_URL = "http://backend:8000"

default_args = {
    'owner'            : 'etlflow',
    'retries'          : 2,
    'retry_delay'      : timedelta(minutes=3),
    'execution_timeout': timedelta(minutes=90),
}


def _on_failure(context):
    conf    = context.get("dag_run").conf or {}
    run_ids = conf.get("run_ids", [])
    err     = str(context.get("exception", "Unknown"))[:400]
    for run_id in run_ids:
        try:
            requests.patch(
                f"{BACKEND_URL}/api/pipelines/runs/{run_id}",
                json={"status": "failed", "message": err},
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
    tags             = ["etl", "workflow", 'wf_1782712299552_s13e'],
    description      = '',
) as dag:

    airflow_tasks = {}

    # ── Register tasks ────────────────────────────────────────────────────────

    task_8969_op = PythonOperator(
        task_id             = 'task_8969',
        python_callable     = lambda **ctx: run_task_8969(
            run_ids     = (ctx.get('dag_run').conf or {}).get('run_ids', []),
            backend_url = BACKEND_URL,
        ),
        on_failure_callback = _on_failure,
        execution_timeout   = timedelta(minutes=90),
    )
    airflow_tasks['task_8969'] = task_8969_op

    task_6784_op = PythonOperator(
        task_id             = 'task_6784',
        python_callable     = lambda **ctx: run_task_6784(
            run_ids     = (ctx.get('dag_run').conf or {}).get('run_ids', []),
            backend_url = BACKEND_URL,
        ),
        on_failure_callback = _on_failure,
        execution_timeout   = timedelta(minutes=90),
    )
    airflow_tasks['task_6784'] = task_6784_op

    # ── Wire dependencies ─────────────────────────────────────────────────────
    pass  # no inter-task dependencies
