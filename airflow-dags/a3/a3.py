# Auto-generated WORKFLOW DAG
# Workflow  : a3
# DAG ID    : a3
# Generated : 2026-07-23T21:10:20.561231
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
sys.path.insert(0, os.path.dirname(__file__))


# ── Import task modules ───────────────────────────────────────────────────────
from tasks.task_plsbisa import run as run_plsbisa

DAG_ID      = 'a3'
WORKFLOW_ID = 'wf_1784839138145_96ke'
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
    tags             = ["etl", "workflow", 'wf_1784839138145_96ke'],
    description      = '',
) as dag:

    airflow_tasks = {}

    # ── Register tasks ────────────────────────────────────────────────────────

    plsbisa_op = PythonOperator(
        task_id             = 'plsbisa',
        python_callable     = lambda **ctx: run_plsbisa(
            run_ids     = (ctx.get('dag_run').conf or {}).get('run_ids', []),
            backend_url = BACKEND_URL,
        ),
        on_failure_callback = _on_failure,
        execution_timeout   = timedelta(minutes=90),
    )
    airflow_tasks['plsbisa'] = plsbisa_op

    # ── Wire dependencies ─────────────────────────────────────────────────────
    pass  # no inter-task dependencies
