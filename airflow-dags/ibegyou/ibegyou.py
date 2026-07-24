# Auto-generated WORKFLOW SPARK DAG
# Workflow  : ibegyou
# DAG ID    : ibegyou
# Generated : 2026-07-23T12:23:44.071108
# ─────────────────────────────────────────────────────────────────────────────

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests

from tasks.task_task_3860 import run as run_task_3860

DAG_ID      = 'ibegyou'
WORKFLOW_ID = 'wf_1784809373412_ikr9'
BACKEND_URL = "http://backend:8000"

default_args = {
    'owner'            : 'etlflow',
    'retries'          : 1,
    'retry_delay'      : timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=90),
}

def _on_failure(context):
    conf    = context.get("dag_run").conf or {}
    run_ids = conf.get("run_ids", [])
    err     = str(context.get("exception", "Spark Execution Failed"))[:400]
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
    tags             = ["etl", "spark", 'wf_1784809373412_ikr9'],
    description      = '',
) as dag:

    airflow_tasks = {}

    task_3860_op = PythonOperator(
        task_id             = 'task_3860',
        python_callable     = lambda **ctx: run_task_3860(
            run_ids     = (ctx.get('dag_run').conf or {}).get('run_ids', []),
            backend_url = BACKEND_URL,
        ),
        on_failure_callback = _on_failure,
        execution_timeout   = timedelta(minutes=90),
    )
    airflow_tasks['task_3860'] = task_3860_op
    pass  # no inter-task dependencies
