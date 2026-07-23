"""
routers.airflow
───────────────────
Thin passthrough endpoints for frontend widgets that want raw Airflow state
(DAG run history, task instance timeline) without going through the
pipelines/ run-tracking abstraction. Everything here delegates straight to
pipelines.airflow_client — no business logic.
"""
from fastapi import APIRouter, HTTPException
from pipelines import airflow_client

router = APIRouter()


@router.get("/airflow/dags/{dag_id}/runs")
def dag_runs(dag_id: str, limit: int = 25):
    try:
        return airflow_client.list_dag_runs(dag_id, limit=limit)
    except airflow_client.AirflowClientError as e:
        raise HTTPException(502, detail=str(e))


@router.get("/airflow/dags/{dag_id}/runs/{dag_run_id}")
def dag_run_detail(dag_id: str, dag_run_id: str):
    try:
        return airflow_client.get_dag_run(dag_id, dag_run_id)
    except airflow_client.AirflowClientError as e:
        raise HTTPException(404, detail=str(e))


@router.get("/airflow/dags/{dag_id}/runs/{dag_run_id}/tasks")
def dag_run_tasks(dag_id: str, dag_run_id: str):
    try:
        return airflow_client.get_task_instances(dag_id, dag_run_id)
    except airflow_client.AirflowClientError as e:
        raise HTTPException(404, detail=str(e))


@router.post("/airflow/dags/{dag_id}/unpause")
def unpause(dag_id: str):
    try:
        return airflow_client.unpause_dag(dag_id)
    except airflow_client.AirflowClientError as e:
        raise HTTPException(502, detail=str(e))


@router.get("/airflow/dags/{dag_id}/exists")
def exists(dag_id: str):
    return {"dag_id": dag_id, "exists": airflow_client.dag_exists(dag_id)}