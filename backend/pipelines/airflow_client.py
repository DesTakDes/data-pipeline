"""
pipelines.airflow_client
───────────────────────────
Thin wrapper around Airflow's stable REST API (`/api/v1/...`). This is the
ONLY module in the backend allowed to know Airflow's HTTP contract — every
other module (service.py, routers/airflow.py, routers/pipelines.py) calls
these functions instead of building requests itself.

Preview never imports this module. Preview and Airflow are two completely
separate execution lifecycles by design (see architecture doc, section 2 /
preview/spark_session_pool.py docstring).
"""
import requests
from requests.auth import HTTPBasicAuth
from core.config import AIRFLOW_BASE_URL, AIRFLOW_USER, AIRFLOW_PASSWORD

_AUTH = HTTPBasicAuth(AIRFLOW_USER, AIRFLOW_PASSWORD)
_TIMEOUT = 15


class AirflowClientError(Exception):
    pass


def _request(method: str, path: str, **kwargs) -> dict:
    url = f"{AIRFLOW_BASE_URL}/api/v1{path}"
    try:
        resp = requests.request(method, url, auth=_AUTH, timeout=_TIMEOUT, **kwargs)
    except requests.RequestException as e:
        raise AirflowClientError(f"Could not reach Airflow at {url}: {e}") from e

    if resp.status_code >= 400:
        raise AirflowClientError(f"Airflow API {method} {path} -> {resp.status_code}: {resp.text[:400]}")
    return resp.json() if resp.content else {}


def unpause_dag(dag_id: str) -> dict:
    """New DAGs are created paused by Airflow's file-scan by default; must be
    unpaused before the first trigger or the run will just sit queued."""
    return _request("PATCH", f"/dags/{dag_id}", json={"is_paused": False})


def trigger_dag(dag_id: str, conf: dict, dag_run_id: str | None = None) -> dict:
    """Fires a single DAG run. `conf` typically carries {"run_ids": [...]}."""
    body = {"conf": conf}
    if dag_run_id:
        body["dag_run_id"] = dag_run_id
    return _request("POST", f"/dags/{dag_id}/dagRuns", json=body)


def get_dag_run(dag_id: str, dag_run_id: str) -> dict:
    return _request("GET", f"/dags/{dag_id}/dagRuns/{dag_run_id}")


def list_dag_runs(dag_id: str, limit: int = 25) -> list[dict]:
    data = _request("GET", f"/dags/{dag_id}/dagRuns", params={"limit": limit, "order_by": "-execution_date"})
    return data.get("dag_runs", [])


def get_task_instances(dag_id: str, dag_run_id: str) -> list[dict]:
    data = _request("GET", f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances")
    return data.get("task_instances", [])


def dag_exists(dag_id: str) -> bool:
    try:
        _request("GET", f"/dags/{dag_id}")
        return True
    except AirflowClientError:
        return False