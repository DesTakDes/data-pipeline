"""
pipelines.service
─────────────────────
PipelineRunService: the business logic behind POST /api/pipelines/run.
This is the "Run Pipeline" counterpart to preview.engine.PreviewEngine —
same graph in, but here it's compiled to an Airflow DAG + task files and
handed off to a completely separate execution lifecycle (Airflow), instead
of being executed in-process against the shared PreviewEngine SparkSession.

Flow:
  1. Generate DAG + task files (dag_generator, reusing transform_lib via the
     generated task file's import — never duplicating transform logic here).
  2. Write them to DAGS_FOLDER (or /tmp fallback).
  3. Record a meta.pipeline_runs row so the frontend can poll status.
  4. Unpause + trigger the DAG in Airflow, passing run_ids in `conf` so the
     generated task's ProgressReporter can PATCH this backend as it runs.
"""
import re
from datetime import datetime

from core.config import DAGS_FOLDER, BACKEND_URL, DEFAULT_TASK_TIMEOUT_MINUTES
from . import airflow_client
from .dag_generator import generate_dag, write_workflow_files
from .repository import PipelineRunRepository


class PipelineRunError(Exception):
    pass


def _safe_dag_id(workflow_id: str, workflow_name: str) -> str:
    base = re.sub(r"[^a-zA-Z0-9_]", "_", workflow_name or workflow_id).strip("_").lower()
    suffix = re.sub(r"[^a-zA-Z0-9]", "", workflow_id)[:8]
    return f"wf_{base}_{suffix}"[:200] or f"wf_{suffix}"


class PipelineRunService:
    def __init__(self, pg_conn_factory, dags_folder: str = DAGS_FOLDER):
        self.repo = PipelineRunRepository(pg_conn_factory)
        self.dags_folder = dags_folder

    def run_workflow(
        self,
        workflow_id: str,
        workflow_name: str,
        input_table: str,
        tasks: list[dict],
        description: str = "",
        execution_timeout_minutes: int = DEFAULT_TASK_TIMEOUT_MINUTES,
        triggered_by: str | None = None,
    ) -> dict:
        if not tasks:
            raise PipelineRunError("Workflow has no tasks to run")

        dag_id = _safe_dag_id(workflow_id, workflow_name)

        dag_content, task_files, task_outputs = generate_dag(
            dag_id=dag_id,
            workflow_id=workflow_id,
            workflow_name=workflow_name,
            input_table=input_table,
            tasks=tasks,
            description=description,
            execution_timeout_minutes=execution_timeout_minutes,
        )

        write_result = write_workflow_files(self.dags_folder, dag_id, dag_content, task_files)

        run_id = self.repo.create_run(workflow_id=workflow_id, dag_id=dag_id, triggered_by=triggered_by)

        try:
            # Airflow's DAG-file scanner runs on an interval; a brand-new DAG
            # may not be registered yet on the very first trigger. unpause_dag
            # will raise AirflowClientError in that race — surfaced as 'queued'
            # rather than failing the whole request, since the DAG usually
            # appears within a few seconds and the run stays pollable.
            airflow_client.unpause_dag(dag_id)
            airflow_client.trigger_dag(dag_id, conf={"run_ids": [run_id]}, dag_run_id=f"manual__{run_id}")
            self.repo.update_status(run_id, "running", message="Triggered in Airflow")
        except airflow_client.AirflowClientError as e:
            self.repo.update_status(run_id, "queued", message=f"Waiting for Airflow: {e}")

        return {
            "run_id": run_id,
            "dag_id": dag_id,
            "workflow_id": workflow_id,
            "status": "running",
            "task_outputs": task_outputs,
            "dag_file": write_result["dag_file"],
            "fallback_write": write_result["fallback"],
        }

    def get_run_status(self, run_id: str) -> dict:
        run = self.repo.get_run(run_id)
        if not run:
            raise PipelineRunError(f"Run '{run_id}' not found")

        # Cross-check live Airflow state when possible — the meta row is
        # updated by the task's ProgressReporter, but Airflow is still the
        # source of truth for e.g. a task that crashed before it could PATCH.
        try:
            live = airflow_client.get_dag_run(run["dag_id"], f"manual__{run_id}")
            run["airflow_state"] = live.get("state")
        except airflow_client.AirflowClientError:
            run["airflow_state"] = None
        return run

    def list_runs(self, workflow_id: str | None = None, limit: int = 50) -> list[dict]:
        return self.repo.list_runs(workflow_id=workflow_id, limit=limit)

    def report_progress(self, run_id: str, status: str, message: str | None = None) -> None:
        """Called by the generated task's ProgressReporter via
        PATCH /api/pipelines/runs/{run_id} (see routers/pipelines.py)."""
        finished = status in ("success", "failed")
        self.repo.update_status(run_id, status, message=message, finished=finished)