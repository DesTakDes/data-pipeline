"""
routers.pipelines
─────────────────────
Pure HTTP layer for "Run Pipeline". Business logic (DAG generation, file
writes, Airflow trigger) lives in pipelines/service.py — this file only
parses requests, maps exceptions to status codes, and also exposes the
PATCH endpoint that the generated task's ProgressReporter calls back into.
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from core.db import get_conn
from pipelines.service import PipelineRunService, PipelineRunError

router = APIRouter()
service = PipelineRunService(pg_conn_factory=get_conn)


class TaskSpec(BaseModel):
    task_id: str
    output_name: str = "output"
    transforms: list[dict] = []
    input_table: str | None = None
    depends_on: list[str] = []


class RunPipelineRequest(BaseModel):
    workflow_id: str
    workflow_name: str
    input_table: str
    tasks: list[TaskSpec]
    description: str = ""
    execution_timeout_minutes: int = 90


class ProgressUpdate(BaseModel):
    status: str
    message: str | None = None


@router.post("/pipelines/run")
def run_pipeline(payload: RunPipelineRequest):
    try:
        return service.run_workflow(
            workflow_id=payload.workflow_id,
            workflow_name=payload.workflow_name,
            input_table=payload.input_table,
            tasks=[t.model_dump() for t in payload.tasks],
            description=payload.description,
            execution_timeout_minutes=payload.execution_timeout_minutes,
        )
    except PipelineRunError as e:
        raise HTTPException(400, detail=str(e))
    except Exception as e:
        raise HTTPException(500, detail=f"Run Pipeline failed: {e}")


@router.get("/pipelines/runs/{run_id}")
def get_run_status(run_id: str):
    try:
        return service.get_run_status(run_id)
    except PipelineRunError as e:
        raise HTTPException(404, detail=str(e))


@router.get("/pipelines/runs")
def list_runs(workflow_id: str | None = None, limit: int = 50):
    return service.list_runs(workflow_id=workflow_id, limit=limit)


@router.patch("/pipelines/runs/{run_id}")
def patch_run_progress(run_id: str, payload: ProgressUpdate):
    """
    Called by the generated Airflow task's ProgressReporter (see
    pipelines/dag_generator.py's task template and pipeline_runtime.progress)
    as the Spark/DuckDB/Postgres job runs, so the frontend can poll live
    status without talking to Airflow directly.
    """
    service.report_progress(run_id, status=payload.status, message=payload.message)
    return {"ok": True}