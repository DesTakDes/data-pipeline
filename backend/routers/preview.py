"""
routers.preview
──────────────────
Pure HTTP layer. This file's only job: parse the request, call PreviewEngine,
map domain exceptions to HTTP status codes, return the response. No graph
algorithm, no Spark call, no SQL lives here.
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from preview.engine import PreviewEngine
from preview.validator import ValidationError
from core.graph_resolver import NodeNotFoundError, GraphCycleError
from core.db import get_conn   # existing get_conn() from main.py, moved to core/db.py

router = APIRouter()
engine = PreviewEngine(pg_conn_factory=get_conn)


class PreviewRequest(BaseModel):
    nodes: list[dict]
    edges: list[dict]
    target_node_id: str
    limit: int = Field(default=100, ge=1, le=5000)


@router.post("/spark-pipeline")
def preview_spark_pipeline(payload: PreviewRequest):
    """
    Replaces the old `/api/preview/spark-pipeline` endpoint. Runs ONLY the
    ancestor subgraph of `target_node_id` — nodes downstream of the target
    are never touched, never executed, never even loaded into the plan.
    """
    try:
        result = engine.run_preview(
            nodes=payload.nodes,
            edges=payload.edges,
            target_node_id=payload.target_node_id,
            limit=payload.limit,
        )
        return result.to_json()
    except ValidationError as e:
        # 422 Unprocessable Entity: the graph itself is invalid — caught
        # BEFORE any Spark job was submitted (see GraphValidator).
        raise HTTPException(422, detail={"validation_errors": e.errors})
    except NodeNotFoundError as e:
        raise HTTPException(404, detail=str(e))
    except GraphCycleError as e:
        raise HTTPException(400, detail=str(e))
    except Exception as e:
        raise HTTPException(500, detail=f"Preview failed: {e}")