"""
preview.engine
─────────────────
Orchestrates the 7-stage Preview pipeline:
  Receive Graph -> Validate Graph -> Resolve Dependency -> Build Execution
  Plan -> Execute Spark Transformation -> Collect Preview Result -> Return JSON

This is the ONLY module that talks to all three "pure" layers at once
(validator, resolver, executor). Nothing here should contain SQL or Spark
API calls directly — those live in their respective modules.
"""
import time
from core.graph_resolver import DependencyResolver, GraphCycleError, NodeNotFoundError
from .validator import GraphValidator, ValidationError
from .spark_session_pool import get_or_create_session
from .spark_executor import SparkNodeExecutor
from .node_cache import default_cache
from .dto import PreviewResult


class PreviewEngine:
    def __init__(self, pg_conn_factory):
        self.pg_conn_factory = pg_conn_factory

    def run_preview(self, nodes: list[dict], edges: list[dict],
                    target_node_id: str, limit: int = 100) -> PreviewResult:
        t0 = time.time()

        # 1. Receive Graph — nodes/edges/target_node_id already arrive as plain args,
        #    parsed by the FastAPI Pydantic model in routers/preview.py.

        # 2. Validate Graph — fail fast, before Spark is ever touched.
        validator = GraphValidator(nodes, edges, self.pg_conn_factory)
        validation = validator.validate(target_node_id)
        if validation.errors:
            raise ValidationError(validation.errors)
        warnings = list(validation.warnings)

        # 3. Resolve Dependency — ancestor closure + topological sort only.
        resolver = DependencyResolver(nodes, edges)
        try:
            required_ids = resolver.ancestor_closure(target_node_id)
            ordered_ids = resolver.topological_order(required_ids)
        except (GraphCycleError, NodeNotFoundError) as e:
            raise ValidationError([str(e)])
        ordered_nodes = [resolver.nodes_by_id[nid] for nid in ordered_ids]

        # 4. Build Execution Plan — decide which nodes are cache candidates.
        fanout_ids = resolver.fanout_node_ids(required_ids)

        # 5. Execute Spark Transformation
        spark = get_or_create_session()
        executor = SparkNodeExecutor(spark, self.pg_conn_factory, node_cache=default_cache)
        df, cached_nodes, node_timings, logical_plan, physical_plan = executor.execute(
            ordered_nodes, fanout_ids=fanout_ids, limit=limit
        )

        # 6. Collect Preview Result
        pdf = df.limit(limit).toPandas()
        pdf = pdf.where(pdf.notnull(), None)
        schema_info = {f.name: str(f.dataType) for f in df.schema.fields}
        elapsed = round(time.time() - t0, 3)

        # 7. Return JSON (as a typed DTO — routers/preview.py calls .to_json())
        return PreviewResult(
            columns=list(pdf.columns),
            rows=pdf.to_dict(orient="records"),
            schema=schema_info,
            execution_time=f"{elapsed}s",
            warnings=warnings,
            cached_nodes=cached_nodes,
            logical_plan=logical_plan,
            physical_plan=physical_plan,
            engine_used="spark",
            node_timings=node_timings,
            # Preview always reads a capped sample from the source (see
            # SparkNodeExecutor._read_input_node), so results are never a
            # full-dataset guarantee — flagged explicitly for the frontend.
            is_sampled=True,
        )