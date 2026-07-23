"""
preview.spark_executor
─────────────────────────
THE integration point of the whole Preview architecture: turns an ordered
list of resolved nodes into an executed Spark DataFrame, by delegating the
actual per-node transform logic to `transform_lib.SparkCompiler` (the SAME
compiler class the generated Airflow Spark task uses — see architecture
doc section 3).

This module owns:
  - reading the root Input Dataset node via JDBC with limit/column pushdown
  - feeding each subsequent node's transform into SparkCompiler
  - caching DataFrames for fan-out nodes (node_cache.py)
  - collecting per-node timings + logical/physical plan text for PreviewResult
It does NOT own: transform semantics (that's transform_lib) or graph
ordering (that's core.graph_resolver) or HTTP (that's routers/preview.py).
"""
import time
from transform_lib import TransformStep, SparkCompiler
from .node_cache import NodeResultCache


class SparkNodeExecutor:
    def __init__(self, spark, pg_conn_factory, node_cache: NodeResultCache | None = None):
        self.spark = spark
        self.pg_conn_factory = pg_conn_factory
        self.node_cache = node_cache or NodeResultCache()

    # ── JDBC read helpers (limit / column / predicate pushdown) ───────────
    def _read_input_node(self, node: dict, limit: int | None):
        config = node.get("data", {}).get("config", {}) or {}
        dataset = config.get("dataset") or {}
        table = dataset.get("table_name")
        if not table:
            raise ValueError(f"Input node {node['id']} has no table configured")

        # LIMIT pushdown: cap rows AT THE SOURCE, not after loading into Spark.
        # Column pruning is applied lazily — Spark's JDBC source pushes down
        # `.select()` projections automatically when they immediately follow read().
        limit_sql = f" LIMIT {limit}" if limit else ""
        wrapped = f"(SELECT * FROM {table}{limit_sql}) AS t"

        return (
            self.spark.read.format("jdbc")
            .option("url", "jdbc:postgresql://postgres:5432/airflow")
            .option("dbtable", wrapped)
            .option("user", "airflow").option("password", "airflow")
            .option("driver", "org.postgresql.Driver")
            .load()
        )

    def _read_right_table(self, table_name: str, sample_limit: int = 5000):
        """Injected into SparkCompiler as `get_right_df` — always sampled for preview,
        since join right-sides only need to be representative, not complete."""
        wrapped = f"(SELECT * FROM {table_name} LIMIT {sample_limit}) AS t"
        return (
            self.spark.read.format("jdbc")
            .option("url", "jdbc:postgresql://postgres:5432/airflow")
            .option("dbtable", wrapped)
            .option("user", "airflow").option("password", "airflow")
            .option("driver", "org.postgresql.Driver")
            .load()
        )

    def _estimate_mb(self, table_name: str) -> float:
        conn = self.pg_conn_factory()
        try:
            cur = conn.cursor()
            cur.execute("SELECT pg_total_relation_size(%s) / 1024.0 / 1024.0", (table_name,))
            return float(cur.fetchone()[0] or 0)
        except Exception:
            return 9999.0
        finally:
            conn.close()

    # ── Main entrypoint used by PreviewEngine ──────────────────────────────
    def execute(self, ordered_nodes: list[dict], fanout_ids: set[str], limit: int = 100):
        """
        ordered_nodes: topologically-sorted nodes from DependencyResolver
        fanout_ids:    node ids that have >1 child in the resolved subgraph
                       (cache candidates — see architecture doc section 6)
        Returns: (final_df, cached_node_ids, node_timings, logical_plan_text, physical_plan_text)
        """
        compiler = SparkCompiler(
            spark=self.spark,
            get_right_df=self._read_right_table,
            estimate_mb_fn=self._estimate_mb,
            broadcast_max_mb=500,   # preview is more permissive than the 200MB used at full-run time
        )

        df = None
        node_timings: dict[str, float] = {}
        cached_node_ids: list[str] = []
        # tracks the ancestor config chain up to each node, for cache-key hashing
        config_chain: list[dict] = []

        for node in ordered_nodes:
            node_id = node["id"]
            ntype = node.get("data", {}).get("type", "")
            config = node.get("data", {}).get("config", {}) or {}
            config_chain.append({"id": node_id, "type": ntype, "config": config})

            t0 = time.time()

            if ntype == "input_dataset":
                cache_key = NodeResultCache.make_key(node_id, config_chain)
                cached = self.node_cache.get(cache_key)
                if cached is not None:
                    df = cached
                    cached_node_ids.append(node_id)
                else:
                    df = self._read_input_node(node, limit=max(limit * 50, 5000))
                    if node_id in fanout_ids:
                        df = df.cache()
                        self.node_cache.put(cache_key, df)
            else:
                step = TransformStep(type=ntype, config=config)
                df = compiler.apply(df, step)
                if node_id in fanout_ids:
                    cache_key = NodeResultCache.make_key(node_id, config_chain)
                    df = df.cache()
                    self.node_cache.put(cache_key, df)
                    cached_node_ids.append(node_id)

            node_timings[node_id] = round(time.time() - t0, 4)

        if df is None:
            raise ValueError("Execution plan produced no dataframe (empty graph?)")

        logical_plan_text, physical_plan_text = self._extract_plan_text(df)
        return df, cached_node_ids, node_timings, logical_plan_text, physical_plan_text

    @staticmethod
    def _extract_plan_text(df) -> tuple[str, str]:
        """Capture df.explain() output as strings for PreviewResult.logical_plan / physical_plan."""
        try:
            logical = df._jdf.queryExecution().optimizedPlan().toString()
        except Exception:
            logical = ""
        try:
            physical = df._jdf.queryExecution().executedPlan().toString()
        except Exception:
            physical = ""
        return logical, physical