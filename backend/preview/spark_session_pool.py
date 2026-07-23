"""
preview.spark_session_pool
─────────────────────────────
Preview needs ONE long-lived SparkSession shared across HTTP requests —
spinning up a new SparkSession per click would cost several seconds of
startup overhead every single time a user opens the Preview panel.

Airflow's Spark task runner (pipelines/) does NOT use this module — each
DAG task run gets its own dedicated `spark-submit` process, isolated from
the backend API process. These are two completely separate Spark lifecycles
by design (see architecture doc, section 2).
"""
import threading

_lock = threading.Lock()
_session = None


def get_or_create_session():
    global _session
    with _lock:
        if _session is None:
            from pyspark.sql import SparkSession
            _session = (
                SparkSession.builder
                .appName("ETLFlow_PreviewEngine")
                .config("spark.master", "spark://spark:7077")
                .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar")
                .config("spark.executor.memory", "2g")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.dynamicAllocation.enabled", "true")
                .config("spark.dynamicAllocation.maxExecutors", "4")
                .getOrCreate()
            )
            _session.sparkContext.setCheckpointDir("/tmp/spark-checkpoints/preview")
        return _session


def stop_session():
    """Used only by graceful shutdown hooks / tests — not called during normal operation."""
    global _session
    with _lock:
        if _session is not None:
            _session.stop()
            _session = None