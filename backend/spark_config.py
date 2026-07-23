import math
import json
from typing import Any, Dict, Optional

_RUNTIME_CONFIG: Dict[str, Any] = {}


def estimate_dataset_profile(
    file_size_bytes: int = 0,
    row_count: int = 0,
    col_count: int = 0,
    sample_rows: int = 0,
) -> Dict[str, Any]:
    size_mb = max(file_size_bytes / (1024 * 1024), 0.0)
    size_gb = max(size_mb / 1024.0, 0.0)
    estimated_memory_mb = max(
        256,
        int(math.ceil(size_mb * 2.5 + max(row_count, sample_rows) / 100_000 * 80 + max(col_count, 1) * 12)),
    )
    ideal_partitions = max(
        8,
        min(
            2048,
            int(math.ceil(max(1.0, size_gb * 24 + max(row_count, 1) / 500_000.0))),
        ),
    )
    return {
        "file_size_bytes": int(file_size_bytes),
        "size_mb": round(size_mb, 2),
        "size_gb": round(size_gb, 2),
        "row_count": int(row_count or 0),
        "col_count": int(col_count or 0),
        "estimated_memory_mb": estimated_memory_mb,
        "ideal_partitions": ideal_partitions,
    }


def recommend_spark_resources(profile: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    prof = profile or estimate_dataset_profile()
    size_mb = float(prof.get("size_mb", 0) or 0)
    size_gb = float(prof.get("size_gb", size_mb / 1024.0) or 0)
    ideal_partitions = int(prof.get("ideal_partitions", 8) or 8)

    if size_mb < 100:
        return {
            "driver_memory": "1g",
            "driver_cores": 1,
            "executor_memory": "1g",
            "executor_cores": 1,
            "num_executors": 1,
            "min_executors": 1,
            "initial_executors": 1,
            "max_executors": 1,
            "shuffle_partitions": max(8, min(64, ideal_partitions)),
            "default_parallelism": max(8, min(64, ideal_partitions)),
            "dynamic_allocation": False,
            "aqe_enabled": True,
            "serializer": "org.apache.spark.serializer.KryoSerializer",
            "broadcast_threshold_mb": 50,
            "memory_fraction": 0.6,
            "storage_fraction": 0.5,
        }

    if size_mb < 1000:
        return {
            "driver_memory": "2g",
            "driver_cores": 1,
            "executor_memory": "4g",
            "executor_cores": 2,
            "num_executors": 2,
            "min_executors": 1,
            "initial_executors": 2,
            "max_executors": 3,
            "shuffle_partitions": max(16, min(256, ideal_partitions * 2)),
            "default_parallelism": max(16, min(256, ideal_partitions * 2)),
            "dynamic_allocation": True,
            "aqe_enabled": True,
            "serializer": "org.apache.spark.serializer.KryoSerializer",
            "broadcast_threshold_mb": 100,
            "memory_fraction": 0.6,
            "storage_fraction": 0.5,
        }

    if size_mb < 10_000:
        return {
            "driver_memory": "2g",
            "driver_cores": 1,
            "executor_memory": "8g",
            "executor_cores": 4,
            "num_executors": 4,
            "min_executors": 2,
            "initial_executors": 4,
            "max_executors": 6,
            "shuffle_partitions": max(32, min(512, ideal_partitions * 3)),
            "default_parallelism": max(32, min(512, ideal_partitions * 3)),
            "dynamic_allocation": True,
            "aqe_enabled": True,
            "serializer": "org.apache.spark.serializer.KryoSerializer",
            "broadcast_threshold_mb": 200,
            "memory_fraction": 0.6,
            "storage_fraction": 0.5,
        }

    executor_count = max(4, min(24, int(math.ceil(max(1.0, size_gb / 2.0)))))
    executor_memory_gb = max(8, int(math.ceil(max(2.0, size_gb * 2.0))))
    shuffle_partitions = max(64, min(4096, int(math.ceil(max(ideal_partitions, size_gb * 32)))))
    return {
        "driver_memory": "4g",
        "driver_cores": 2,
        "executor_memory": f"{executor_memory_gb}g",
        "executor_cores": 4,
        "num_executors": executor_count,
        "min_executors": max(2, executor_count // 2),
        "initial_executors": max(2, executor_count // 2),
        "max_executors": executor_count,
        "shuffle_partitions": shuffle_partitions,
        "default_parallelism": shuffle_partitions,
        "dynamic_allocation": True,
        "aqe_enabled": True,
        "serializer": "org.apache.spark.serializer.KryoSerializer",
        "broadcast_threshold_mb": 200,
        "memory_fraction": 0.6,
        "storage_fraction": 0.5,
    }


def build_spark_session_config(resource_config: Optional[Dict[str, Any]] = None, profile: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
    cfg = recommend_spark_resources(profile)
    if resource_config:
        cfg.update(resource_config)

    dynamic_allocation = "true" if bool(cfg.get("dynamic_allocation", True)) else "false"
    aqe_enabled = "true" if bool(cfg.get("aqe_enabled", True)) else "false"
    broadcast_threshold = int(cfg.get("broadcast_threshold_mb", 200)) * 1024 * 1024

    return {
        "spark.app.name": "ETLFlow_Spark_Engine",
        "spark.master": "spark://spark:7077",
        "spark.jars": "/opt/spark/jars/postgresql-42.6.0.jar",
        "spark.driver.memory": str(cfg.get("driver_memory", "2g")),
        "spark.driver.cores": str(cfg.get("driver_cores", 1)),
        "spark.executor.memory": str(cfg.get("executor_memory", "2g")),
        "spark.executor.cores": str(cfg.get("executor_cores", 2)),
        "spark.executor.instances": str(cfg.get("num_executors", 2)),
        "spark.dynamicAllocation.enabled": dynamic_allocation,
        "spark.dynamicAllocation.minExecutors": str(cfg.get("min_executors", 1)),
        "spark.dynamicAllocation.maxExecutors": str(cfg.get("max_executors", cfg.get("num_executors", 2))),
        "spark.dynamicAllocation.initialExecutors": str(cfg.get("initial_executors", cfg.get("num_executors", 2))),
        "spark.sql.adaptive.enabled": aqe_enabled,
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": str(64 * 1024 * 1024),
        "spark.sql.shuffle.partitions": str(cfg.get("shuffle_partitions", 64)),
        "spark.default.parallelism": str(cfg.get("default_parallelism", cfg.get("shuffle_partitions", 64))),
        "spark.sql.autoBroadcastJoinThreshold": str(broadcast_threshold),
        "spark.serializer": str(cfg.get("serializer", "org.apache.spark.serializer.KryoSerializer")),
        "spark.kryo.registrationRequired": "false",
        "spark.memory.fraction": str(cfg.get("memory_fraction", 0.6)),
        "spark.memory.storageFraction": str(cfg.get("storage_fraction", 0.5)),
        "spark.hadoop.fs.permissions.umask-mode": "000",
    }


def set_runtime_spark_config(payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if payload:
        _RUNTIME_CONFIG.update(payload)
    return get_runtime_spark_config()


def get_runtime_spark_config(profile: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    base = recommend_spark_resources(profile)
    if not _RUNTIME_CONFIG:
        return base

    merged = dict(base)
    for key, value in _RUNTIME_CONFIG.items():
        if value is None:
            continue
        if key in {"driver_memory", "executor_memory"}:
            merged[key] = str(value)
        elif key in {"driver_cores", "executor_cores", "num_executors", "min_executors", "initial_executors", "max_executors", "shuffle_partitions", "default_parallelism"}:
            merged[key] = int(value)
        elif key in {"dynamic_allocation", "aqe_enabled"}:
            merged[key] = bool(value)
        else:
            merged[key] = value
    return merged


def get_runtime_spark_session_config(profile: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
    return build_spark_session_config(get_runtime_spark_config(profile), profile)


def serialize_runtime_config(config: Dict[str, Any]) -> str:
    return json.dumps(config, sort_keys=True)