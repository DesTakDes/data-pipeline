from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

from spark_config import estimate_dataset_profile, recommend_spark_resources, build_spark_session_config


def test_small_dataset_recommendation_is_conservative():
    profile = estimate_dataset_profile(file_size_bytes=50 * 1024 * 1024, row_count=1000, col_count=5)
    cfg = recommend_spark_resources(profile)
    assert cfg["num_executors"] == 1
    assert cfg["executor_cores"] == 1
    assert cfg["executor_memory"] == "1g"
    assert cfg["shuffle_partitions"] >= 8


def test_large_dataset_config_enables_dynamic_allocation_and_aqe():
    profile = estimate_dataset_profile(file_size_bytes=20 * 1024 * 1024 * 1024, row_count=5_000_000, col_count=40)
    cfg = recommend_spark_resources(profile)
    spark_cfg = build_spark_session_config(cfg, profile)
    assert spark_cfg["spark.dynamicAllocation.enabled"] == "true"
    assert spark_cfg["spark.sql.adaptive.enabled"] == "true"
    assert spark_cfg["spark.serializer"] == "org.apache.spark.serializer.KryoSerializer"
    assert int(spark_cfg["spark.sql.shuffle.partitions"]) >= 16
