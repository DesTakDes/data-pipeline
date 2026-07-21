from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

import main


def test_spark_resource_recommendation_endpoint():
    response = main.get_spark_resource_recommendation(file_size_bytes=50 * 1024 * 1024, row_count=1000, col_count=5)
    assert 'profile' in response
    assert 'recommendation' in response
    assert response['recommendation']['num_executors'] == 1


def test_spark_runtime_config_endpoint():
    response = main.set_spark_runtime_config({'executor_memory': '2g', 'num_executors': 2})
    assert 'runtime_config' in response
    assert 'session_config' in response
