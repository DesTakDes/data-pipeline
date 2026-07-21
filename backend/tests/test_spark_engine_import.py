from pathlib import Path
import importlib
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))


def test_spark_engine_imports_on_python_39_compatible_syntax():
    module = importlib.import_module('spark_engine')
    assert hasattr(module, 'get_spark_session')
    assert hasattr(module, 'preview_pipeline_spark')
