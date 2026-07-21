from pathlib import Path
import importlib.util
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))


def test_generated_task_template_imports_spark_modules_without_failing():
    from main import generate_task_file

    code = generate_task_file(
        task_id='task_6248',
        dag_id='ayobisa',
        workflow_id='wf_test',
        input_table='staging.sales',
        output_name='sales_out',
        transforms=[{'type': 'select_col', 'config': {'columns': ['id']}}],
    )

    assert 'import spark_engine' in code
    assert 'import spark_config' in code
    assert 'from airflow.providers.postgres.hooks.postgres import PostgresHook' in code
