from pathlib import Path
import sys

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))

from spark_engine import apply_node_transform


def test_add_const_honors_integer_dtype():
    df = pd.DataFrame([{"id": 1}, {"id": 2}])
    node = {"data": {"type": "add_const", "config": {"name": "flag", "value": 7, "dtype": "INTEGER"}}}

    out = apply_node_transform(None, df, node, {}, lambda _id: None)

    assert out["flag"].dtype.kind in {"i", "u"}
    assert out.loc[0, "flag"] == 7


def test_val_mapper_supports_like_condition():
    df = pd.DataFrame([{"name": "alpha"}, {"name": "beta"}, {"name": "gamma"}])
    node = {"data": {"type": "val_mapper", "config": {
        "sourceCol": "name",
        "newColName": "bucket",
        "whens": [{"condition": "LIKE", "value": "a%", "result": "A"}],
        "elseValue": "OTHER",
    }}}

    out = apply_node_transform(None, df, node, {}, lambda _id: None)

    assert out.loc[out["name"] == "alpha", "bucket"].iloc[0] == "A"
    assert out.loc[out["name"] == "beta", "bucket"].iloc[0] == "OTHER"


def test_fill_null_uses_mean_for_selected_columns():
    df = pd.DataFrame([{"id": 1, "value": None}, {"id": 2, "value": 10.0}, {"id": 3, "value": 20.0}])
    node = {"data": {"type": "fill_null", "config": {"columns": ["value"], "fillType": "mean"}}}

    out = apply_node_transform(None, df, node, {}, lambda _id: None)

    assert out.loc[0, "value"] == 15.0


def test_join_data_can_use_right_table_config():
    left = pd.DataFrame([{"code": "A", "id": 1}, {"code": "B", "id": 2}])
    right = pd.DataFrame([{"code": "A", "value": 100}, {"code": "B", "value": 200}])

    node = {"data": {"type": "join_data", "config": {
        "leftCol": "code",
        "rightCol": "code",
        "joinType": "INNER JOIN",
        "rightTable": right,
    }}}

    out = apply_node_transform(None, left, node, {}, lambda _id: None)

    assert out.loc[out["code"] == "A", "value"].iloc[0] == 100
    assert out.loc[out["code"] == "B", "value"].iloc[0] == 200
