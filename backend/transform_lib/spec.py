"""
transform_lib.spec
──────────────────
Engine-agnostic representation of a single pipeline node's transform.
Both the Preview Engine (Spark) and the generated Airflow task files
(DuckDB / Postgres / Spark) consume this same structure.
"""
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class TransformStep:
    type: str
    config: dict[str, Any]

    @classmethod
    def from_dict(cls, d: dict) -> "TransformStep":
        return cls(type=d.get("type", ""), config=d.get("config") or {})

    @classmethod
    def many_from_list(cls, items: list[dict]) -> list["TransformStep"]:
        return [cls.from_dict(d) for d in items]


# The 15 transform types currently supported end-to-end by all three engines.
SUPPORTED_TYPES = (
    "filter_rows", "select_col", "drop_col", "rename_col", "add_const",
    "set_val", "val_mapper", "fill_null", "change_type", "order_table",
    "group_agg", "calc", "adv_calculator", "combine_cols", "join_data",
)