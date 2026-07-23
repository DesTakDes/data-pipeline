"""
preview.dto
──────────────
Stable response contract returned to ReactFlow. Kept separate from the
internal engine so the API shape can evolve independently of how Spark
executes things internally.
"""
from dataclasses import dataclass, field, asdict


@dataclass
class PreviewResult:
    columns: list[str]
    rows: list[dict]
    schema: dict[str, str]
    execution_time: str
    warnings: list[str] = field(default_factory=list)
    cached_nodes: list[str] = field(default_factory=list)
    logical_plan: str = ""
    physical_plan: str = ""

    row_count_estimate: int | None = None
    is_sampled: bool = False
    engine_used: str = "spark"
    node_timings: dict[str, float] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    def to_json(self) -> dict:
        return asdict(self)