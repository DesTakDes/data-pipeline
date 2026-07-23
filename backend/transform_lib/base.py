"""
transform_lib.base
───────────────────
Abstract contract every engine-specific compiler must follow. Keeping the
method names identical across DuckDBCompiler / SparkCompiler / PostgresCompiler
means a reviewer can diff the three implementations side by side and instantly
see if one engine's behavior has drifted from the others.
"""
from abc import ABC
from .spec import TransformStep


class TransformCompiler(ABC):

    DISPATCH: dict[str, str] = {
        "filter_rows":    "apply_filter_rows",
        "select_col":     "apply_select_col",
        "drop_col":       "apply_drop_col",
        "rename_col":     "apply_rename_col",
        "add_const":      "apply_add_const",
        "set_val":        "apply_set_val",
        "val_mapper":     "apply_val_mapper",
        "fill_null":      "apply_fill_null",
        "change_type":    "apply_change_type",
        "order_table":    "apply_order_table",
        "group_agg":      "apply_group_agg",
        "calc":           "apply_calc",
        "adv_calculator": "apply_adv_calculator",
        "combine_cols":   "apply_combine_cols",
        "join_data":      "apply_join_data",
    }

    def apply(self, state, step: TransformStep):
        method_name = self.DISPATCH.get(step.type)
        if not method_name or not hasattr(self, method_name):
            self.on_unsupported(step)
            return state
        method = getattr(self, method_name)
        try:
            return method(state, step.config)
        except Exception as e:
            self.on_error(step, e)
            return state

    def compile_all(self, initial_state, steps: list):
        """Fold every TransformStep into the running state, in order."""
        state = initial_state
        for raw in steps:
            ts = raw if isinstance(raw, TransformStep) else TransformStep.from_dict(raw)
            state = self.apply(state, ts)
        return state

    def on_unsupported(self, step: TransformStep):
        print(f"[{self.__class__.__name__}] Unsupported/unknown step skipped: {step.type}")

    def on_error(self, step: TransformStep, exc: Exception):
        print(f"[{self.__class__.__name__}] {step.type} failed: {exc} — step skipped")