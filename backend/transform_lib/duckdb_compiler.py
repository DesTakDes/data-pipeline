"""
transform_lib.duckdb_compiler
───────────────────────────────
Compiles a list of TransformStep into a single DuckDB CTE-chain SQL string.
Used by:
  - Preview Engine's fast-preview path (small in-memory samples)
  - Airflow's DuckDB task runner (mid-size datasets, 50MB–5GB)

State shape: tuple(cte_parts: list[str], cur_alias: str, cur_cols: list[str] | None)
`cur_cols` is None whenever the exact output column list isn't statically known
(e.g. after `SELECT *`-style ops) — methods fall back to `EXCLUDE`/`RENAME`/`REPLACE`
syntax in that case, same as the original main.py implementation.
"""
from .base import TransformCompiler
from .sql_helpers import (
    quote_cols, DUCKDB_DTYPE_MAP, SCI_FUNC_MAP, build_case_expression, concat_expression,
)

State = tuple  # (cte_parts, cur_alias, cur_cols)


class DuckDBCompiler(TransformCompiler):
    def __init__(self, right_tables: dict | None = None):
        """
        right_tables: {rightTable_name: {"alias": "_right_0", "columns": [...]}}
        Populated by the caller after registering right-side join tables
        into the same DuckDB connection (see DuckDBExecutor below).
        """
        self.right_tables = right_tables or {}
        self.step = 0

    def _next_alias(self) -> str:
        self.step += 1
        return f"s{self.step}"

    # ── 1. filter_rows ────────────────────────────────────────────────────
    def apply_filter_rows(self, state: State, config: dict) -> State:
        cte_parts, cur_alias, cur_cols = state
        formula = config.get("formula", "1=1")
        alias = self._next_alias()
        cte_parts.append(f"{alias} AS (SELECT * FROM {cur_alias} WHERE {formula})")
        return cte_parts, alias, cur_cols

    # ── 2. select_col ─────────────────────────────────────────────────────
    def apply_select_col(self, state: State, config: dict) -> State:
        cte_parts, cur_alias, cur_cols = state
        cols = [c for c in config.get("columns", []) if c]
        if not cols:
            return state
        alias = self._next_alias()
        cte_parts.append(f"{alias} AS (SELECT {quote_cols(cols)} FROM {cur_alias})")
        return cte_parts, alias, cols

    # ── 3. drop_col ───────────────────────────────────────────────────────
    def apply_drop_col(self, state: State, config: dict) -> State:
        cte_parts, cur_alias, cur_cols = state
        drop = set(config.get("columns", []))
        alias = self._next_alias()
        if cur_cols:
            keep = [c for c in cur_cols if c not in drop]
            cte_parts.append(f"{alias} AS (SELECT {quote_cols(keep)} FROM {cur_alias})")
            return cte_parts, alias, keep
        excl = ", ".join(f'"{c}"' for c in drop)
        cte_parts.append(f"{alias} AS (SELECT * EXCLUDE ({excl}) FROM {cur_alias})")
        return cte_parts, alias, None

    # ── 4. rename_col ─────────────────────────────────────────────────────
    def apply_rename_col(self, state: State, config: dict) -> State:
        cte_parts, cur_alias, cur_cols = state
        renames = config.get("renames", {})
        if not renames:
            return state
        alias = self._next_alias()
        if cur_cols:
            exprs = [f'"{c}" AS "{renames.get(c, c)}"' for c in cur_cols]
            cte_parts.append(f"{alias} AS (SELECT {', '.join(exprs)} FROM {cur_alias})")
            new_cols = [renames.get(c, c) for c in cur_cols]
            return cte_parts, alias, new_cols
        rename_sql = ", ".join(f'"{o}" AS "{n}"' for o, n in renames.items())
        cte_parts.append(f"{alias} AS (SELECT * RENAME ({rename_sql}) FROM {cur_alias})")
        return cte_parts, alias, None

    # ── 5. add_const ──────────────────────────────────────────────────────
    def apply_add_const(self, state: State, config: dict) -> State:
        cte_parts, cur_alias, cur_cols = state
        name = config.get("name", "new_col")
        val = config.get("value", "")
        dtype = DUCKDB_DTYPE_MAP.get(config.get("dtype", "TEXT"), "VARCHAR")
        alias = self._next_alias()
        cte_parts.append(f'{alias} AS (SELECT *, CAST({val!r} AS {dtype}) AS "{name}" FROM {cur_alias})')
        new_cols = (cur_cols + [name]) if cur_cols else None
        return cte_parts, alias, new_cols

    # ── 6. set_val ────────────────────────────────────────────────────────
    def apply_set_val(self, state: State, config: dict) -> State:
        cte_parts, cur_alias, cur_cols = state
        target = config.get("targetCol", "")
        if not target:
            return state
        if config.get("useExpr"):
            expr = config.get("expr", f'"{target}"')
        else:
            src = config.get("sourceCol", target)
            expr = f'"{src}"'
        alias = self._next_alias()
        cte_parts.append(f'{alias} AS (SELECT * REPLACE ({expr} AS "{target}") FROM {cur_alias})')
        return cte_parts, alias, cur_cols

    # ── 7. val_mapper ─────────────────────────────────────────────────────
    def apply_val_mapper(self, state: State, config: dict) -> State:
        cte_parts, cur_alias, cur_cols = state
        src = config.get("sourceCol", "")
        new_col = config.get("newColName", "mapped")
        whens = config.get("whens", [])
        else_v = config.get("elseValue", "")
        if not src or not whens:
            return state
        case_expr = build_case_expression(src, whens, else_v, new_col)
        if not case_expr:
            return state
        alias = self._next_alias()
        cte_parts.append(f"{alias} AS (SELECT *, {case_expr} FROM {cur_alias})")
        new_cols = (cur_cols + [new_col]) if cur_cols else None
        return cte_parts, alias, new_cols

    # ── 8. fill_null ──────────────────────────────────────────────────────
    def apply_fill_null(self, state: State, config: dict) -> State:
        cte_parts, cur_alias, cur_cols = state
        fill_cols = config.get("columns", [])
        fill_val = config.get("fillValue", "")
        if not (fill_cols and config.get("fillType", "value") == "value"):
            return state
        replace_parts = ", ".join(
            f'COALESCE("{c}", {str(fill_val)!r}) AS "{c}"' for c in fill_cols
        )
        alias = self._next_alias()
        cte_parts.append(f"{alias} AS (SELECT * REPLACE ({replace_parts}) FROM {cur_alias})")
        return cte_parts, alias, cur_cols

    # ── 9. change_type ────────────────────────────────────────────────────
    def apply_change_type(self, state: State, config: dict) -> State:
        cte_parts, cur_alias, cur_cols = state
        types = config.get("types", {})
        if not types:
            return state
        replace_parts = ", ".join(
            f'TRY_CAST("{c}" AS {DUCKDB_DTYPE_MAP.get(t, "VARCHAR")}) AS "{c}"'
            for c, t in types.items()
        )
        alias = self._next_alias()
        cte_parts.append(f"{alias} AS (SELECT * REPLACE ({replace_parts}) FROM {cur_alias})")
        return cte_parts, alias, cur_cols

    # ── 10. order_table ───────────────────────────────────────────────────
    def apply_order_table(self, state: State, config: dict) -> State:
        cte_parts, cur_alias, cur_cols = state
        orders = config.get("orders", [])
        if not orders:
            return state
        oc = ", ".join(f'"{o["col"]}" {o.get("dir", "ASC")}' for o in orders if o.get("col"))
        if not oc:
            return state
        alias = self._next_alias()
        cte_parts.append(f"{alias} AS (SELECT * FROM {cur_alias} ORDER BY {oc})")
        return cte_parts, alias, cur_cols

    # ── 11. group_agg ─────────────────────────────────────────────────────
    def apply_group_agg(self, state: State, config: dict) -> State:
        cte_parts, cur_alias, cur_cols = state
        gcols = config.get("groupCols", [])
        acols = config.get("aggCols", [])
        if not gcols or not acols:
            return state
        agg_exprs = []
        for a in acols:
            fn = a.get("func", "COUNT")
            col = a.get("col", "")
            aln = a.get("alias", f"{col}_{fn.lower()}")
            if fn == "COUNT DISTINCT":
                agg_exprs.append(f'COUNT(DISTINCT "{col}") AS "{aln}"')
            else:
                agg_exprs.append(f'{fn}("{col}") AS "{aln}"')
        g = quote_cols(gcols)
        alias = self._next_alias()
        cte_parts.append(f"{alias} AS (SELECT {g}, {', '.join(agg_exprs)} FROM {cur_alias} GROUP BY {g})")
        new_cols = gcols + [a.get("alias", "") for a in acols]
        return cte_parts, alias, new_cols

    # ── 12. calc ──────────────────────────────────────────────────────────
    def apply_calc(self, state: State, config: dict) -> State:
        cte_parts, cur_alias, cur_cols = state
        new_col = (config.get("newColName") or "result").strip()
        col_a, col_b = config.get("colA", ""), config.get("colB", "")
        operation = config.get("operation", "+")
        if not (new_col and col_a and col_b):
            return state
        if operation == "/":
            op_expr = (
                f'CASE WHEN TRY_CAST("{col_b}" AS DOUBLE) != 0 '
                f'THEN TRY_CAST("{col_a}" AS DOUBLE) / TRY_CAST("{col_b}" AS DOUBLE) '
                f'ELSE NULL END'
            )
        else:
            op_expr = f'TRY_CAST("{col_a}" AS DOUBLE) {operation} TRY_CAST("{col_b}" AS DOUBLE)'
        alias = self._next_alias()
        cte_parts.append(f'{alias} AS (SELECT *, ({op_expr}) AS "{new_col}" FROM {cur_alias})')
        new_cols = (cur_cols + [new_col]) if cur_cols else None
        return cte_parts, alias, new_cols

    # ── 13. adv_calculator ────────────────────────────────────────────────
    def apply_adv_calculator(self, state: State, config: dict) -> State:
        cte_parts, cur_alias, cur_cols = state
        calcs = config.get("calculations", [])
        exprs = []
        new_names = []
        for calc in calcs:
            fn = SCI_FUNC_MAP.get(calc.get("operation", "sin"), "SIN")
            col_a = calc.get("colA", "")
            col_b = calc.get("colB", "")
            new_c = (calc.get("newColName") or "").strip()
            if not new_c or not col_a:
                continue
            if fn in ("ATAN2", "POWER"):
                exprs.append(f'{fn}(TRY_CAST("{col_a}" AS DOUBLE), TRY_CAST("{col_b}" AS DOUBLE)) AS "{new_c}"')
            else:
                exprs.append(f'{fn}(TRY_CAST("{col_a}" AS DOUBLE)) AS "{new_c}"')
            new_names.append(new_c)
        if not exprs:
            return state
        alias = self._next_alias()
        cte_parts.append(f'{alias} AS (SELECT *, {", ".join(exprs)} FROM {cur_alias})')
        new_cols = (cur_cols + new_names) if cur_cols else None
        return cte_parts, alias, new_cols

    # ── 14. combine_cols ──────────────────────────────────────────────────
    def apply_combine_cols(self, state: State, config: dict) -> State:
        cte_parts, cur_alias, cur_cols = state
        new_col = (config.get("newColName") or "combined").strip()
        sep = config.get("separator", " ")
        selected = config.get("selectedCols", [])
        remove_orig = config.get("removeOriginal", False)
        if not new_col or not selected:
            return state
        concat_parts = concat_expression(selected, sep)
        alias = self._next_alias()
        if remove_orig:
            excl = ", ".join(f'"{c}"' for c in selected)
            cte_parts.append(
                f'{alias} AS (SELECT * EXCLUDE ({excl}), ({concat_parts}) AS "{new_col}" FROM {cur_alias})'
            )
        else:
            cte_parts.append(f'{alias} AS (SELECT *, ({concat_parts}) AS "{new_col}" FROM {cur_alias})')
        if cur_cols:
            kept = [c for c in cur_cols if not (remove_orig and c in selected)]
            new_cols = kept + ([new_col] if new_col not in kept else [])
        else:
            new_cols = None
        return cte_parts, alias, new_cols

    # ── 15. join_data ─────────────────────────────────────────────────────
    def apply_join_data(self, state: State, config: dict) -> State:
        cte_parts, cur_alias, cur_cols = state
        right_table = config.get("rightTable", "")
        left_col = config.get("leftCol", "")
        right_col = config.get("rightCol", "")
        r_info = self.right_tables.get(right_table)

        if not (right_table and left_col and r_info):
            return state

        r_alias = r_info["alias"]
        r_cols = r_info.get("columns", [])
        raw_type = config.get("joinType", "INNER JOIN").upper()
        is_cross = "CROSS" in raw_type
        sql_join = "CROSS JOIN" if is_cross else raw_type

        dup = [c for c in r_cols if cur_cols and c in cur_cols and c != right_col]
        right_select = ", ".join(
            f'{r_alias}."{c}" AS "{c}_right"' if c in dup else f'{r_alias}."{c}"'
            for c in r_cols
        ) if r_cols else f"{r_alias}.*"

        select_clause = f"{cur_alias}.*, {right_select}"
        alias = self._next_alias()

        if is_cross:
            cte_parts.append(f"{alias} AS (SELECT {select_clause} FROM {cur_alias} CROSS JOIN {r_alias})")
        elif right_col:
            cte_parts.append(
                f'{alias} AS (SELECT {select_clause} FROM {cur_alias} '
                f'{sql_join} {r_alias} ON {cur_alias}."{left_col}" = {r_alias}."{right_col}")'
            )
        else:
            return state

        # Column set becomes unknown after a join (duplicates/right-side cols) —
        # downstream steps fall back to EXCLUDE/RENAME/REPLACE, exactly as before.
        return cte_parts, alias, None

    # ── Public compile entrypoint ─────────────────────────────────────────
    def compile(self, input_alias: str, steps: list, limit: int | None = None) -> str:
        cte_parts, cur_alias, _ = self.compile_all(([], input_alias, None), steps)
        limit_clause = f" LIMIT {limit}" if limit else ""
        if cte_parts:
            return f"WITH {', '.join(cte_parts)} SELECT * FROM {cur_alias}{limit_clause}"
        return f"SELECT * FROM {input_alias}{limit_clause}"