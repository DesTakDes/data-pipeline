"""
transform_lib.postgres_compiler
──────────────────────────────────
Native Postgres runner for small datasets (<50MB). Each transform is written
as an immutable `CREATE TABLE staging._etl_{task_id}_s{N} AS SELECT ...` step,
exactly like the original `_run_postgres` in main.py — refactored into
one-method-per-transform-type so it can be diffed against DuckDBCompiler /
SparkCompiler and unit-tested independently of a live Postgres connection
(by injecting a fake `pg_hook`).

State shape: tuple(cur_from: str, cols: list[str])

Unlike DuckDB/Spark (pure in-memory chaining), this compiler has a side
effect per step: it physically materializes each intermediate result via
`pg_hook.run(...)`. This mirrors production behavior — Postgres CTEs would
work too, but the original design intentionally uses physical staging
tables so a failed step doesn't force re-planning the whole chain.
"""
from .base import TransformCompiler
from .sql_helpers import quote_cols, POSTGRES_DTYPE_MAP, sql_when_fragment

State = tuple  # (cur_from, cols)


class PostgresCompiler(TransformCompiler):
    def __init__(self, pg_hook, task_id: str):
        self.pg_hook = pg_hook
        self.task_id = task_id
        self.step = 0

    def _tmp(self) -> str:
        self.step += 1
        return f"staging._etl_{self.task_id}_s{self.step}"

    # ── 1. filter_rows ────────────────────────────────────────────────────
    def apply_filter_rows(self, state: State, config: dict) -> State:
        cur_from, cols = state
        tmp = self._tmp()
        formula = config.get("formula", "1=1")
        self.pg_hook.run(f"CREATE TABLE {tmp} AS SELECT * FROM {cur_from} WHERE {formula}")
        return tmp, cols

    # ── 2. select_col ─────────────────────────────────────────────────────
    def apply_select_col(self, state: State, config: dict) -> State:
        cur_from, cols = state
        sc = [c for c in config.get("columns", []) if c in cols]
        if not sc:
            return state
        tmp = self._tmp()
        self.pg_hook.run(f"CREATE TABLE {tmp} AS SELECT {quote_cols(sc)} FROM {cur_from}")
        return tmp, sc

    # ── 3. drop_col ───────────────────────────────────────────────────────
    def apply_drop_col(self, state: State, config: dict) -> State:
        cur_from, cols = state
        kc = [c for c in cols if c not in set(config.get("columns", []))]
        tmp = self._tmp()
        self.pg_hook.run(f"CREATE TABLE {tmp} AS SELECT {quote_cols(kc)} FROM {cur_from}")
        return tmp, kc

    # ── 4. rename_col ─────────────────────────────────────────────────────
    def apply_rename_col(self, state: State, config: dict) -> State:
        cur_from, cols = state
        rn = config.get("renames", {})
        if not rn:
            return state
        ex = ", ".join(f'"{c}" AS "{rn.get(c, c)}"' for c in cols)
        tmp = self._tmp()
        self.pg_hook.run(f"CREATE TABLE {tmp} AS SELECT {ex} FROM {cur_from}")
        return tmp, [rn.get(c, c) for c in cols]

    # ── 5. add_const ──────────────────────────────────────────────────────
    def apply_add_const(self, state: State, config: dict) -> State:
        cur_from, cols = state
        name = config.get("name", "new_col")
        val = config.get("value", "")
        dtype = POSTGRES_DTYPE_MAP.get(config.get("dtype", "TEXT"), "TEXT")
        if not name:
            return state
        tmp = self._tmp()
        self.pg_hook.run(
            f'CREATE TABLE {tmp} AS SELECT {quote_cols(cols)}, CAST({val!r} AS {dtype}) AS "{name}" '
            f'FROM {cur_from}'
        )
        new_cols = cols if name in cols else cols + [name]
        return tmp, new_cols

    # ── 6. set_val ────────────────────────────────────────────────────────
    def apply_set_val(self, state: State, config: dict) -> State:
        cur_from, cols = state
        target = config.get("targetCol", "")
        if not (target and target in cols):
            return state
        if config.get("useExpr"):
            expr = config.get("expr", f'"{target}"')
        else:
            src = config.get("sourceCol", target)
            expr = f'"{src}"' if src in cols else f'"{target}"'
        sel = ", ".join(f'({expr}) AS "{c}"' if c == target else f'"{c}"' for c in cols)
        tmp = self._tmp()
        self.pg_hook.run(f"CREATE TABLE {tmp} AS SELECT {sel} FROM {cur_from}")
        return tmp, cols

    # ── 7. val_mapper ─────────────────────────────────────────────────────
    def apply_val_mapper(self, state: State, config: dict) -> State:
        cur_from, cols = state
        src, new_col = config.get("sourceCol", ""), config.get("newColName", "mapped")
        whens, else_v = config.get("whens", []), config.get("elseValue", "")
        if not (src in cols and whens):
            return state
        fragments = []
        for w in whens:
            condition = w.get("condition", "=")
            value = w.get("value", "")
            result = w.get("result", "")
            if condition not in ("IS NULL", "IS NOT NULL") and value == "":
                continue
            frag = sql_when_fragment(src, condition, value, result)
            if frag:
                fragments.append(frag)
        if not fragments:
            return state
        case_expr = f'CASE {" ".join(fragments)} ELSE {else_v!r} END AS "{new_col}"'
        tmp = self._tmp()
        self.pg_hook.run(f"CREATE TABLE {tmp} AS SELECT {quote_cols(cols)}, {case_expr} FROM {cur_from}")
        new_cols = cols if new_col in cols else cols + [new_col]
        return tmp, new_cols

    # ── 8. fill_null ──────────────────────────────────────────────────────
    def apply_fill_null(self, state: State, config: dict) -> State:
        cur_from, cols = state
        fc = [c for c in config.get("columns", []) if c in cols]
        ft = config.get("fillType", "value")
        fv = config.get("fillValue", "")
        if not fc:
            return state
        tmp = self._tmp()

        if ft == "value":
            sel = ", ".join(
                f'COALESCE("{c}"::TEXT,{str(fv)!r})::TEXT AS "{c}"' if c in fc else f'"{c}"' for c in cols
            )
        elif ft == "mean":
            sel = ", ".join(
                f'COALESCE("{c}", (SELECT AVG("{c}") FROM {cur_from})) AS "{c}"' if c in fc else f'"{c}"'
                for c in cols
            )
        elif ft == "median":
            sel = ", ".join(
                f'COALESCE("{c}", (SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY "{c}") FROM {cur_from})) AS "{c}"'
                if c in fc else f'"{c}"' for c in cols
            )
        elif ft == "mode":
            sel = ", ".join(
                f'COALESCE("{c}", (SELECT "{c}" FROM {cur_from} WHERE "{c}" IS NOT NULL '
                f'GROUP BY "{c}" ORDER BY COUNT(*) DESC LIMIT 1)) AS "{c}"' if c in fc else f'"{c}"'
                for c in cols
            )
        elif ft in ("forward", "backward"):
            # Correlated subquery via ctid — acceptable for the small-data Postgres
            # engine tier (<50MB) this compiler targets; not meant to scale beyond that.
            cmp_op = "<" if ft == "forward" else ">"
            order_dir = "DESC" if ft == "forward" else "ASC"
            sel_parts = []
            for c in cols:
                if c in fc:
                    sel_parts.append(
                        f'COALESCE(t1."{c}", (SELECT t2."{c}" FROM {cur_from} t2 '
                        f'WHERE t2.ctid {cmp_op} t1.ctid AND t2."{c}" IS NOT NULL '
                        f'ORDER BY t2.ctid {order_dir} LIMIT 1)) AS "{c}"'
                    )
                else:
                    sel_parts.append(f't1."{c}"')
            self.pg_hook.run(f"CREATE TABLE {tmp} AS SELECT {', '.join(sel_parts)} FROM {cur_from} t1")
            return tmp, cols
        else:
            return state

        self.pg_hook.run(f"CREATE TABLE {tmp} AS SELECT {sel} FROM {cur_from}")
        return tmp, cols

    # ── 9. change_type ────────────────────────────────────────────────────
    def apply_change_type(self, state: State, config: dict) -> State:
        cur_from, cols = state
        types = config.get("types", {})
        if not types:
            return state
        sel = ", ".join(
            f'CAST("{c}" AS {POSTGRES_DTYPE_MAP.get(types[c], "TEXT")}) AS "{c}"' if c in types else f'"{c}"'
            for c in cols
        )
        tmp = self._tmp()
        self.pg_hook.run(f"CREATE TABLE {tmp} AS SELECT {sel} FROM {cur_from}")
        return tmp, cols

    # ── 10. order_table ───────────────────────────────────────────────────
    def apply_order_table(self, state: State, config: dict) -> State:
        cur_from, cols = state
        orders = [o for o in config.get("orders", []) if o.get("col") in cols]
        if not orders:
            return state
        oc = ", ".join(f'"{o["col"]}" {o.get("dir", "ASC")}' for o in orders)
        tmp = self._tmp()
        self.pg_hook.run(f"CREATE TABLE {tmp} AS SELECT {quote_cols(cols)} FROM {cur_from} ORDER BY {oc}")
        return tmp, cols

    # ── 11. group_agg ─────────────────────────────────────────────────────
    def apply_group_agg(self, state: State, config: dict) -> State:
        cur_from, cols = state
        gc = config.get("groupCols", [])
        ac = config.get("aggCols", [])
        if not gc or not ac:
            return state
        ae = ", ".join(f'{a["func"]}("{a["col"]}") AS "{a["alias"]}"' for a in ac)
        tmp = self._tmp()
        self.pg_hook.run(f"CREATE TABLE {tmp} AS SELECT {quote_cols(gc)}, {ae} FROM {cur_from} GROUP BY {quote_cols(gc)}")
        return tmp, gc + [a["alias"] for a in ac]

    # ── 12. calc ──────────────────────────────────────────────────────────
    def apply_calc(self, state: State, config: dict) -> State:
        cur_from, cols = state
        new_col = (config.get("newColName") or "result").strip()
        col_a, col_b, op = config.get("colA", ""), config.get("colB", ""), config.get("operation", "+")
        if not (new_col and col_a in cols and col_b in cols):
            return state
        if op == "/":
            expr = (f'CASE WHEN CAST("{col_b}" AS DOUBLE PRECISION) != 0 '
                    f'THEN CAST("{col_a}" AS DOUBLE PRECISION) / CAST("{col_b}" AS DOUBLE PRECISION) '
                    f'ELSE NULL END')
        else:
            expr = f'(CAST("{col_a}" AS DOUBLE PRECISION) {op} CAST("{col_b}" AS DOUBLE PRECISION))'
        tmp = self._tmp()
        self.pg_hook.run(f'CREATE TABLE {tmp} AS SELECT {quote_cols(cols)}, {expr} AS "{new_col}" FROM {cur_from}')
        new_cols = cols if new_col in cols else cols + [new_col]
        return tmp, new_cols

    # ── 13. adv_calculator ────────────────────────────────────────────────
    def apply_adv_calculator(self, state: State, config: dict) -> State:
        cur_from, cols = state
        SCI = {"sin": "SIN", "cos": "COS", "sqrt": "SQRT", "radians": "RADIANS",
               "atan2": "ATAN2", "power": "POWER"}
        exprs, new_cols_add = [], []
        for calc in config.get("calculations", []):
            fn = SCI.get(calc.get("operation", "sin"), "SIN")
            col_a, col_b = calc.get("colA", ""), calc.get("colB", "")
            new_c = (calc.get("newColName") or "").strip()
            if not new_c or col_a not in cols:
                continue
            if fn in ("ATAN2", "POWER") and col_b in cols:
                exprs.append(f'{fn}(CAST("{col_a}" AS DOUBLE PRECISION), CAST("{col_b}" AS DOUBLE PRECISION)) AS "{new_c}"')
            else:
                exprs.append(f'{fn}(CAST("{col_a}" AS DOUBLE PRECISION)) AS "{new_c}"')
            new_cols_add.append(new_c)
        if not exprs:
            return state
        tmp = self._tmp()
        self.pg_hook.run(f"CREATE TABLE {tmp} AS SELECT {quote_cols(cols)}, {', '.join(exprs)} FROM {cur_from}")
        return tmp, cols + new_cols_add

    # ── 14. combine_cols ──────────────────────────────────────────────────
    def apply_combine_cols(self, state: State, config: dict) -> State:
        cur_from, cols = state
        new_col = (config.get("newColName") or "combined").strip()
        sep = config.get("separator", " ")
        selected = [c for c in config.get("selectedCols", []) if c in cols]
        remove_orig = config.get("removeOriginal", False)
        if not (new_col and selected):
            return state
        concat_expr = f" || {sep!r} || ".join(f'COALESCE(CAST("{c}" AS TEXT), \'\')' for c in selected)
        keep = [c for c in cols if not (remove_orig and c in selected)]
        tmp = self._tmp()
        self.pg_hook.run(f'CREATE TABLE {tmp} AS SELECT {quote_cols(keep)}, ({concat_expr}) AS "{new_col}" FROM {cur_from}')
        return tmp, keep + [new_col]

    # ── 15. join_data ─────────────────────────────────────────────────────
    def apply_join_data(self, state: State, config: dict) -> State:
        cur_from, cols = state
        right_table = config.get("rightTable", "")
        left_col = config.get("leftCol", "")
        right_col = config.get("rightCol", "")
        if not (right_table and left_col):
            return state

        raw_type = config.get("joinType", "INNER JOIN").upper()
        is_cross = "CROSS" in raw_type
        sql_join = "CROSS JOIN" if is_cross else raw_type

        schema_r, tname_r = right_table.split(".", 1) if "." in right_table else ("staging", right_table)
        r_cols = [r[0] for r in self.pg_hook.get_records(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='{schema_r}' AND table_name='{tname_r.strip(chr(34))}'
            ORDER BY ordinal_position
        """)]
        if not r_cols:
            return state

        dup = [c for c in r_cols if c in cols and c != right_col]
        left_sel = ", ".join(f'l."{c}"' for c in cols)
        right_sel = ", ".join(f'r."{c}" AS "{c}_right"' if c in dup else f'r."{c}"' for c in r_cols)
        new_cols = cols + [f"{c}_right" if c in dup else c for c in r_cols]
        tmp = self._tmp()

        if is_cross:
            self.pg_hook.run(f"CREATE TABLE {tmp} AS SELECT {left_sel}, {right_sel} FROM {cur_from} l CROSS JOIN {right_table} r")
            return tmp, new_cols
        if right_col:
            self.pg_hook.run(
                f"CREATE TABLE {tmp} AS SELECT {left_sel}, {right_sel} "
                f'FROM {cur_from} l {sql_join} {right_table} r ON l."{left_col}" = r."{right_col}"'
            )
            return tmp, new_cols
        return state

    # ── Public compile entrypoint ─────────────────────────────────────────
    def compile_and_run(self, input_table: str, cols: list[str], steps: list) -> State:
        """Runs every step against Postgres, returning the final (table, columns)."""
        return self.compile_all((input_table, cols), steps)

    def cleanup_intermediate_tables(self):
        """Drop all staging._etl_{task_id}_s* tables created during this run."""
        rows = self.pg_hook.get_records(f"""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema='staging' AND table_name LIKE '_etl_{self.task_id}_s%'
        """)
        for (tname,) in rows:
            self.pg_hook.run(f'DROP TABLE IF EXISTS staging."{tname}"')