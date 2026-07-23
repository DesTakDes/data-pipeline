"""
transform_lib.spark_compiler
──────────────────────────────
Compiles a list of TransformStep into a chain of Spark DataFrame operations.
Used by:
  - Preview Engine (preview/spark_executor.py) — in-process, long-lived SparkSession
  - Airflow's Spark task runner (large datasets, >5GB)

State shape: a single Spark DataFrame (transformed in place, functionally).
"""
from .base import TransformCompiler
from .sql_helpers import SPARK_DTYPE_MAP


class SparkCompiler(TransformCompiler):
    def __init__(self, spark, get_right_df, estimate_mb_fn=None, broadcast_max_mb: int = 200):
        """
        spark:            active SparkSession
        get_right_df:     callable(table_name: str) -> DataFrame — injected so this
                           compiler never hardcodes JDBC connection details itself.
        estimate_mb_fn:   optional callable(table_name) -> float, used to decide
                           whether to broadcast the right side of a join.
        broadcast_max_mb: threshold below which the right table is broadcast.
        """
        self.spark = spark
        self.get_right_df = get_right_df
        self.estimate_mb_fn = estimate_mb_fn or (lambda _: 9999.0)
        self.broadcast_max_mb = broadcast_max_mb

    # ── 1. filter_rows ────────────────────────────────────────────────────
    def apply_filter_rows(self, df, config):
        from pyspark.sql import functions as F
        return df.filter(F.expr(config.get("formula", "1=1")))

    # ── 2. select_col ─────────────────────────────────────────────────────
    def apply_select_col(self, df, config):
        cols = [c for c in config.get("columns", []) if c in df.columns]
        return df.select(*cols) if cols else df

    # ── 3. drop_col ───────────────────────────────────────────────────────
    def apply_drop_col(self, df, config):
        drop = [c for c in config.get("columns", []) if c in df.columns]
        return df.drop(*drop) if drop else df

    # ── 4. rename_col ─────────────────────────────────────────────────────
    def apply_rename_col(self, df, config):
        for o, n in config.get("renames", {}).items():
            if o in df.columns:
                df = df.withColumnRenamed(o, n)
        return df

    # ── 5. add_const ──────────────────────────────────────────────────────
    def apply_add_const(self, df, config):
        from pyspark.sql import functions as F
        name = config.get("name", "new_col")
        val = config.get("value", "")
        dtype = SPARK_DTYPE_MAP.get(config.get("dtype", "TEXT"), "string")
        return df.withColumn(name, F.lit(val).cast(dtype))

    # ── 6. set_val ────────────────────────────────────────────────────────
    def apply_set_val(self, df, config):
        from pyspark.sql import functions as F
        target = config.get("targetCol")
        if not target:
            return df
        if config.get("useExpr"):
            return df.withColumn(target, F.expr(config.get("expr", target)))
        src = config.get("sourceCol", target)
        if src in df.columns:
            return df.withColumn(target, F.col(src))
        return df

    # ── 7. val_mapper ─────────────────────────────────────────────────────
    def apply_val_mapper(self, df, config):
        from pyspark.sql import functions as F
        src = config.get("sourceCol")
        new_col = config.get("newColName", "mapped")
        whens = config.get("whens", [])
        else_v = config.get("elseValue", "")
        if not src or src not in df.columns:
            return df
        expr = None
        for w in whens:
            condition = w.get("condition", "=")
            value = w.get("value", "")
            result = w.get("result", "")
            if condition not in ("IS NULL", "IS NOT NULL") and not value and value != 0:
                continue
            cond_expr = self._spark_when_condition(F, df[src], condition, value)
            if cond_expr is None:
                continue
            expr = F.when(cond_expr, F.lit(result)) if expr is None else expr.when(cond_expr, F.lit(result))
        return df.withColumn(new_col, expr.otherwise(F.lit(else_v)) if expr is not None else F.lit(else_v))

    @staticmethod
    def _spark_when_condition(F, col, condition, value):
        if condition == "=":
            return col == value
        if condition == "!=":
            return col != value
        if condition in (">", ">=", "<", "<="):
            try:
                num = float(value)
            except (TypeError, ValueError):
                return None
            c_num = col.cast("double")
            return {">": c_num > num, ">=": c_num >= num, "<": c_num < num, "<=": c_num <= num}[condition]
        if condition == "LIKE":
            return col.like(value)
        if condition == "IS NULL":
            return col.isNull()
        if condition == "IS NOT NULL":
            return col.isNotNull()
        if condition == "IN":
            return col.isin(*[v.strip() for v in str(value).split(",")])
        if condition == "NOT IN":
            return ~col.isin(*[v.strip() for v in str(value).split(",")])
        return col == value

    # ── 8. fill_null ──────────────────────────────────────────────────────
    def apply_fill_null(self, df, config):
        from pyspark.sql import functions as F, Window
        fc = [c for c in config.get("columns", []) if c in df.columns]
        ft = config.get("fillType", "value")
        fv = config.get("fillValue", "")
        if not fc:
            return df
        if ft == "value":
            return df.fillna(fv, subset=fc)
        if ft == "mean":
            stats = df.select([F.mean(F.col(c)).alias(c) for c in fc]).collect()[0].asDict()
            stats = {k: v for k, v in stats.items() if v is not None}
            return df.fillna(stats) if stats else df
        if ft == "median":
            meds = {}
            for c in fc:
                q = df.approxQuantile(c, [0.5], 0.001)
                if q:
                    meds[c] = q[0]
            return df.fillna(meds) if meds else df
        if ft == "mode":
            modes = {}
            for c in fc:
                row = (df.filter(F.col(c).isNotNull()).groupBy(c).count()
                         .orderBy(F.desc("count")).limit(1).collect())
                if row:
                    modes[c] = row[0][c]
            return df.fillna(modes) if modes else df
        if ft in ("forward", "backward"):
            df = df.withColumn("_rn", F.monotonically_increasing_id())
            if ft == "forward":
                win = Window.orderBy("_rn").rowsBetween(Window.unboundedPreceding, 0)
                fn = F.last
            else:
                win = Window.orderBy("_rn").rowsBetween(0, Window.unboundedFollowing)
                fn = F.first
            for c in fc:
                df = df.withColumn(c, fn(F.col(c), ignorenulls=True).over(win))
            return df.drop("_rn")
        return df

    # ── 9. change_type ────────────────────────────────────────────────────
    def apply_change_type(self, df, config):
        from pyspark.sql import functions as F
        for col, dtype in (config.get("types") or {}).items():
            if col in df.columns:
                df = df.withColumn(col, F.col(col).cast(SPARK_DTYPE_MAP.get(dtype, "string")))
        return df

    # ── 10. order_table ───────────────────────────────────────────────────
    def apply_order_table(self, df, config):
        from pyspark.sql import functions as F
        orders = config.get("orders", [])
        cols = [
            F.col(o["col"]).asc() if o.get("dir", "ASC") == "ASC" else F.col(o["col"]).desc()
            for o in orders if o.get("col") in df.columns
        ]
        return df.orderBy(*cols) if cols else df

    # ── 11. group_agg ─────────────────────────────────────────────────────
    def apply_group_agg(self, df, config):
        from pyspark.sql import functions as F
        gc = [c for c in config.get("groupCols", []) if c in df.columns]
        ac = config.get("aggCols", [])
        if not gc or not ac:
            return df
        fn_map = {
            "COUNT": F.count, "SUM": F.sum, "AVG": F.avg,
            "MIN": F.min, "MAX": F.max, "COUNT DISTINCT": F.countDistinct,
        }
        aggs = [
            fn_map.get(a["func"], F.count)(a["col"]).alias(a["alias"])
            for a in ac if a.get("col") in df.columns
        ]
        return df.groupBy(*gc).agg(*aggs) if aggs else df

    # ── 12. calc ──────────────────────────────────────────────────────────
    def apply_calc(self, df, config):
        from pyspark.sql import functions as F
        new_col = (config.get("newColName") or "result").strip()
        col_a, col_b = config.get("colA"), config.get("colB")
        op = config.get("operation", "+")
        if not (new_col and col_a in df.columns and col_b in df.columns):
            return df
        a, b = F.col(col_a).cast("double"), F.col(col_b).cast("double")
        expr = {"+": a + b, "-": a - b, "*": a * b, "/": F.when(b != 0, a / b)}.get(op, a + b)
        return df.withColumn(new_col, expr)

    # ── 13. adv_calculator ────────────────────────────────────────────────
    def apply_adv_calculator(self, df, config):
        from pyspark.sql import functions as F
        SCI = {"sin": F.sin, "cos": F.cos, "sqrt": F.sqrt, "radians": F.radians,
               "atan2": F.atan2, "power": F.pow}
        for calc in config.get("calculations", []):
            op_name = calc.get("operation", "sin")
            fn = SCI.get(op_name, F.sin)
            new_c = (calc.get("newColName") or "").strip()
            col_a, col_b = calc.get("colA"), calc.get("colB")
            if not new_c or col_a not in df.columns:
                continue
            if op_name in ("atan2", "power") and col_b in df.columns:
                df = df.withColumn(new_c, fn(F.col(col_a).cast("double"), F.col(col_b).cast("double")))
            else:
                df = df.withColumn(new_c, fn(F.col(col_a).cast("double")))
        return df

    # ── 14. combine_cols ──────────────────────────────────────────────────
    def apply_combine_cols(self, df, config):
        from pyspark.sql import functions as F
        new_col = (config.get("newColName") or "combined").strip()
        sep = config.get("separator", " ")
        selected = [c for c in config.get("selectedCols", []) if c in df.columns]
        remove_orig = config.get("removeOriginal", False)
        if not new_col or not selected:
            return df
        parts = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in selected]
        combined = parts[0]
        for p in parts[1:]:
            combined = F.concat(combined, F.lit(sep), p)
        df = df.withColumn(new_col, combined)
        if remove_orig:
            df = df.drop(*selected)
        return df

    # ── 15. join_data ─────────────────────────────────────────────────────
    def apply_join_data(self, df, config):
        from pyspark.sql import functions as F
        right_table = config.get("rightTable")
        left_col = config.get("leftCol")
        right_col = config.get("rightCol")
        if not (right_table and left_col):
            return df

        right_df = self.get_right_df(right_table)

        raw_type = config.get("joinType", "INNER JOIN").upper()
        is_cross = "CROSS" in raw_type
        join_type = raw_type.replace(" JOIN", "").lower().replace("full outer", "outer")

        dup_cols = [c for c in right_df.columns if c in df.columns and c != right_col]
        for c in dup_cols:
            right_df = right_df.withColumnRenamed(c, f"{c}_right")

        right_size_mb = self.estimate_mb_fn(right_table)
        if right_size_mb <= self.broadcast_max_mb:
            right_df = F.broadcast(right_df)

        if is_cross:
            return df.crossJoin(right_df)
        if right_col:
            return df.join(right_df, df[left_col] == right_df[right_col], join_type)
        return df