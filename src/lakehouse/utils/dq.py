"""Reusable data quality rules.

Each rule is a named SQL boolean expression that must be TRUE for a valid row.
Rows failing any rule are routed to quarantine with the list of failed rules.
"""
from dataclasses import dataclass
from typing import Iterable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True)
class Rule:
    name: str
    expr: str


def split_valid_invalid(df: DataFrame, rules: Iterable[Rule]) -> tuple[DataFrame, DataFrame]:
    """Return (valid, quarantine). Quarantine has a `_failed_rules` array column.

    A NULL result of a rule counts as a failure.
    """
    rules = list(rules)
    failed = F.array_compact(
        F.array(
            *[
                F.when(F.coalesce(F.expr(r.expr), F.lit(False)), F.lit(None)).otherwise(F.lit(r.name))
                for r in rules
            ]
        )
    )
    flagged = df.withColumn("_failed_rules", failed)
    valid = flagged.filter(F.size("_failed_rules") == 0).drop("_failed_rules")
    quarantine = flagged.filter(F.size("_failed_rules") > 0)
    return valid, quarantine


def summarize(quarantine: DataFrame, total: int, table: str) -> DataFrame:
    """Per-rule failure counts, ready to append to governance.dq_results."""
    return (
        quarantine.select(F.explode("_failed_rules").alias("rule"))
        .groupBy("rule")
        .agg(F.count("*").alias("failed_rows"))
        .withColumn("table_name", F.lit(table))
        .withColumn("total_rows", F.lit(total))
        .withColumn("failed_pct", F.round(F.col("failed_rows") / F.lit(total) * 100, 4))
        .withColumn("run_ts", F.current_timestamp())
    )
