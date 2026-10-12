"""Post-load gate: fail the workflow if data quality or freshness thresholds are breached."""
import sys

from pyspark.sql import functions as F

from lakehouse.config import get_config
from lakehouse.utils.spark import get_spark

MAX_FAILED_PCT = 5.0
MAX_STALENESS_HOURS = 26


def main() -> None:
    cfg = get_config()
    spark = get_spark("dq_gate")
    errors = []

    latest = spark.table(cfg.table("governance", "dq_results")).agg(F.max("run_ts")).first()[0]
    worst = (
        spark.table(cfg.table("governance", "dq_results"))
        .filter(F.col("run_ts") == F.lit(latest))
        .agg(F.max("failed_pct")).first()[0]
    )
    if worst is not None and worst > MAX_FAILED_PCT:
        errors.append(f"DQ rule failure rate {worst}% exceeds {MAX_FAILED_PCT}%")

    stale = spark.sql(
        f"SELECT (unix_timestamp(current_timestamp()) - unix_timestamp(max(event_ts))) / 3600 "
        f"FROM {cfg.table('gold', 'fact_sales')}"
    ).first()[0]
    if stale is None or stale > MAX_STALENESS_HOURS:
        errors.append(f"fact_sales is stale: {stale} hours")

    if errors:
        print("\n".join(errors))
        sys.exit(1)
    print("DQ gate passed")


if __name__ == "__main__":
    main()
