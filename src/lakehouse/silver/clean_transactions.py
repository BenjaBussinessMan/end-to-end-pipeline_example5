"""Silver: clean, validate and incrementally upsert transactions.

Reads only rows newer than the Silver high-water mark, so each run is incremental.
Invalid rows go to silver.transactions_quarantine; rule metrics to governance.dq_results.
"""
from delta.tables import DeltaTable
from pyspark.sql import functions as F

from lakehouse.config import get_config
from lakehouse.silver.transformations import TRANSACTION_RULES, deduplicate, standardize_transactions
from lakehouse.utils.dq import split_valid_invalid, summarize
from lakehouse.utils.spark import get_spark


def main() -> None:
    cfg = get_config()
    spark = get_spark("silver_transactions")
    bronze = cfg.table("bronze", "transactions")
    target = cfg.table("silver", "transactions")

    if spark.catalog.tableExists(target):
        hwm = spark.table(target).agg(F.max("_ingest_ts")).first()[0]
        src = spark.table(bronze).filter(F.col("_ingest_ts") > F.lit(hwm)) if hwm else spark.table(bronze)
    else:
        src = spark.table(bronze)

    clean = deduplicate(standardize_transactions(src)).cache()
    total = clean.count()
    if total == 0:
        return

    valid, quarantine = split_valid_invalid(clean, TRANSACTION_RULES)

    (
        quarantine.withColumn("_quarantined_at", F.current_timestamp())
        .write.format("delta").mode("append").option("mergeSchema", "true")
        .saveAsTable(cfg.table("silver", "transactions_quarantine"))
    )
    summarize(quarantine, total, target).write.format("delta").mode("append").saveAsTable(
        cfg.table("governance", "dq_results")
    )

    if not spark.catalog.tableExists(target):
        (
            valid.write.format("delta")
            .clusterBy("event_date", "store_id")  # Liquid Clustering
            .saveAsTable(target)
        )
    else:
        (
            DeltaTable.forName(spark, target).alias("t")
            .merge(valid.alias("s"), "t.transaction_id = s.transaction_id")
            .whenMatchedUpdateAll(condition="s._ingest_ts > t._ingest_ts")
            .whenNotMatchedInsertAll()
            .execute()
        )
    clean.unpersist()


if __name__ == "__main__":
    main()
