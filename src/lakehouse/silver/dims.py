"""Silver dimensions. customers is SCD Type 2; products and stores are Type 1."""
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse.config import get_config
from lakehouse.silver.transformations import deduplicate
from lakehouse.utils.spark import get_spark

TRACKED = ["region", "segment"]  # attributes whose change opens a new version


def scd2_merge(spark: SparkSession, incoming: DataFrame, target: str, key: str = "customer_id") -> None:
    """Close the current version when tracked attributes change and insert the new one."""
    incoming = incoming.select(key, "full_name", "email", *TRACKED).withColumn("_eff", F.current_timestamp())

    if not spark.catalog.tableExists(target):
        (
            incoming.withColumn("valid_from", F.col("_eff")).withColumn("valid_to", F.lit(None).cast("timestamp"))
            .withColumn("is_current", F.lit(True)).drop("_eff")
            .write.format("delta").saveAsTable(target)
        )
        return

    changed_cond = " OR ".join(f"t.{c} <> s.{c}" for c in TRACKED)
    current = spark.table(target).filter("is_current")
    # New versions for changed keys + brand-new keys.
    new_versions = (
        incoming.alias("s").join(current.alias("t"), key, "left")
        .filter(F.expr(f"t.valid_from IS NULL OR {changed_cond}"))
        .select("s.*")
    )
    # Rows with NULL merge key never match -> always inserted.
    staged = new_versions.withColumn("_merge_key", F.lit(None).cast("long")).unionByName(
        new_versions.withColumn("_merge_key", F.col(key))
    )
    (
        DeltaTable.forName(spark, target).alias("t")
        .merge(staged.alias("s"), f"t.{key} = s._merge_key AND t.is_current = true")
        .whenMatchedUpdate(set={"is_current": "false", "valid_to": "s._eff"})
        .whenNotMatchedInsert(
            values={
                key: f"s.{key}", "full_name": "s.full_name", "email": "s.email",
                "region": "s.region", "segment": "s.segment",
                "valid_from": "s._eff", "valid_to": "NULL", "is_current": "true",
            }
        )
        .execute()
    )


def main() -> None:
    cfg = get_config()
    spark = get_spark("silver_dims")

    customers = deduplicate(spark.table(cfg.table("bronze", "customers")), "customer_id")
    scd2_merge(spark, customers, cfg.table("silver", "customers"))

    for entity, key in [("products", "product_id"), ("stores", "store_id")]:
        df = deduplicate(spark.table(cfg.table("bronze", entity)), key).drop("_ingest_ts", "_source_file")
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            cfg.table("silver", entity)
        )


if __name__ == "__main__":
    main()
