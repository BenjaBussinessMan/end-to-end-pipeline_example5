"""Bronze: incremental raw ingestion with Auto Loader. Append-only, no business logic.

Databricks only (uses the `cloudFiles` source). Run as a job task.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse.config import get_config
from lakehouse.utils.spark import get_spark

ENTITIES = ["transactions", "customers", "products", "stores"]


def with_ingest_metadata(df: DataFrame) -> DataFrame:
    return df.withColumn("_ingest_ts", F.current_timestamp()).withColumn(
        "_source_file", F.col("_metadata.file_path")
    )


def ingest(spark, entity: str, cfg) -> None:
    src = f"{cfg.landing_path}/{entity}"
    raw = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{cfg.checkpoint_path}/{entity}/schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(src)
    )
    (
        with_ingest_metadata(raw)
        .writeStream.option("checkpointLocation", f"{cfg.checkpoint_path}/{entity}/bronze")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)  # incremental batch: process new files, then stop
        .toTable(cfg.table("bronze", entity))
        .awaitTermination()
    )


def main() -> None:
    cfg = get_config()
    spark = get_spark("bronze_ingest")
    for entity in ENTITIES:
        ingest(spark, entity, cfg)


if __name__ == "__main__":
    main()
