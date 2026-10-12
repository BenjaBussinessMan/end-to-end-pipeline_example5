"""SparkSession helper: reuses the Databricks session, falls back to local Delta."""
from pyspark.sql import SparkSession


def get_spark(app_name: str = "lakehouse") -> SparkSession:
    existing = SparkSession.getActiveSession()
    if existing is not None:
        return existing
    # Local mode (tests / laptop): requires `delta-spark`.
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
