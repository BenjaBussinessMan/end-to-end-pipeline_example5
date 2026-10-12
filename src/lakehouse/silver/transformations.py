"""Pure DataFrame transformations for the Silver layer (unit-testable locally)."""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from lakehouse.utils.dq import Rule

TRANSACTION_RULES = [
    Rule("transaction_id_not_null", "transaction_id IS NOT NULL"),
    Rule("customer_id_not_null", "customer_id IS NOT NULL"),
    Rule("quantity_positive", "quantity > 0"),
    Rule("unit_price_non_negative", "unit_price >= 0"),
    Rule("discount_in_range", "discount_pct BETWEEN 0 AND 1"),
    Rule("status_valid", "status IN ('COMPLETED','REFUNDED','CANCELLED')"),
    Rule("event_ts_not_null", "event_ts IS NOT NULL"),
]


def standardize_transactions(df: DataFrame) -> DataFrame:
    """Type casting, trimming and derived columns."""
    return (
        df.withColumn("transaction_id", F.trim(F.col("transaction_id")))
        .withColumn("customer_id", F.col("customer_id").cast("long"))
        .withColumn("product_id", F.col("product_id").cast("long"))
        .withColumn("store_id", F.col("store_id").cast("long"))
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("unit_price", F.col("unit_price").cast("decimal(12,2)"))
        .withColumn("discount_pct", F.col("discount_pct").cast("decimal(4,2)"))
        .withColumn("payment_method", F.upper(F.trim("payment_method")))
        .withColumn("status", F.upper(F.trim("status")))
        .withColumn("event_ts", F.col("event_ts").cast("timestamp"))
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn(
            "net_amount",
            F.round(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount_pct")), 2).cast("decimal(14,2)"),
        )
    )


def deduplicate(df: DataFrame, key: str = "transaction_id", order_col: str = "_ingest_ts") -> DataFrame:
    """Keep the most recently ingested record per key."""
    w = Window.partitionBy(key).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
