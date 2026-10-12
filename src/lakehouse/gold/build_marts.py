"""Gold: star schema and BI-ready aggregates."""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse.config import get_config
from lakehouse.utils.spark import get_spark


def build_dim_date(spark: SparkSession, start: str = "2024-01-01", end: str = "2027-12-31") -> DataFrame:
    return (
        spark.sql(f"SELECT explode(sequence(to_date('{start}'), to_date('{end}'), interval 1 day)) AS date")
        .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("date"))
        .withColumn("quarter", F.quarter("date"))
        .withColumn("month", F.month("date"))
        .withColumn("month_name", F.date_format("date", "MMMM"))
        .withColumn("day_of_week", F.dayofweek("date"))
        .withColumn("is_weekend", F.dayofweek("date").isin(1, 7))
    )


def build_fact_sales(transactions: DataFrame, products: DataFrame) -> DataFrame:
    """Completed sales with cost/margin; dimensions are small so broadcast them."""
    prod = F.broadcast(products.select("product_id", "unit_cost"))
    return (
        transactions.filter("status = 'COMPLETED'")
        .join(prod, "product_id", "left")
        .withColumn("date_key", F.date_format("event_date", "yyyyMMdd").cast("int"))
        .withColumn("cost_amount", F.round(F.col("quantity") * F.col("unit_cost"), 2).cast("decimal(14,2)"))
        .withColumn("margin_amount", (F.col("net_amount") - F.col("cost_amount")).cast("decimal(14,2)"))
        .select(
            "transaction_id", "date_key", "event_ts", "customer_id", "product_id", "store_id",
            "payment_method", "quantity", "unit_price", "discount_pct",
            "net_amount", "cost_amount", "margin_amount",
        )
    )


def build_daily_sales(fact: DataFrame, stores: DataFrame, products: DataFrame) -> DataFrame:
    """Pre-aggregated mart: one row per date x region x category (powers the executive dashboard)."""
    return (
        fact.join(F.broadcast(stores.select("store_id", F.col("region").alias("region"))), "store_id")
        .join(F.broadcast(products.select("product_id", "category")), "product_id")
        .groupBy("date_key", "region", "category")
        .agg(
            F.sum("net_amount").alias("revenue"),
            F.sum("margin_amount").alias("margin"),
            F.sum("quantity").alias("units"),
            F.countDistinct("transaction_id").alias("transactions"),
            F.countDistinct("customer_id").alias("customers"),
        )
    )


def main() -> None:
    cfg = get_config()
    spark = get_spark("gold_marts")
    w = lambda df, name: df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(  # noqa: E731
        cfg.table("gold", name)
    )

    txn = spark.table(cfg.table("silver", "transactions"))
    products = spark.table(cfg.table("silver", "products"))
    stores = spark.table(cfg.table("silver", "stores"))
    customers = spark.table(cfg.table("silver", "customers")).filter("is_current")

    fact = build_fact_sales(txn, products)
    # Liquid Clustering on the columns used by most filters (Databricks Runtime).
    fact.write.format("delta").mode("overwrite").option("overwriteSchema", "true").clusterBy(
        "date_key", "store_id"
    ).saveAsTable(cfg.table("gold", "fact_sales"))
    w(build_dim_date(spark), "dim_date")
    w(customers.select("customer_id", "full_name", "email", "region", "segment"), "dim_customer")
    w(products, "dim_product")
    w(stores, "dim_store")
    w(build_daily_sales(spark.table(cfg.table("gold", "fact_sales")), stores, products), "mart_daily_sales")


if __name__ == "__main__":
    main()
