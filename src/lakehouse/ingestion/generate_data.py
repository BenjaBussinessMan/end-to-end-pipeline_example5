"""Synthetic source-system data generator.

Produces dimension snapshots (customers, products, stores) and daily
transaction files, with controlled data quality defects:
  ~1.0% duplicated transaction_id, ~0.5% null customer_id,
  ~0.3% non-positive quantity, ~0.3% negative unit_price.

Usage:
  python -m lakehouse.ingestion.generate_data --out ./data/landing --rows 1000000 --date 2025-01-15
"""
import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse.utils.spark import get_spark

REGIONS = ["NORTE", "SUR", "CENTRO", "ESTE", "OESTE"]
CATEGORIES = ["Electronica", "Hogar", "Moda", "Alimentos", "Deportes", "Belleza"]
PAYMENTS = ["CARD", "CASH", "TRANSFER", "WALLET"]
STATUSES = ["COMPLETED", "COMPLETED", "COMPLETED", "COMPLETED", "REFUNDED", "CANCELLED"]


def _pick(col_seed: str, values: list[str]) -> F.Column:
    """Deterministic pseudo-random choice from a list using a seed column."""
    arr = F.array(*[F.lit(v) for v in values])
    return arr[(F.abs(F.hash(F.col(col_seed))) % len(values)).cast("int")]


def make_customers(spark: SparkSession, n: int = 50_000) -> DataFrame:
    return (
        spark.range(1, n + 1)
        .withColumnRenamed("id", "customer_id")
        .withColumn("full_name", F.concat(F.lit("Customer "), F.col("customer_id")))
        .withColumn("email", F.concat(F.lit("customer"), F.col("customer_id"), F.lit("@example.com")))
        .withColumn("region", _pick("customer_id", REGIONS))
        .withColumn("segment", _pick("customer_id", ["RETAIL", "PREMIUM", "CORPORATE"]))
        .withColumn("updated_at", F.current_timestamp())
    )


def make_products(spark: SparkSession, n: int = 2_000) -> DataFrame:
    return (
        spark.range(1, n + 1)
        .withColumnRenamed("id", "product_id")
        .withColumn("product_name", F.concat(F.lit("Product "), F.col("product_id")))
        .withColumn("category", _pick("product_id", CATEGORIES))
        .withColumn("unit_cost", F.round(F.rand(7) * 80 + 5, 2))
    )


def make_stores(spark: SparkSession, n: int = 100) -> DataFrame:
    return (
        spark.range(1, n + 1)
        .withColumnRenamed("id", "store_id")
        .withColumn("store_name", F.concat(F.lit("Store "), F.col("store_id")))
        .withColumn("region", _pick("store_id", REGIONS))
    )


def make_transactions(
    spark: SparkSession,
    rows: int,
    day: str,
    n_customers: int = 50_000,
    n_products: int = 2_000,
    n_stores: int = 100,
    seed: int = 42,
) -> DataFrame:
    df = (
        spark.range(0, rows)
        .withColumn("transaction_id", F.concat(F.lit(day.replace("-", "")), F.lit("-"), F.lpad(F.col("id"), 10, "0")))
        .withColumn("customer_id", (F.rand(seed) * n_customers).cast("long") + 1)
        .withColumn("product_id", (F.rand(seed + 1) * n_products).cast("long") + 1)
        .withColumn("store_id", (F.rand(seed + 2) * n_stores).cast("long") + 1)
        .withColumn("quantity", (F.rand(seed + 3) * 5).cast("int") + 1)
        .withColumn("unit_price", F.round(F.rand(seed + 4) * 195 + 5, 2))
        .withColumn("discount_pct", F.round(F.rand(seed + 5) * 0.3, 2))
        .withColumn("payment_method", _pick("id", PAYMENTS))
        .withColumn("status", _pick("id", STATUSES))
        .withColumn(
            "event_ts",
            (F.to_timestamp(F.lit(day)).cast("long") + (F.rand(seed + 6) * 86399).cast("long")).cast("timestamp"),
        )
        .drop("id")
    )

    # Inject defects.
    r = F.rand(seed + 9)
    df = (
        df.withColumn("customer_id", F.when(r < 0.005, F.lit(None)).otherwise(F.col("customer_id")))
        .withColumn("quantity", F.when((r >= 0.005) & (r < 0.008), F.lit(0)).otherwise(F.col("quantity")))
        .withColumn("unit_price", F.when((r >= 0.008) & (r < 0.011), -F.col("unit_price")).otherwise(F.col("unit_price")))
    )
    # Duplicates: re-emit ~1% of rows.
    dupes = df.sample(fraction=0.01, seed=seed)
    return df.unionByName(dupes)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--out", required=True, help="Landing root (local path or /Volumes/...)")
    p.add_argument("--rows", type=int, default=1_000_000)
    p.add_argument("--date", required=True, help="YYYY-MM-DD")
    p.add_argument("--with-dims", action="store_true", help="Also (re)generate dimension snapshots")
    a = p.parse_args()

    spark = get_spark("generate_data")
    if a.with_dims:
        make_customers(spark).write.mode("overwrite").json(f"{a.out}/customers")
        make_products(spark).write.mode("overwrite").json(f"{a.out}/products")
        make_stores(spark).write.mode("overwrite").json(f"{a.out}/stores")
    (
        make_transactions(spark, a.rows, a.date)
        .repartition(8)
        .write.mode("overwrite")
        .json(f"{a.out}/transactions/dt={a.date}")
    )


if __name__ == "__main__":
    main()
