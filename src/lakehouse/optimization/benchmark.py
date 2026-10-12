"""Reproducible query benchmark: baseline vs optimized layout.

Baseline  : copy of silver.transactions written as many small, unclustered files,
            queried with a plain join and the result cache disabled.
Optimized : OPTIMIZE'd, Liquid-Clustered fact_sales + pre-aggregated mart_daily_sales.

Each query runs N times (first run discarded as warm-up); the median is reported.
Results are appended to governance.benchmark_results. The improvement percentage is
computed from measurements - it is not hard-coded anywhere.
"""
import statistics
import time

from pyspark.sql import functions as F

from lakehouse.config import get_config
from lakehouse.utils.spark import get_spark

RUNS = 5

# name -> (baseline SQL, optimized SQL). {b} = baseline txn table, placeholders resolved below.
QUERIES = {
    "q1_revenue_by_region_day": (
        """SELECT t.event_date, s.region, SUM(t.net_amount) revenue
           FROM {b} t JOIN {silver_stores} s ON t.store_id = s.store_id
           WHERE t.status = 'COMPLETED' GROUP BY t.event_date, s.region""",
        """SELECT d.date, m.region, SUM(m.revenue) revenue
           FROM {mart} m JOIN {dim_date} d ON m.date_key = d.date_key GROUP BY d.date, m.region""",
    ),
    "q2_top_categories": (
        """SELECT p.category, SUM(t.net_amount) revenue
           FROM {b} t JOIN {silver_products} p ON t.product_id = p.product_id
           WHERE t.status = 'COMPLETED' GROUP BY p.category ORDER BY revenue DESC""",
        """SELECT category, SUM(revenue) revenue FROM {mart} GROUP BY category ORDER BY revenue DESC""",
    ),
    "q3_single_store_one_day": (
        """SELECT SUM(net_amount) FROM {b} WHERE store_id = 42 AND event_date = '{day}'""",
        """SELECT SUM(net_amount) FROM {fact} WHERE store_id = 42 AND date_key = {day_key}""",
    ),
    "q4_customer_history": (
        """SELECT * FROM {b} WHERE customer_id = 12345 ORDER BY event_ts DESC LIMIT 100""",
        """SELECT * FROM {fact} WHERE customer_id = 12345 ORDER BY event_ts DESC LIMIT 100""",
    ),
}


def timed(spark, sql: str) -> float:
    t0 = time.perf_counter()
    spark.sql(sql).write.format("noop").mode("overwrite").save()  # force full execution
    return time.perf_counter() - t0


def main() -> None:
    cfg = get_config()
    spark = get_spark("benchmark")
    spark.conf.set("spark.databricks.io.cache.enabled", "false")

    baseline = cfg.table("gold", "_bench_txn_baseline")
    spark.table(cfg.table("silver", "transactions")).repartition(400).write.format("delta").mode(
        "overwrite"
    ).option("overwriteSchema", "true").option("delta.autoOptimize.autoCompact", "false").saveAsTable(baseline)

    fact = cfg.table("gold", "fact_sales")
    spark.sql(f"OPTIMIZE {fact}")
    spark.sql(f"ANALYZE TABLE {fact} COMPUTE STATISTICS FOR ALL COLUMNS")

    day = spark.table(cfg.table("silver", "transactions")).agg(F.max("event_date")).first()[0]
    ctx = dict(
        b=baseline, fact=fact, mart=cfg.table("gold", "mart_daily_sales"), dim_date=cfg.table("gold", "dim_date"),
        silver_stores=cfg.table("silver", "stores"), silver_products=cfg.table("silver", "products"),
        day=str(day), day_key=str(day).replace("-", ""),
    )

    rows = []
    for name, (base_sql, opt_sql) in QUERIES.items():
        med = {}
        for label, sql in (("baseline", base_sql), ("optimized", opt_sql)):
            sql = sql.format(**ctx)
            timed(spark, sql)  # warm-up
            med[label] = statistics.median(timed(spark, sql) for _ in range(RUNS))
        gain = (1 - med["optimized"] / med["baseline"]) * 100
        rows.append((name, med["baseline"], med["optimized"], gain))
        print(f"{name}: baseline={med['baseline']:.2f}s optimized={med['optimized']:.2f}s gain={gain:.1f}%")

    avg_gain = sum(r[3] for r in rows) / len(rows)
    print(f"Average improvement: {avg_gain:.1f}%")

    (
        spark.createDataFrame(rows, "query STRING, baseline_s DOUBLE, optimized_s DOUBLE, improvement_pct DOUBLE")
        .withColumn("run_ts", F.current_timestamp())
        .write.format("delta").mode("append").saveAsTable(cfg.table("governance", "benchmark_results"))
    )
    spark.sql(f"DROP TABLE IF EXISTS {baseline}")


if __name__ == "__main__":
    main()
