from lakehouse.gold.build_marts import build_daily_sales, build_fact_sales


def test_fact_excludes_non_completed_and_computes_margin(spark):
    txn = spark.createDataFrame(
        [
            ("T1", 1, 10, 1, 2, "CARD", "COMPLETED", "2025-01-01", 20.0, 10.0, 0.0),
            ("T2", 2, 10, 1, 1, "CARD", "CANCELLED", "2025-01-01", 5.0, 5.0, 0.0),
        ],
        "transaction_id string, customer_id long, product_id long, store_id long, quantity int, "
        "payment_method string, status string, d string, net_amount double, unit_price double, discount_pct double",
    ).selectExpr(
        "transaction_id", "customer_id", "product_id", "store_id", "quantity", "payment_method", "status",
        "to_date(d) as event_date", "to_timestamp(d) as event_ts", "cast(net_amount as decimal(14,2)) net_amount",
        "cast(unit_price as decimal(12,2)) unit_price", "cast(discount_pct as decimal(4,2)) discount_pct",
    )
    products = spark.createDataFrame([(10, "Hogar", 4.0)], "product_id long, category string, unit_cost double")
    stores = spark.createDataFrame([(1, "NORTE")], "store_id long, region string")

    fact = build_fact_sales(txn, products)
    assert fact.count() == 1
    assert float(fact.first()["margin_amount"]) == 12.0  # 20 - 2 * 4

    mart = build_daily_sales(fact, stores, products).first()
    assert mart["region"] == "NORTE" and float(mart["revenue"]) == 20.0 and mart["transactions"] == 1
