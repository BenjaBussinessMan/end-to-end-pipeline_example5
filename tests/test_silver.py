from datetime import datetime

from lakehouse.silver.transformations import TRANSACTION_RULES, deduplicate, standardize_transactions
from lakehouse.utils.dq import split_valid_invalid

COLS = ["transaction_id", "customer_id", "product_id", "store_id", "quantity", "unit_price",
        "discount_pct", "payment_method", "status", "event_ts", "_ingest_ts"]


def _df(spark, rows):
    return spark.createDataFrame(rows, COLS)


def _row(tid="T1", cust=1, qty=2, price=10.0, disc=0.1, status="completed", ingest=datetime(2025, 1, 1, 10)):
    return (tid, cust, 5, 7, qty, price, disc, " card ", status, "2025-01-01 09:00:00", ingest)


def test_standardize_computes_net_amount_and_normalizes(spark):
    out = standardize_transactions(_df(spark, [_row()])).first()
    assert float(out["net_amount"]) == 18.0  # 2 * 10 * 0.9
    assert out["payment_method"] == "CARD" and out["status"] == "COMPLETED"


def test_deduplicate_keeps_latest_ingest(spark):
    df = _df(spark, [_row(qty=1, ingest=datetime(2025, 1, 1, 10)), _row(qty=9, ingest=datetime(2025, 1, 1, 11))])
    out = deduplicate(df).collect()
    assert len(out) == 1 and out[0]["quantity"] == 9


def test_quarantine_routes_bad_rows_with_rule_names(spark):
    rows = [_row("OK"), _row("B1", cust=None), _row("B2", qty=0), _row("B3", price=-1.0)]
    valid, quarantine = split_valid_invalid(standardize_transactions(_df(spark, rows)), TRANSACTION_RULES)
    assert [r["transaction_id"] for r in valid.collect()] == ["OK"]
    failed = {r["transaction_id"]: r["_failed_rules"] for r in quarantine.collect()}
    assert failed["B1"] == ["customer_id_not_null"]
    assert failed["B2"] == ["quantity_positive"]
    assert failed["B3"] == ["unit_price_non_negative"]
