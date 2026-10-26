"""Task 9: tests for Task 7 (Entity Resolution)."""

import pytest
from pyspark.sql import functions as F

from src import config, ingest
from src.entity_resolution import resolve_entities
from src.spark_session import get_spark
from src.type_casting import cast_billing_accounts


@pytest.fixture(scope="module")
def spark_session():
    """Shared SparkSession for test execution."""
    spark = get_spark()
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def baseline_silver_df(spark_session):
    """Baseline silver billing accounts."""
    raw_df = ingest.read_billing_accounts(spark_session)
    casted_df, _ = cast_billing_accounts(raw_df)
    return casted_df


@pytest.fixture(scope="module")
def batch5_df(spark_session):
    """Batch 5 containing reused account IDs."""
    batch5_path = (
        config.DATA_DIR
        / "sample_batches"
        / "batch_5_reused_account_id"
        / "billing_accounts_batch5_reused_account_id.csv"
    )
    raw_b5 = spark_session.read.format("csv").option("header", "true").load(str(batch5_path))
    casted_b5, _ = cast_billing_accounts(raw_b5)
    return casted_b5


def test_reused_account_id_gets_new_distinct_identity(baseline_silver_df, batch5_df):
    """For each of the 3 reused account_ids in Batch 5, assert the resulting
    silver row has a new, distinct identity rather than colliding with legacy_v1.
    """
    reused_raw_ids = [row.account_id for row in batch5_df.collect()]
    assert len(reused_raw_ids) == 3

    resolved_df = resolve_entities(batch5_df, baseline_silver_df)
    resolved_ids = [row.account_id for row in resolved_df.collect()]

    # None of the resolved IDs should match the original raw IDs that existed in silver
    for raw_id in reused_raw_ids:
        assert raw_id not in resolved_ids

    # Each new identity contains the surrogate suffix
    for resolved_id in resolved_ids:
        assert resolved_id.endswith("-platform_v2")


def test_original_legacy_account_history_untouched(baseline_silver_df, batch5_df):
    """Assert the original legacy_v1 account's existing silver rows
    are unchanged after entity resolution.
    """
    reused_raw_ids = [row.account_id for row in batch5_df.collect()]

    # Pre-resolution snapshot of the 3 accounts
    before_rows = {
        row.account_id: (row.customer_name, row.source_system, row.plan_id)
        for row in baseline_silver_df.filter(F.col("account_id").isin(reused_raw_ids)).collect()
    }

    _ = resolve_entities(batch5_df, baseline_silver_df)

    # Post-resolution check on baseline
    after_rows = {
        row.account_id: (row.customer_name, row.source_system, row.plan_id)
        for row in baseline_silver_df.filter(F.col("account_id").isin(reused_raw_ids)).collect()
    }

    assert before_rows == after_rows


def test_masked_email_never_exposed_via_join(baseline_silver_df, batch5_df):
    """Assert the new identity's email_masked is carried through unchanged
    and cannot be joined back to the original legacy_v1 account's email_masked.
    """
    resolved_df = resolve_entities(batch5_df, baseline_silver_df)

    # 1. Masked emails carried through verbatim from input
    orig_emails = sorted([r.email_masked for r in batch5_df.collect()])
    resolved_emails = sorted([r.email_masked for r in resolved_df.collect()])
    assert orig_emails == resolved_emails

    # 2. Joining old and new on email_masked produces zero matches
    reused_raw_ids = [row.account_id for row in batch5_df.collect()]
    legacy_reused = baseline_silver_df.filter(F.col("account_id").isin(reused_raw_ids))

    cross_email_matches = legacy_reused.join(
        resolved_df,
        on="email_masked",
        how="inner",
    )
    assert cross_email_matches.count() == 0
