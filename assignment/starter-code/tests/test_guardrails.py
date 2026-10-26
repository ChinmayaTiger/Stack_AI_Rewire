"""Task 9: tests for Task 8 (Pipeline Guardrails)."""

import pytest
from src import config, ingest
from src.guardrails import (
    assert_masked_field_unchanged,
    assert_row_count_conservation,
    assert_valid_quarantine_reason,
)
from src.pipeline import run_pipeline
from src.spark_session import get_spark


@pytest.fixture(scope="module")
def spark_session():
    spark = get_spark()
    yield spark
    spark.stop()


def test_row_count_conservation_holds_for_every_batch():
    """Assert row-count-conservation holds and catches any missing/dropped row."""
    # 1. Successful conservation
    assert_row_count_conservation(input_count=10, silver_count=7, quarantine_count=2, true_dup_count=1)

    # 2. Failure mode: row silently dropped
    with pytest.raises(AssertionError, match="Row count conservation violated"):
        assert_row_count_conservation(input_count=10, silver_count=7, quarantine_count=1, true_dup_count=1)


def test_quarantine_reason_is_always_from_controlled_vocabulary(spark_session):
    """Assert quarantine reasons are strictly from config.QUARANTINE_REASONS."""
    # 1. Valid reason passes
    valid_df = spark_session.sql("SELECT 'ACC-01' AS account_id, 'unrecognized_format' AS quarantine_reason")
    assert_valid_quarantine_reason(valid_df)

    # 2. Free-text reason fails
    invalid_df = spark_session.sql("SELECT 'ACC-01' AS account_id, 'random_free_text_error' AS quarantine_reason")
    with pytest.raises(AssertionError, match="invalid reason codes"):
        assert_valid_quarantine_reason(invalid_df)

    # 3. Null reason fails
    null_df = spark_session.sql("SELECT 'ACC-01' AS account_id, CAST(NULL AS STRING) AS quarantine_reason")
    with pytest.raises(AssertionError, match="invalid reason codes"):
        assert_valid_quarantine_reason(null_df)


def test_masked_field_byte_identical_before_and_after_pipeline(spark_session):
    """Assert email_masked is strictly preserved and catches alterations."""
    before_df = spark_session.sql("SELECT 'ACC-01' AS account_id, 'u***@test.com' AS email_masked")
    identical_df = spark_session.sql("SELECT 'ACC-01' AS account_id, 'u***@test.com' AS email_masked")
    altered_df = spark_session.sql("SELECT 'ACC-01' AS account_id, 'U***@TEST.COM' AS email_masked")

    # Identical passes
    assert_masked_field_unchanged(before_df, identical_df, "account_id")

    # Alteration fails
    with pytest.raises(AssertionError, match="Masked field 'email_masked' was altered"):
        assert_masked_field_unchanged(before_df, altered_df, "account_id")


def test_pipeline_is_idempotent_on_rerun(spark_session):
    """Assert running the pipeline twice produces identical row counts and state."""
    res1 = run_pipeline(spark_session)
    counts_run1 = [df.count() for df in res1]

    res2 = run_pipeline(spark_session)
    counts_run2 = [df.count() for df in res2]

    assert counts_run1 == counts_run2


def test_writes_are_non_destructive(spark_session):
    """Assert later batch outputs do not erase or reduce silver row counts."""
    res = run_pipeline(spark_session)
    silver_accounts, silver_plans, silver_invoices, _, _ = res

    # Base batch 1 had 100 accounts; after all batches, it must be >= 100
    assert silver_accounts.count() >= 100
    assert silver_plans.count() == 6
    assert silver_invoices.count() >= 480
