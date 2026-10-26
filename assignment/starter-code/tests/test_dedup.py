"""Task 9: tests for Task 2 (Deduplication).

Fill in each test body. Use small in-memory DataFrames built from
data/invoices.csv and data/sample_batches/batch_2_duplicate_retry/ - you do
not need to run the full pipeline to test this module in isolation.
"""

import pytest
from src import config, ingest
from src.dedup import dedup_invoices
from src.spark_session import get_spark


@pytest.fixture(scope="module")
def spark_session():
    """Shared SparkSession for test execution."""
    spark = get_spark()
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def invoices_baseline_and_batch2(spark_session):
    """Load baseline invoices (Batch 1) and Batch 2 incoming invoices."""
    silver_inv = ingest.read_invoices(spark_session)
    batch2_path = (
        config.DATA_DIR
        / "sample_batches"
        / "batch_2_duplicate_retry"
        / "invoices_batch2_duplicate_retry.csv"
    )
    incoming_inv = ingest.read_invoices(spark_session, path=batch2_path)
    return silver_inv, incoming_inv


def test_true_duplicate_is_skipped_not_inserted(invoices_baseline_and_batch2):
    """A byte-identical redelivered invoice row must not increase the

    silver invoices row count.
    """
    silver_inv, incoming_inv = invoices_baseline_and_batch2
    to_insert, _ = dedup_invoices(incoming_inv, silver_inv)

    assert to_insert.count() == 0


def test_conflicting_key_is_quarantined_not_inserted_or_overwritten(
    invoices_baseline_and_batch2,
):
    """A row sharing an existing invoice_id but with different

    amount_due/payment_status must land in quarantine, not silver.
    """
    silver_inv, incoming_inv = invoices_baseline_and_batch2
    to_insert, to_quarantine = dedup_invoices(incoming_inv, silver_inv)

    assert to_insert.count() == 0
    assert to_quarantine.count() == 4


def test_conflicting_key_quarantine_reason_is_correct(
    invoices_baseline_and_batch2,
):
    """Assert the quarantine reason for a conflicting-key row is exactly

    config.QUARANTINE_REASONS['CONFLICTING_KEY'].
    """
    silver_inv, incoming_inv = invoices_baseline_and_batch2
    _, to_quarantine = dedup_invoices(incoming_inv, silver_inv)

    reasons = [row.quarantine_reason for row in to_quarantine.collect()]
    expected_reason = config.QUARANTINE_REASONS["CONFLICTING_KEY"]
    assert len(reasons) == 4
    assert all(reason == expected_reason for reason in reasons)


def test_batch2_expected_row_counts(invoices_baseline_and_batch2):
    """Assert silver invoices count is unchanged from Batch 1, and

    quarantine count reflects the conflicting rows in Batch 2.
    """
    silver_inv, incoming_inv = invoices_baseline_and_batch2
    to_insert, to_quarantine = dedup_invoices(incoming_inv, silver_inv)

    assert incoming_inv.count() == 6
    assert to_insert.count() == 0
    assert to_quarantine.count() == 4
