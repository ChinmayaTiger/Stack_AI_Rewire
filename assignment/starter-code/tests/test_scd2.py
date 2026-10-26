"""Task 9: tests for Task 5/Task 6 (SCD2 History Tracking)."""

from datetime import datetime, timedelta
import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

from src import config, ingest
from src.scd2_writer import apply_plan_change
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
    """Initial silver billing_accounts reflecting Batch 3 types and SCD2 columns."""
    # 1. Baseline batch 1
    raw_b1 = ingest.read_billing_accounts(spark_session)
    casted_b1, _ = cast_billing_accounts(raw_b1)

    # 2. Batch 3 (date format v2) normalized types
    batch3_path = (
        config.DATA_DIR
        / "sample_batches"
        / "batch_3_date_format_v2"
        / "billing_accounts_batch3_date_format_v2.csv"
    )
    if batch3_path.exists():
        raw_b3 = ingest.read_billing_accounts(spark_session, path=batch3_path)
        casted_b3, _ = cast_billing_accounts(raw_b3)
        combined_df = casted_b1.unionByName(casted_b3)
    else:
        combined_df = casted_b1

    # 3. Populate SCD2 initial interval tracking columns
    return (
        combined_df.withColumn("valid_from", F.col("signup_date"))
        .withColumn("valid_to", F.lit(None).cast(DateType()))
        .withColumn("is_current", F.lit(True))
    )


@pytest.fixture(scope="module")
def batch4_df(spark_session):
    """Load Batch 4 plan change events."""
    batch4_path = (
        config.DATA_DIR
        / "sample_batches"
        / "batch_4_plan_change"
        / "billing_accounts_batch4_plan_change.csv"
    )
    return (
        spark_session.read.format("csv")
        .option("header", "true")
        .load(str(batch4_path))
    )


def test_normal_plan_change_closes_prior_row_and_opens_new_row(
    baseline_silver_df, batch4_df
):
    """For a normally-ordered effective_date, assert the prior row is
    closed (valid_to set, is_current False) and exactly one new row is
    is_current True.
    """
    updated_silver, quarantine = apply_plan_change(batch4_df, baseline_silver_df)

    quarantined_ids = [r.account_id for r in quarantine.collect()]
    valid_changes = batch4_df.filter(~F.col("account_id").isin(quarantined_ids)).collect()
    assert len(valid_changes) > 0

    sample = valid_changes[0]
    acc_id = sample.account_id
    expected_new_plan = sample.new_plan_id.strip().upper()

    acc_rows = updated_silver.filter(F.col("account_id") == acc_id).collect()
    assert len(acc_rows) >= 2

    closed_rows = [r for r in acc_rows if not r.is_current]
    active_rows = [r for r in acc_rows if r.is_current]

    assert len(active_rows) == 1
    assert len(closed_rows) >= 1

    active_row = active_rows[0]
    closed_row = closed_rows[-1]

    assert active_row.plan_id == expected_new_plan
    assert active_row.valid_to is None
    assert closed_row.valid_to is not None
    assert closed_row.valid_to == active_row.valid_from


def test_exactly_one_current_row_per_account_after_batch4(
    baseline_silver_df, batch4_df
):
    """Assert every account_id in silver billing_accounts has exactly
    one is_current = True row after processing Batch 4.
    """
    updated_silver, _ = apply_plan_change(batch4_df, baseline_silver_df)

    current_counts = (
        updated_silver.filter(F.col("is_current") == True)
        .groupBy("account_id")
        .count()
    )

    invalid_counts = current_counts.filter(F.col("count") != 1)
    assert invalid_counts.count() == 0


def test_history_queryable_by_date(baseline_silver_df, batch4_df):
    """Assert querying an account's plan_id as of a date before its
    change returns the old plan, and as of a date on/after returns the new
    plan.
    """
    updated_silver, quarantine = apply_plan_change(batch4_df, baseline_silver_df)

    quarantined_ids = [r.account_id for r in quarantine.collect()]
    valid_changes = batch4_df.filter(~F.col("account_id").isin(quarantined_ids)).collect()
    assert len(valid_changes) > 0

    sample = valid_changes[0]
    acc_id = sample.account_id
    eff_date_str = sample.effective_date

    try:
        eff_date = datetime.strptime(eff_date_str, "%Y-%m-%d").date()
    except ValueError:
        eff_date = datetime.strptime(eff_date_str, "%m/%d/%Y").date()

    before_date = eff_date - timedelta(days=1)
    after_date = eff_date + timedelta(days=1)

    # Lookup helper: valid_from <= date and (valid_to is null or date < valid_to)
    def query_plan_at(query_date):
        res = updated_silver.filter(
            (F.col("account_id") == acc_id)
            & (F.col("valid_from") <= query_date)
            & (F.col("valid_to").isNull() | (query_date < F.col("valid_to")))
        ).collect()
        return res[0].plan_id if res else None

    assert query_plan_at(before_date) == sample.old_plan_id.strip().upper()
    assert query_plan_at(eff_date) == sample.new_plan_id.strip().upper()
    assert query_plan_at(after_date) == sample.new_plan_id.strip().upper()


def test_out_of_order_effective_date_is_handled_or_quarantined(
    baseline_silver_df, batch4_df
):
    """Assert the out-of-order row from Batch 4 either produces
    correct, non-overlapping history per your modified skill, or is
    quarantined with reason
    config.QUARANTINE_REASONS['OUT_OF_ORDER_EFFECTIVE_DATE'] - and in either
    case does not corrupt (e.g. create two open rows for) the account.
    """
    updated_silver, quarantine = apply_plan_change(batch4_df, baseline_silver_df)

    # 1. Exactly one row in Batch 4 is out of order
    assert quarantine.count() == 1
    q_row = quarantine.collect()[0]
    assert q_row.quarantine_reason == config.QUARANTINE_REASONS["OUT_OF_ORDER_EFFECTIVE_DATE"]

    # 2. Silver history for that quarantined account is not corrupted
    quarantined_acc_id = q_row.account_id
    acc_rows = updated_silver.filter(F.col("account_id") == quarantined_acc_id).collect()
    current_rows = [r for r in acc_rows if r.is_current]

    assert len(current_rows) == 1
    assert current_rows[0].valid_to is None
