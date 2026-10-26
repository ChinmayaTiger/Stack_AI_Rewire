"""Task 9: tests for Task 3 (Type Casting)."""

from datetime import date
import pytest
from pyspark.sql.types import DateType
from src import config, ingest
from src.spark_session import get_spark
from src.type_casting import cast_billing_accounts


@pytest.fixture(scope="module")
def spark_session():
    """Shared SparkSession for test execution."""
    spark = get_spark()
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def batch3_df(spark_session):
    """Load Batch 3 billing accounts."""
    batch3_path = (
        config.DATA_DIR
        / "sample_batches"
        / "batch_3_date_format_v2"
        / "billing_accounts_batch3_date_format_v2.csv"
    )
    return ingest.read_billing_accounts(spark_session, path=batch3_path)


def test_legacy_v1_date_format_casts_correctly(spark_session):
    """A YYYY-MM-DD signup_date casts to the correct date value."""
    df = spark_session.sql("""
        SELECT 'ACC-9001' AS account_id,
               'Alice' AS customer_name,
               'a***@test.com' AS email_masked,
               '2025-05-15' AS signup_date,
               'legacy_v1' AS source_system,
               'PLAN-GOLD' AS plan_id,
               'active' AS status
    """)
    casted, quarantine = cast_billing_accounts(df)

    assert quarantine.count() == 0
    row = casted.collect()[0]
    assert row.signup_date == date(2025, 5, 15)
    assert isinstance(casted.schema["signup_date"].dataType, DateType)


def test_platform_v2_date_format_casts_correctly(spark_session):
    """A MM/DD/YYYY signup_date casts to the correct date value,

    equal to the legacy_v1 format's result for the same logical date.
    """
    df = spark_session.sql("""
        SELECT 'ACC-9002' AS account_id,
               'Bob' AS customer_name,
               'b***@test.com' AS email_masked,
               '05/15/2025' AS signup_date,
               'platform_v2' AS source_system,
               'PLAN-GOLD' AS plan_id,
               'active' AS status
    """)
    casted, quarantine = cast_billing_accounts(df)

    assert quarantine.count() == 0
    row = casted.collect()[0]
    assert row.signup_date == date(2025, 5, 15)


def test_plan_id_casing_normalized(spark_session):
    """'gold' and 'GOLD' (or similar casing variants) both normalize to

    the same canonical plan_id.
    """
    df = spark_session.sql("""
        SELECT 'ACC-9003' AS account_id,
               'Carol' AS customer_name,
               'c***@test.com' AS email_masked,
               '2025-01-01' AS signup_date,
               'legacy_v1' AS source_system,
               'plan-gold' AS plan_id,
               'active' AS status
        UNION ALL
        SELECT 'ACC-9004' AS account_id,
               'Dave' AS customer_name,
               'd***@test.com' AS email_masked,
               '01/01/2025' AS signup_date,
               'platform_v2' AS source_system,
               'PLAN-GOLD' AS plan_id,
               'active' AS status
    """)
    casted, quarantine = cast_billing_accounts(df)

    plan_ids = [row.plan_id for row in casted.collect()]
    assert set(plan_ids) == {"PLAN-GOLD"}


def test_unrecognized_format_is_quarantined_not_guessed(batch3_df):
    """The long-form textual date row from Batch 3 is not cast to any

    date value and is routed to quarantine with reason
    config.QUARANTINE_REASONS['UNRECOGNIZED_FORMAT'].
    """
    casted, quarantine = cast_billing_accounts(batch3_df)

    assert quarantine.count() == 1
    q_row = quarantine.collect()[0]
    assert q_row.quarantine_reason == config.QUARANTINE_REASONS["UNRECOGNIZED_FORMAT"]
    assert "quarantine_reason" in quarantine.columns


def test_batch3_expected_row_counts(batch3_df):
    """Assert silver billing_accounts gains exactly 3 rows (not 4) from

    Batch 3, and quarantine gains exactly 1.
    """
    casted, quarantine = cast_billing_accounts(batch3_df)

    assert batch3_df.count() == 4
    assert casted.count() == 3
    assert quarantine.count() == 1
