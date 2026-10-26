"""Task 3: Type Casting - Inconsistent Formats.

Read skills/type_casting/SKILL.md (used as-is, no modification needed).

Implements cast_billing_accounts() so that:
  - signup_date is cast to a consistent date type across legacy_v1
    (YYYY-MM-DD) and platform_v2 (MM/DD/YYYY or YYYY-MM-DD) rows
  - plan_id casing is normalized to one canonical uppercase representation
  - a row whose signup_date matches neither known pattern is NOT cast and is
    routed to quarantine with reason UNRECOGNIZED_FORMAT
    (see config.QUARANTINE_REASONS)
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from src import config


def cast_billing_accounts(incoming_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Casts and normalizes billing accounts, splitting clean rows from unrecognized formats.

    Returns:
        (casted_df, rows_to_quarantine_df)
    """
    # 1. Parse date based on known source system patterns
    # legacy_v1 uses yyyy-MM-dd; platform_v2 uses MM/dd/yyyy or yyyy-MM-dd
    parsed_date_col = (
        F.when(
            F.col("source_system") == "legacy_v1",
            F.to_date(F.col("signup_date"), "yyyy-MM-dd"),
        )
        .when(
            F.col("source_system") == "platform_v2",
            F.coalesce(
                F.to_date(F.col("signup_date"), "MM/dd/yyyy"),
                F.to_date(F.col("signup_date"), "yyyy-MM-dd"),
            ),
        )
        .otherwise(F.lit(None))
    )

    df_with_parsed = incoming_df.withColumn("_parsed_signup_date", parsed_date_col)

    # 2. Identify unrecognized formats: original string was not null, but parsed date is null
    is_unrecognized = (
        F.col("_parsed_signup_date").isNull() & F.col("signup_date").isNotNull()
    )

    # 3. Build Quarantine DataFrame: keep original columns + quarantine_reason
    quarantine_df = (
        df_with_parsed.filter(is_unrecognized)
        .drop("_parsed_signup_date")
        .withColumn(
            "quarantine_reason",
            F.lit(config.QUARANTINE_REASONS["UNRECOGNIZED_FORMAT"]),
        )
    )

    # 4. Build Clean Casted DataFrame:
    #    - Replace signup_date with parsed DateType column
    #    - Canonicalize plan_id casing to uppercase
    casted_df = (
        df_with_parsed.filter(~is_unrecognized)
        .withColumn("signup_date", F.col("_parsed_signup_date"))
        .drop("_parsed_signup_date")
        .withColumn("plan_id", F.upper(F.trim(F.col("plan_id"))))
    )

    return casted_df, quarantine_df
