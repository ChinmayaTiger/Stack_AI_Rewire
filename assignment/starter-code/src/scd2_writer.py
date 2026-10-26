"""Task 5/Task 6: SCD2 History Tracking - Plan Change."""

from typing import Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

from src import config


def apply_plan_change(
    change_events_df: DataFrame,
    current_silver_df: DataFrame,
) -> Tuple[DataFrame, DataFrame]:
    """Applies SCD2 plan change events to current silver billing accounts.

    Args:
        change_events_df: Incoming change events containing account_id,
            new_plan_id, effective_date.
        current_silver_df: Existing silver billing accounts.

    Returns:
        (updated_silver_df, rows_to_quarantine_df)
    """
    # 1. Ensure current_silver_df has SCD2 tracking columns initialized if not present
    silver_df = current_silver_df
    if "valid_from" not in silver_df.columns:
        silver_df = silver_df.withColumn(
            "valid_from",
            F.coalesce(
                F.to_date(F.col("signup_date"), "yyyy-MM-dd"),
                F.to_date(F.col("signup_date"), "MM/dd/yyyy"),
                F.to_date(F.col("signup_date")),
            ),
        )
    if "valid_to" not in silver_df.columns:
        valid_from_dtype = silver_df.schema["valid_from"].dataType
        silver_df = silver_df.withColumn("valid_to", F.lit(None).cast(valid_from_dtype))
    if "is_current" not in silver_df.columns:
        silver_df = silver_df.withColumn("is_current", F.lit(True))

    valid_from_is_date = isinstance(silver_df.schema["valid_from"].dataType, DateType)

    # 2. Normalize change events: parse effective_date and normalize new_plan_id
    parsed_eff_date_col = F.coalesce(
        F.to_date(F.col("effective_date"), "yyyy-MM-dd"),
        F.to_date(F.col("effective_date"), "MM/dd/yyyy"),
        F.to_date(F.col("effective_date")),
    )

    changes_normalized = (
        change_events_df.withColumn("_eff_date", parsed_eff_date_col)
        .withColumn("new_plan_id", F.upper(F.trim(F.col("new_plan_id"))))
    )

    # 3. Match against current active records in silver to detect ordering
    current_active = silver_df.filter(F.col("is_current") == True).select(
        F.col("account_id"),
        F.coalesce(
            F.to_date(F.col("valid_from"), "yyyy-MM-dd"),
            F.to_date(F.col("valid_from")),
        ).alias("_cur_valid_from"),
    )

    changes_with_cur = changes_normalized.join(
        current_active,
        on="account_id",
        how="left",
    )

    # Out of order: effective_date < current active valid_from
    is_out_of_order = (
        F.col("_cur_valid_from").isNotNull()
        & (F.col("_eff_date") < F.col("_cur_valid_from"))
    )

    # 4. Quarantine out-of-order changes
    quarantine_reason = config.QUARANTINE_REASONS["OUT_OF_ORDER_EFFECTIVE_DATE"]
    quarantine_df = (
        changes_with_cur.filter(is_out_of_order)
        .select(change_events_df.columns)
        .withColumn("quarantine_reason", F.lit(quarantine_reason))
    )

    # 5. Valid changes to apply
    valid_changes = changes_with_cur.filter(~is_out_of_order).drop("_cur_valid_from")

    # Match valid changes with current active silver rows
    valid_changes_for_join = valid_changes.select(
        F.col("account_id"),
        F.col("new_plan_id").alias("_chg_new_plan_id"),
        (
            F.col("_eff_date")
            if valid_from_is_date
            else F.date_format(F.col("_eff_date"), "yyyy-MM-dd")
        ).alias("_chg_eff_date"),
    )

    joined_silver = silver_df.join(
        valid_changes_for_join,
        on="account_id",
        how="left",
    )

    # Close out prior active rows for accounts undergoing valid change
    should_close = F.col("is_current") & F.col("_chg_new_plan_id").isNotNull()

    closed_silver = (
        joined_silver.withColumn(
            "valid_to",
            F.when(should_close, F.col("_chg_eff_date")).otherwise(F.col("valid_to")),
        )
        .withColumn(
            "is_current",
            F.when(should_close, F.lit(False)).otherwise(F.col("is_current")),
        )
    )

    # Generate new active rows carrying forward unchanged attributes
    new_rows = (
        joined_silver.filter(should_close)
        .withColumn("plan_id", F.col("_chg_new_plan_id"))
        .withColumn("valid_from", F.col("_chg_eff_date"))
        .withColumn("valid_to", F.lit(None).cast(silver_df.schema["valid_to"].dataType))
        .withColumn("is_current", F.lit(True))
    )

    # Drop temporary columns and union
    final_cols = silver_df.columns
    updated_silver_df = closed_silver.select(final_cols).unionByName(
        new_rows.select(final_cols)
    )

    return updated_silver_df, quarantine_df
