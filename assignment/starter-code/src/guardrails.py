"""Task 8: Pipeline Guardrails.

Enforces cross-cutting silver quality invariants:
  - row-count conservation (input = silver + quarantine + true dups skipped)
  - controlled quarantine reason codes (see config.QUARANTINE_REASONS)
  - masked-field passthrough integrity (email_masked never altered)
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src import config


def assert_row_count_conservation(
    input_count: int,
    silver_count: int,
    quarantine_count: int,
    true_dup_count: int = 0,
) -> None:
    """Raises AssertionError if input_count != silver_count + quarantine_count + true_dup_count."""
    expected_count = silver_count + quarantine_count + true_dup_count
    if input_count != expected_count:
        raise AssertionError(
            f"Row count conservation violated: input ({input_count}) != "
            f"silver ({silver_count}) + quarantine ({quarantine_count}) + "
            f"true_duplicates_skipped ({true_dup_count}) = {expected_count}"
        )


def assert_masked_field_unchanged(
    before_df: DataFrame,
    after_df: DataFrame,
    key_col: str,
    masked_col: str = "email_masked",
) -> None:
    """Raises AssertionError if masked_col differs for any matching key_col row
    between before_df and after_df.
    """
    if masked_col not in before_df.columns or masked_col not in after_df.columns:
        return

    joined = before_df.select(key_col, masked_col).join(
        after_df.select(key_col, F.col(masked_col).alias(f"_after_{masked_col}")),
        on=key_col,
        how="inner",
    )

    mismatches = joined.filter(
        (F.col(masked_col) != F.col(f"_after_{masked_col}"))
        | (F.col(masked_col).isNull() & F.col(f"_after_{masked_col}").isNotNull())
        | (F.col(masked_col).isNotNull() & F.col(f"_after_{masked_col}").isNull())
    )

    mismatch_count = mismatches.count()
    if mismatch_count > 0:
        sample = mismatches.limit(5).collect()
        raise AssertionError(
            f"Masked field '{masked_col}' was altered for {mismatch_count} rows. Sample: {sample}"
        )


def assert_valid_quarantine_reason(
    quarantine_df: DataFrame,
    reason_col: str = "quarantine_reason",
) -> None:
    """Raises AssertionError if any value in reason_col is null, empty, or not in
    config.QUARANTINE_REASONS.values().
    """
    if quarantine_df.count() == 0:
        return

    actual_col = reason_col
    if actual_col not in quarantine_df.columns:
        if "quarantine_reason" in quarantine_df.columns:
            actual_col = "quarantine_reason"
        elif "reason" in quarantine_df.columns:
            actual_col = "reason"
        else:
            raise AssertionError(
                f"Quarantine reason column '{reason_col}' not found in DataFrame columns: {quarantine_df.columns}"
            )

    valid_reasons = set(config.QUARANTINE_REASONS.values())
    invalid_rows = quarantine_df.filter(
        F.col(actual_col).isNull()
        | (~F.col(actual_col).isin(list(valid_reasons)))
    )

    invalid_count = invalid_rows.count()
    if invalid_count > 0:
        sample = [row[actual_col] for row in invalid_rows.limit(5).collect()]
        raise AssertionError(
            f"Found {invalid_count} quarantined rows with invalid reason codes: {sample}. "
            f"Allowed reasons are: {valid_reasons}"
        )
