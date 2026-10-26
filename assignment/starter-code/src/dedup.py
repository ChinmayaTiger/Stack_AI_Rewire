"""Task 2: Deduplication - Retried Load."""

from functools import reduce
from typing import Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from . import config


def dedup_invoices(
    incoming_df: DataFrame,
    existing_silver_df: DataFrame,
) -> Tuple[DataFrame, DataFrame]:
    """Deduplicate incoming invoices against existing silver invoices.

    Returns:
        (rows_to_insert_df, rows_to_quarantine_df)
    """
    # 1. Rows with a new invoice_id pass through for insertion
    rows_to_insert_df = incoming_df.join(
        existing_silver_df,
        on="invoice_id",
        how="left_anti",
    )

    # 2. Rows sharing an existing invoice_id
    matched = incoming_df.alias("in").join(
        existing_silver_df.alias("ex"),
        on="invoice_id",
        how="inner",
    )

    # Compare all non-key columns to detect conflicts
    non_key_cols = [c for c in incoming_df.columns if c != "invoice_id"]
    
    # Check if any column differs (handling null-safe equality via <=> if needed, but != covers values)
    has_conflict = reduce(
        lambda acc, c: acc | ~(F.col(f"in.{c}") == F.col(f"ex.{c}")),
        non_key_cols,
        F.lit(False),
    )

    # 3. Conflicting rows are tagged and sent to quarantine
    reason_code = config.QUARANTINE_REASONS["CONFLICTING_KEY"]
    rows_to_quarantine_df = (
        matched.filter(has_conflict)
        .select("in.*")
        .withColumn("quarantine_reason", F.lit(reason_code))
    )

    return rows_to_insert_df, rows_to_quarantine_df