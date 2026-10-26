"""Task 7: Entity Resolution - Reused Account IDs."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def resolve_entities(
    incoming_df: DataFrame,
    existing_silver_df: DataFrame,
) -> DataFrame:
    """Assigns a new surrogate identity to incoming rows whose account_id collides
    with an existing silver entity representing a different physical customer.

    Args:
        incoming_df: Incoming accounts batch.
        existing_silver_df: Existing silver billing accounts.

    Returns:
        DataFrame with resolved account_id values.
    """
    # Extract existing identities without touching masked fields
    existing_identities = existing_silver_df.select(
        F.col("account_id"),
        F.col("customer_name").alias("_ex_customer_name"),
        F.col("source_system").alias("_ex_source_system"),
    ).dropDuplicates(["account_id"])

    joined = incoming_df.join(
        existing_identities,
        on="account_id",
        how="left",
    )

    # Inconsistent non-key attributes indicate a distinct physical entity
    is_conflict = (
        F.col("_ex_customer_name").isNotNull()
        & (
            (F.col("customer_name") != F.col("_ex_customer_name"))
            | (F.col("source_system") != F.col("_ex_source_system"))
        )
    )

    orig_cols = incoming_df.columns

    resolved_df = joined.withColumn(
        "account_id",
        F.when(
            is_conflict,
            F.concat(F.col("account_id"), F.lit("-"), F.col("source_system")),
        ).otherwise(F.col("account_id")),
    ).select(orig_cols)

    return resolved_df
