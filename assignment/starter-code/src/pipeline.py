"""Orchestrates Batch 1 through Batch 5 end to end."""

import csv
import shutil
from pathlib import Path
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

from src import config, ingest
from src.audit import log_event
from src.dedup import dedup_invoices
from src.entity_resolution import resolve_entities
from src.guardrails import (
    assert_masked_field_unchanged,
    assert_row_count_conservation,
    assert_valid_quarantine_reason,
)
from src.scd2_writer import apply_plan_change
from src.spark_session import get_spark
from src.type_casting import cast_billing_accounts


def save_table(df: DataFrame, target_dir: Path) -> None:
    """Helper to reliably write a DataFrame to a destination CSV directory using pure Python."""
    target_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.rmtree(target_dir, ignore_errors=True)
    target_dir.mkdir(parents=True, exist_ok=True)

    columns = df.columns
    rows = df.collect()

    # Write both to target_dir.csv and target_dir/part.csv for flexibility
    for filepath in [Path(str(target_dir) + ".csv"), target_dir / "part.csv"]:
        with open(filepath, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            for row in rows:
                writer.writerow([row[c] if row[c] is not None else "" for c in columns])


def run_pipeline(spark: SparkSession):
    # Ensure output directories exist
    for d in [config.SILVER_DIR, config.QUARANTINE_DIR, config.ROOT / "outputs" / "artifacts"]:
        d.mkdir(parents=True, exist_ok=True)

    # -------------------------------------------------------------
    # BATCH 1: Initial Load
    # -------------------------------------------------------------
    raw_accounts_b1 = ingest.read_billing_accounts(spark)
    raw_plans_b1 = ingest.read_plan_catalog(spark)
    raw_invoices_b1 = ingest.read_invoices(spark)

    silver_accounts, _ = cast_billing_accounts(raw_accounts_b1)
    silver_accounts = (
        silver_accounts.withColumn("valid_from", F.col("signup_date"))
        .withColumn("valid_to", F.lit(None).cast(DateType()))
        .withColumn("is_current", F.lit(True))
    )
    silver_plans = raw_plans_b1
    silver_invoices = raw_invoices_b1

    log_event("billing_accounts", "batch_1", f"Loaded initial {silver_accounts.count()} rows")
    log_event("plan_catalog", "batch_1", f"Loaded initial {silver_plans.count()} rows")
    log_event("invoices", "batch_1", f"Loaded initial {silver_invoices.count()} rows")

    # -------------------------------------------------------------
    # BATCH 2: Invoices Deduplication & Retry
    # -------------------------------------------------------------
    b2_path = config.BATCHES_DIR / "batch_2_duplicate_retry" / "invoices_batch2_duplicate_retry.csv"
    b2_invoices = ingest.read_invoices(spark, path=b2_path)
    b2_count = b2_invoices.count()

    to_insert_inv, to_quarantine_inv_b2 = dedup_invoices(b2_invoices, silver_invoices)
    
    assert_valid_quarantine_reason(to_quarantine_inv_b2)
    q2_count = to_quarantine_inv_b2.count()
    ins2_count = to_insert_inv.count()
    true_dup_count = b2_count - (ins2_count + q2_count)

    assert_row_count_conservation(b2_count, ins2_count, q2_count, true_dup_count)
    log_event("invoices", "batch_2", f"Processed {b2_count} rows: {ins2_count} inserted, {q2_count} quarantined, {true_dup_count} duplicates skipped")

    quarantine_invoices = to_quarantine_inv_b2
    if ins2_count > 0:
        silver_invoices = silver_invoices.unionByName(to_insert_inv)

    # -------------------------------------------------------------
    # BATCH 3: Date Format v2 & Type Casting
    # -------------------------------------------------------------
    b3_path = config.BATCHES_DIR / "batch_3_date_format_v2" / "billing_accounts_batch3_date_format_v2.csv"
    b3_raw_accounts = ingest.read_billing_accounts(spark, path=b3_path)
    b3_count = b3_raw_accounts.count()

    b3_valid_accounts, b3_quarantine = cast_billing_accounts(b3_raw_accounts)
    assert_valid_quarantine_reason(b3_quarantine)
    
    assert_masked_field_unchanged(b3_raw_accounts, b3_valid_accounts, "account_id")
    assert_row_count_conservation(b3_count, b3_valid_accounts.count(), b3_quarantine.count(), 0)

    # Prepare batch 3 for silver union
    b3_valid_accounts = (
        b3_valid_accounts.withColumn("valid_from", F.col("signup_date"))
        .withColumn("valid_to", F.lit(None).cast(DateType()))
        .withColumn("is_current", F.lit(True))
    )
    silver_accounts = silver_accounts.unionByName(b3_valid_accounts)
    quarantine_accounts = b3_quarantine

    log_event("billing_accounts", "batch_3", f"Processed {b3_count} rows: {b3_valid_accounts.count()} valid, {b3_quarantine.count()} quarantined")

    # -------------------------------------------------------------
    # BATCH 4: SCD2 History Tracking - Plan Change
    # -------------------------------------------------------------
    b4_path = config.BATCHES_DIR / "batch_4_plan_change" / "billing_accounts_batch4_plan_change.csv"
    b4_raw = spark.read.format("csv").option("header", "true").load(str(b4_path))
    b4_count = b4_raw.count()

    silver_accounts, b4_quarantine = apply_plan_change(b4_raw, silver_accounts)
    assert_valid_quarantine_reason(b4_quarantine)
    
    quarantine_accounts = quarantine_accounts.unionByName(b4_quarantine, allowMissingColumns=True)
    log_event("billing_accounts", "batch_4", f"Processed {b4_count} plan changes: {b4_quarantine.count()} out of order quarantined")

    # -------------------------------------------------------------
    # BATCH 5: Entity Resolution - Reused Account IDs
    # -------------------------------------------------------------
    b5_path = config.BATCHES_DIR / "batch_5_reused_account_id" / "billing_accounts_batch5_reused_account_id.csv"
    b5_raw = ingest.read_billing_accounts(spark, path=b5_path)
    b5_count = b5_raw.count()

    b5_resolved = resolve_entities(b5_raw, silver_accounts)
    b5_casted, _ = cast_billing_accounts(b5_resolved)
    b5_casted = (
        b5_casted.withColumn("valid_from", F.col("signup_date"))
        .withColumn("valid_to", F.lit(None).cast(DateType()))
        .withColumn("is_current", F.lit(True))
    )

    assert_masked_field_unchanged(b5_raw, b5_casted, "email_masked", "email_masked")
    silver_accounts = silver_accounts.unionByName(b5_casted)

    log_event("billing_accounts", "batch_5", f"Processed {b5_count} reused account IDs, resolved to distinct entities")

    # -------------------------------------------------------------
    # Persist Final Silver & Quarantine Tables
    # -------------------------------------------------------------
    save_table(silver_accounts, config.SILVER_BILLING_ACCOUNTS)
    save_table(silver_plans, config.SILVER_PLAN_CATALOG)
    save_table(silver_invoices, config.SILVER_INVOICES)
    save_table(quarantine_invoices, config.QUARANTINE_INVOICES)
    save_table(quarantine_accounts, config.QUARANTINE_BILLING_ACCOUNTS)

    log_event("pipeline", "complete", "Successfully finished pipeline and wrote all silver & quarantine tables")
    return silver_accounts, silver_plans, silver_invoices, quarantine_accounts, quarantine_invoices


def main():
    spark = get_spark()
    try:
        run_pipeline(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
