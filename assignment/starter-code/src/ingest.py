"""Read bronze CSVs into DataFrames with explicit schemas."""

from pathlib import Path
from typing import Optional, Union
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from . import config

# Explicit schema definitions for Bronze CSV ingestion
BILLING_ACCOUNTS_SCHEMA = StructType([
    StructField("account_id", StringType(), False),
    StructField("customer_name", StringType(), True),
    StructField("email_masked", StringType(), True),
    StructField("signup_date", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("plan_id", StringType(), True),
    StructField("status", StringType(), True),
])

PLAN_CATALOG_SCHEMA = StructType([
    StructField("plan_id", StringType(), False),
    StructField("plan_name", StringType(), True),
    StructField("monthly_price", DoubleType(), True),
    StructField("tier", StringType(), True),
])

INVOICES_SCHEMA = StructType([
    StructField("invoice_id", StringType(), False),
    StructField("account_id", StringType(), False),
    StructField("invoice_date", StringType(), True),
    StructField("amount_due", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("payment_status", StringType(), True),
])


def read_billing_accounts(
    spark: SparkSession,
    path: Optional[Union[str, Path]] = None,
) -> DataFrame:
    """Read baseline billing_accounts.csv or a specific batch file."""
    file_path = str(path) if path is not None else str(config.DATA_DIR / "billing_accounts.csv")
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(BILLING_ACCOUNTS_SCHEMA)
        .load(file_path)
    )


def read_plan_catalog(
    spark: SparkSession,
    path: Optional[Union[str, Path]] = None,
) -> DataFrame:
    """Read plan_catalog.csv."""
    file_path = str(path) if path is not None else str(config.DATA_DIR / "plan_catalog.csv")
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(PLAN_CATALOG_SCHEMA)
        .load(file_path)
    )


def read_invoices(
    spark: SparkSession,
    path: Optional[Union[str, Path]] = None,
) -> DataFrame:
    """Read baseline invoices.csv or a specific batch file."""
    file_path = str(path) if path is not None else str(config.DATA_DIR / "invoices.csv")
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(INVOICES_SCHEMA)
        .load(file_path)
    )