---
name: silver-pipeline-guardrails
description: >
  Enforce cross-cutting data integrity invariants (row-count conservation,
  controlled quarantine reasons, masked field passthrough, idempotency, and
  non-destructive writes) across all silver pipeline processing stages.
---

# Skill Name

Pipeline Guardrails (Cross-Cutting Silver Quality Invariants)

## Purpose

Define and enforce end-to-end data safety invariants across all ingestion,
transformation, deduplication, entity resolution, and SCD2 stages in the
silver layer.

## Problem This Skill Solves

Individual transformation steps focus on their own local logic, but errors
often manifest at pipeline seams - rows silently dropped without entering
silver or quarantine, arbitrary quarantine strings preventing automated
triage, accidental leakage or corruption of PII/masked columns, non-idempotent
reruns generating duplicate rows, or destructive overwrites that wipe prior
batch history.

## When to Use

- Across every batch execution (Batch 1 through Batch 5).
- After every transformation boundary where rows split between clean silver
  tables and quarantine sinks.
- In automated test suites validating pipeline resilience.

## Inputs / Context Required

- **Input Bronze Batch DataFrame**: raw rows arriving for processing.
- **Silver Target DataFrame**: rows destined for or existing in silver.
- **Quarantine Sink DataFrame**: rows rejected for quality or ordering reasons.
- **Controlled Reason Dictionary**: `config.QUARANTINE_REASONS`.
- **Masked Columns**: `email_masked`.

## Expected Outcome

1. Every ingested row is accounted for mathematically (zero row loss).
2. All quarantine rows have standardized reason codes for automated triage.
3. Masked PII remains byte-for-byte identical to the raw source.
4. Re-executing a batch results in identical silver and quarantine tables.
5. Later batches do not erase, truncate, or overwrite previous batch data.

## Decision Rules

- **Row-Count Conservation**:
  - For every batch: `input_count == silver_written + quarantine_written + true_duplicates_skipped`.
  - Any disparity raises an immediate critical alert or exception.
- **Controlled Quarantine Reasons**:
  - Quarantined rows must have a valid `quarantine_reason` belonging to `config.QUARANTINE_REASONS.values()`.
  - Null, empty string, or free-text descriptions are rejected.
- **Masked-Field Passthrough Integrity**:
  - `email_masked` must never be altered, unmasked, stripped, or joined on.
  - Comparing input vs output on primary/surrogate key must show zero differences.
- **Idempotency**:
  - Running a batch sequence multiple times against the silver layer must yield identical row counts and table hashes.
- **Non-Destructive Writes**:
  - Output writes must use append or explicitly partitioned/versioned patterns.
  - A batch write must never perform a blind table truncate or unrestricted overwrite.

## Implementation Guidance

1. Implement assertion helpers:
   - `assert_row_count_conservation(input_count, silver_count, quarantine_count, true_dup_count)`
   - `assert_valid_quarantine_reason(quarantine_df, reason_col)`
   - `assert_masked_field_unchanged(before_df, after_df, key_col, masked_col)`
2. Invoke guardrail assertions at the boundary of each batch stage before persisting.
3. Validate idempotency in test fixtures by executing double runs and asserting equality.

## Validation

- Assert conservation formula holds across Batch 2, 3, 4, and 5.
- Assert all rows in quarantine tables match `config.QUARANTINE_REASONS`.
- Assert join of input and silver tables on key yields exact equality for `email_masked`.
- Verify pipeline rerun results in no duplicate keys or inflated counts.

## Common Failure Scenarios

- Silent filtering via inner joins dropping rows without routing them to quarantine.
- Hardcoding unvetted strings into quarantine reason columns.
- Re-formatting masked fields (e.g. trimming or lowercasing placeholders).
- Non-deterministic surrogate generation breaking idempotency.
