---
name: silver-deduplication
description: >
  Guide a bronze-to-silver transformation step so that a redelivered batch does
  not create duplicate records, and so that a repeated business key is routed
  to the correct outcome: quarantine conflicting records, retain unchanged,
  or overwrite only where explicitly permitted. Use whenever a batch could
  plausibly be delivered or triggered more than once and a business key can be
  identified in the incoming data.
---

# Skill Name

Deduplication (Retried Batch Delivery & Legitimate Updates)

## Purpose

Guide the design of a silver-layer write so that a repeated business key is
never blindly inserted, blindly overwritten, or blindly dropped. For immutable
records (such as invoices), every repeated key must resolve to either retaining
the unchanged record or quarantining conflicting payloads.

## Problem This Skill Solves

Retries, timeouts, and manual re-runs occasionally cause the same batch to be
delivered more than once. But not every row sharing an existing business key
is a bad retry — some legitimately update the record's business state, while
others represent conflicting data under the same key. A plain append-only write
doubles records on retry; a plain "skip everything with an existing key" write
silently discards updates or conflicts; a plain "always overwrite" write lets
corrupted or conflicting data replace good data. This skill defines how to tell
these cases apart.

## When to Use

- A silver-layer write persists across runs.
- The same batch (same file, same date, same extract) could plausibly be
  delivered more than once.
- The requirement is "re-running this must not change the result, true retries
  must be skipped idempotently, and conflicting records must not overwrite
  existing silver data."

## Inputs / Context Required

- **The business key**: the column that uniquely identifies a real-world record
  for this entity (e.g. `invoice_id`). Must come from the data itself.
- **Identity-defining (immutable) columns**: columns that describe *what* the
  record is and should never change once written for a given business key
  (e.g. `account_id`, `invoice_date`, `currency` for an invoice).
- **Mutable / business-state columns**: columns that represent mutable state
  (e.g. `payment_status`, `amount_due` for an invoice).
- **The target silver table**: read before writing, so incoming rows can be
  checked against it.
- **The incoming batch**: the new data to be ingested.

## Expected Outcome

Every incoming row whose business key already exists in the target resolves
to exactly one of:

1. **Quarantine** — a row where an existing business key arrives with conflicting
   values (any identity-defining mismatch, or differing non-key values in an
   immutable/financial table like invoices) is routed to quarantine with reason
   `conflicting_key`.
2. **Overwrite (update, no history)** — only applies where in-place mutation is
   explicitly permitted. For immutable transaction tables (like invoices),
   differing values are conflicts and must be quarantined rather than overwritten.
3. **Retain unchanged** — a row that is byte-identical to the existing row on
   every non-key column is a true duplicate: no write occurs, and the existing
   row is left exactly as it is.

## Core Guidance

**Understand**: the problem is not binary ("new vs. duplicate"). A repeated
business key can mean multiple things, and code must distinguish them
by whether attributes match or conflict.

**Reason**: before writing code, decide for the entity at hand:
- What is the business key, and is it stable and unique?
- Which columns are identity-defining (must never change for that key)?
- Does the entity allow in-place overwrite, or must differing values be quarantined?
- Does the target table already exist, or is this the first load?

## Decision Rules

- If the target does not yet exist or is empty, treat the entire incoming
  batch as new.
- If a business key does not exist in the target, insert it.
- If a business key exists in the target and the incoming row is identical to
  the existing row on every non-key column, **retain unchanged**: skip the
  write, do not quarantine, do not duplicate.
- If a business key exists in the target and the incoming row differs on non-key
  columns: for entities where in-place mutation is prohibited (such as invoices),
  or where identity-defining columns differ, **quarantine** with reason
  `conflicting_key` — do not insert, do not overwrite, do not drop silently.
- Never use a surrogate value generated during ingestion (e.g. an
  auto-incrementing id) as part of the duplicate/identity check.

## Implementation Guidance

1. Load the incoming batch.
2. Join incoming rows to the target table on the business key.
3. For matched rows, compare non-key columns:
   - All columns match → retain unchanged (no-op).
   - Any column differs → route to quarantine with reason `conflicting_key`.
4. For unmatched keys, insert as new.
5. Report counts for each path: new inserted, retained unchanged, and quarantined.

```python
from functools import reduce
from pyspark.sql import functions as F

matched = incoming_df.alias("in").join(existing_df.alias("ex"), on=business_key, how="inner")

non_key_cols = [c for c in incoming_df.columns if c != business_key]
has_conflict = reduce(
    lambda acc, c: acc | (F.col(f"in.{c}") != F.col(f"ex.{c}")),
    non_key_cols,
    F.lit(False)
)

conflicts = (
    matched.filter(has_conflict)
    .select("in.*")
    .withColumn("quarantine_reason", F.lit("conflicting_key"))
)
unchanged = matched.filter(~has_conflict)
new_rows = incoming_df.join(existing_df, on=business_key, how="left_anti")
```

## Validation

- Process a batch once; record the resulting row count and the counts for
  each outcome.
- Process the exact same batch again; assert the row count is unchanged and
  every row resolves to "retain unchanged."
- Process a batch containing conflicting rows with an existing key; assert
  they are quarantined with reason `conflicting_key`, the target row count is
  unaffected, and existing silver records are untouched.

## Common Failure Scenarios

- The redelivered batch is not byte-identical (different row order, re-quoted
  values) — key on business identity and column-level comparison, not
  file/content equality.
- The target table does not exist yet on the first run.
- An empty batch should succeed and add zero rows.

## Common Mistakes

- Silently overwriting immutable/financial records (like invoices) when amounts
  or statuses differ on redelivery, instead of quarantining the conflict.
- Quarantining true duplicate retries instead of idempotently retaining unchanged.
- Using `mode("append")` with no check against existing data.
- Comparing rows including ingestion-metadata columns, causing every
  redelivery to look "new" or "changed" because a timestamp differs.

## Reusability

Applies to any batch write, for any entity, as long as a business key can be
identified and the entity's columns can be split into identity-defining vs.
mutable/business-state. Independent of storage format. Distinct from the SCD2
skill: use this skill when handling retried batches and isolated conflicts;
use SCD2 when point-in-time history of a legitimate change must remain queryable.