---
name: silver-entity-resolution
description: >
  Disambiguate repeated business keys representing distinct real-world entities
  across source systems, assigning surrogate identities rather than merging
  or corrupting historical state.
---

# Skill Name

Entity Resolution (Reused Business Keys Across Systems)

## Purpose

Define the procedure for detecting when an incoming business key (such as
`account_id`) collides with an existing record in silver but represents a
completely distinct physical entity (e.g. account numbers recycled or
colliding between legacy and modern platforms).

## Problem This Skill Solves

Deduplication assumes repeated keys represent identical or updated records for
the same entity. SCD2 assumes repeated keys represent attribute changes for
the same entity over time. When source systems independently assign or reuse
the same business key for entirely different customers, applying standard dedup
or SCD2 merges or overwrites histories of unrelated physical entities.

## When to Use

- A business key arrives that already exists in silver.
- Core immutable identity attributes (such as `customer_name`, `signup_date`,
  or originating `source_system`) conflict with the existing record.
- The pipeline must preserve independent identities and historical integrity
  for both entities.

## Inputs / Context Required

- **Incoming Batch DataFrame**: containing the business key (`account_id`),
  continuity attributes (`customer_name`, `signup_date`, `source_system`),
  and tracked attributes (`plan_id`, `status`).
- **Existing Silver DataFrame**: containing current and historical entity
  records.
- **Privacy Constraint**: `email_masked` must NEVER be used to join, derive, or
  disambiguate identity across entities.

## Expected Outcome

1. The reused key is assigned a deterministic, distinct surrogate identity
   (e.g. `{account_id}-{source_system}`) for the new physical entity.
2. The original entity's silver rows and SCD2 history remain completely
   untouched.
3. The new entity begins its own independent SCD2 lifecycle in silver.
4. `email_masked` is passed through verbatim without cross-referencing.

## Decision Rules

- **Identity Continuity Check**:
  - Compare incoming records against existing silver records on `account_id`.
  - Check non-key identity attributes: if `customer_name` differs, or if
    `source_system` differs with inconsistent registration dates, the record
    represents a distinct physical entity.
  - **Masked Field Isolation**: Do NOT use `email_masked` as a match key,
    join condition, or disambiguation feature.
- **Surrogate Identity Generation**:
  - For conflicting entities, mint a new unique identifier:
    `{account_id}-{source_system}`.
  - The incoming record adopts this new surrogate identifier as its
    `account_id` before entering SCD2 or downstream writes.
- **Preservation Rule**:
  - Never close, update, or overwrite existing records of the original
    entity.

## Implementation Guidance

1. Perform a left join between incoming rows and existing silver rows on
   `account_id`.
2. Evaluate identity conflict:
   - Conflict condition: `existing.account_id IS NOT NULL` AND
     (`incoming.customer_name != existing.customer_name` OR
      `incoming.source_system != existing.source_system`).
3. For conflicting rows:
   - Assign `account_id = concat(account_id, lit('-'), source_system)`.
4. Leave non-conflicting rows with their original `account_id`.
5. Drop temporary lookup columns and pass the transformed DataFrame to
   downstream SCD2 ingestion.

## Validation

- Assert all reused `account_id` rows from the batch receive new distinct
  identifiers.
- Assert the original entity's row count, plan, and SCD2 dates in silver remain
  identical before and after the batch.
- Assert joining the old and new entity records on `email_masked` yields zero
  matches.

## Common Failure Scenarios

- Joining on `email_masked`, risking data leakage or false matches on masked
  placeholders (e.g. `***@***.com`).
- Overwriting or closing out the original entity via standard SCD2 logic.
- Generating non-deterministic surrogate keys that fail idempotency on rerun.
