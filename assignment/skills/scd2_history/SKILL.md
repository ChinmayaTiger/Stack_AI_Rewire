---
name: silver-scd2-history
description: >
  Track a tracked attribute's changes over time (e.g. a subscription plan) so
  that historical state remains queryable, rather than being overwritten on
  change. Use whenever an entity's attribute can change during its lifetime and
  the pipeline must answer "what was true as of date X."
---

# Skill Name

SCD2 History Tracking (Slowly Changing Dimension, Type 2)

## Purpose

Guide the design of a silver-layer write so that a change to a tracked
attribute (e.g. a customer's subscription plan) is recorded as history, rather
than overwriting the prior value.

## Problem This Skill Solves

A plain upsert-in-place loses the previous value the moment an attribute
changes - there is no way to answer "what plan was this account on as of last
month" afterward. This skill defines how to keep both the old and new values
queryable by time.

## When to Use

- An entity has at least one attribute (e.g. `plan_id`) that can legitimately
  change during the entity's lifetime.
- The requirement is to answer point-in-time questions about that attribute's
  value, not just its current value.

This skill assumes at most one change event per entity per batch. It does not
cover multiple simultaneous attribute changes on the same row.

## Inputs / Context Required

- **The tracked attribute(s)**: which column(s) must have their history
  preserved (e.g. `plan_id`).
- **An effective date** for the change, supplied by the incoming event (e.g.
  `effective_date`).
- **The target silver table**, including its current `valid_from`, `valid_to`,
  and `is_current` columns for the entity being changed.

## Expected Outcome

For any entity that has undergone a tracked change, querying its attribute as
of a date before the change returns the old value, and as of a date on or
after the change returns the new value. At most one row per entity has
`is_current = true` at any time.

## Core Guidance

**Understand**: this is a close-and-append operation, not an update-in-place.
The prior "current" row is closed out (its `valid_to` is set, `is_current`
becomes false) and a new row is appended representing the new value.

**Reason**: before implementing, decide:
- Which row currently represents the entity's "current" state?
- What is the effective date of the incoming change, and does it fall on or after
  the current row's `valid_from`?
- What are `valid_from`/`valid_to` for the new row, and for the row being
  closed?

## Decision Rules

- Exactly one row per entity may have `is_current = true` at any time.
- **Normal Chronological Change**: When `effective_date >= current.valid_from`:
  - Set the previously-current row's `valid_to` to `effective_date`, and its `is_current` to `false`.
  - Insert a new row for the entity with the new attribute value, `valid_from` set to `effective_date`, `valid_to` set to `null`, and `is_current` set to `true`.
- **Out-of-Order Effective Date (Gap Closure)**: When `effective_date < current.valid_from`:
  - A change event arriving with an effective date preceding the entity's current active interval cannot be cleanly appended without corrupting temporal validity (`valid_to < valid_from`) or creating overlapping current intervals.
  - The incoming change row MUST NOT modify the existing silver record and MUST be routed to quarantine with controlled reason `out_of_order_effective_date`.
- Historical rows are never deleted or overwritten once closed.

## Implementation Guidance

1. Read the incoming change event (`account_id`, `new_plan_id`, `effective_date`).
2. Match against current silver records where `is_current = true`.
3. Compare `effective_date` against `valid_from`:
   - If `effective_date < valid_from`, route the change event to quarantine with `quarantine_reason = 'out_of_order_effective_date'`.
   - If `effective_date >= valid_from`, proceed to SCD2 update.
4. For valid change events, close out the current active row: set `valid_to = effective_date`, `is_current = false`.
5. Construct and append the new active row: carry over non-changing attributes (`customer_name`, `email_masked`, etc.), assign `plan_id = new_plan_id`, `valid_from = effective_date`, `valid_to = null`, and `is_current = true`.
6. Retain all untouched accounts and historical inactive rows without modification.

## Validation

- Ingest a baseline row, then a change event; assert exactly one `is_current = true` row remains for the entity afterward.
- Assert querying the entity's attribute as of a date before the change returns the old value, and as of a date on/after the change returns the new value.
- Assert the closed row's `valid_to` exactly equals the new row's `valid_from`, with no gap or overlap.
- Assert that any out-of-order event is quarantined with reason `out_of_order_effective_date` and leaves the existing silver row intact.

## Common Failure Scenarios

- The incoming effective date is earlier than `valid_from`, producing an inverted interval (`valid_to < valid_from`) or an invalid second active record.
- The current-row lookup matches more than one row, producing duplicate `is_current = true` rows.
- The new row is appended without closing the old one, leaving two open rows.
- Forgetting to carry forward unchanged columns onto the new row, leaving them null.
