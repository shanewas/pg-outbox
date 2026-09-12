# Changelog

## v0.1.2 — 2026-09-12
- LICENSE + README embedded in the package. No code changes.

## v0.1.1 — 2026-09-12
- Claims carry a 30s lease (`ClaimLease` option): claimed rows hide from other
  relays until the lease lapses, so scale-out and restarts stop double-claiming.
- `inbox_consumed` upsert names its conflict target explicitly.
- README rewritten around code that compiles: install names, schema command,
  options table, user-code markers.

## v0.1.0 — 2026-09-12
- Transaction-scoped `Writer` + `outbox_messages` schema (one table, partial
  index on pending rows).
- `RelayWorker` (BackgroundService): SKIP LOCKED batch claim, at-least-once
  dispatch func, exponential backoff, dead-letter after max attempts.
- `InboxDedupe` consumer-side exactly-once guard via `inbox_consumed`.
- 7 xUnit tests green: crash redelivery, ordering, poison dead-letter, dedup.
