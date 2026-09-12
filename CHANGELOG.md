# Changelog

## v0.1.0 — 2026-09-12
- Transaction-scoped `Writer` + `outbox_messages` schema (one table, partial
  index on pending rows).
- `RelayWorker` (BackgroundService): SKIP LOCKED batch claim, at-least-once
  dispatch func, exponential backoff, dead-letter after max attempts.
- `InboxDedupe` consumer-side exactly-once guard via `inbox_consumed`.
- 7 xUnit tests green: crash redelivery, ordering, poison dead-letter, dedup.
