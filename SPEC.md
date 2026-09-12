# pg-outbox — Specification

## 1. Problem & buyer
Dual-write bug: DB commit succeeds, event publish fails (or reverse) -> lost/
duplicate domain events. Buyer: .NET + Postgres shops without MassTransit/CAP.

## 2. Differentiation (verified 2026-09-12)
- Closest .NET rival `dotnetcore/CAP` (7111 stars) is a full framework with
  transports, not a lightweight lib. `cajuncoding/SqlTransactionalOutbox` is
  SQL-Server-only. `PandaTech.MassTransit.PostgresOutbox` requires MassTransit.
  TS side `zehelein/pg-transactional-outbox` (36 stars) is TS-only.
- No lightweight PG-first standalone .NET outbox exists. No dual-stack one.
- Beat angle: Zehelein-compatible table shape so mixed .NET+TS shops adopt
  incrementally; drop-in lib, zero framework adoption; LISTEN/NOTIFY with poll
  fallback; consumer-side dedup (inbox table).

## 3. Stack
.NET 9 (EF Core or raw Npgsql — Npgsql, fewer deps), Postgres. MIT.

## 4. v0.1 scope (<= ~1.5k LOC)
- `outbox_messages` table (id, aggregate, type, payload JSONB, headers,
  created_at, dispatched_at, attempts) + writer API (transaction-scoped).
- Relay worker (BackgroundService): SKIP LOCKED batch claim, at-least-once
  dispatch to `Func<OutboxMessage, Task>`, exponential backoff, dead-letter
  after N attempts.
- Consumer dedup: `inbox_consumed` table + `Dedupe.CheckAndMark`.
- Tests: xUnit + Testcontainers — crash-mid-dispatch redelivery test,
  ordering test, poison-message dead-letter test, dedup test.
- Non-goals: no Kafka/Rabbit drivers (handler func only), no sagas, no UI,
  no Debezium/CDC.

## 5. Architecture (file tree)
- src/PgOutbox/{Writer,RelayWorker,InboxDedupe,Options}.cs + Schema.sql
- tests/PgOutbox.Tests/{Redelivery,Ordering,DeadLetter,Dedupe}Tests.cs
- README.md, CHANGELOG.md, LICENSE (MIT)

## 6. Anti-patterns
- No at-most-once claims. No broker SDK deps in core.
- Document at-least-once + ordering limits plainly (partition key ordering only).
- No invented throughput numbers.

## 7. Release criteria
- `dotnet test` green incl. redelivery test. README 5-min quickstart
  (schema + writer + worker snippets). CHANGELOG. Tag v0.1.0. nupkg in dist/.
