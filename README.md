# pg-outbox

For .NET + Postgres shops that don't run CAP or MassTransit: transactional
outbox as a drop-in library. No framework, no broker SDK.

Write your business row and your event row in one transaction. A background
relay delivers the events afterwards. Your DB commit and your event publish
stop disagreeing.

```csharp
using Npgsql;
using PgOutbox;

await using var dataSource = NpgsqlDataSource.Create(connString);

await using var conn = await dataSource.OpenConnectionAsync();
await using var tx = await conn.BeginTransactionAsync();
await new OrderRepository(conn, tx).InsertAsync(order); // your own code
await new Writer(conn, tx).WriteAsync("order", "order.placed", """{"id":7}""");
await tx.CommitAsync();
```

Install: `dotnet add package PgOutbox` (plus `Npgsql` directly). Run the
schema once before first use:

```sh
psql "$CONN" -f src/PgOutbox/Schema.sql
```

Relay at startup (hosted service, `bus` is your own broker client):

```csharp
var store = new PgOutboxStore(dataSource);
services.AddHostedService(sp => new RelayWorker(store, async msg =>
{
    await bus.PublishAsync(msg.Type, msg.Payload);
}));
```

Options and defaults: `BatchSize` 50, `PollInterval` 1s, `MaxAttempts` 10,
`BaseDelay` 1s doubling per attempt, capped at `MaxDelay` 5min, `ClaimLease`
30s (hides claimed rows from other relays while one works them).

## How delivery works

The relay polls, claims a batch with `FOR UPDATE SKIP LOCKED` (Postgres lets
concurrent workers skip each other's locked rows instead of blocking), and
calls your `dispatch` func per message. Success marks it dispatched. Failure
backs off exponentially and dead-letters after `MaxAttempts`. A claimed batch
carries a 30s lease, so a second relay (or a restarted one) won't grab the
same rows mid-dispatch. Crash before dispatch completes means the row stays
pending, so the next poll redelivers. Covered by `CrashRedelivers`,
`PoisonDeadLetters`, `OrderedWithinBatch`.

Consumers dedupe with the inbox table inside their own transaction:

```csharp
await using var tx = await conn.BeginTransactionAsync();
if (!await new InboxDedupe(conn, tx).CheckAndMarkAsync(msg.Id.ToString()))
    return; // already handled
// ... handle ...
await tx.CommitAsync();
```

## Limits, stated plainly

- At-least-once, not exactly-once. Handlers must tolerate redelivery; the
  inbox table is how you do that.
- Poll-based relay (default 1s). No `LISTEN/NOTIFY` fast path in v0.1, and no
  exact wire-compat promise with other outbox tables — same idea as
  `zehelein/pg-transactional-outbox` on npm, built for .NET.
- Ordering holds within a relay batch (`created_at, id`), not across retries.
- No Kafka/Rabbit drivers ship with it. Your `dispatch` func owns the broker,
  which keeps this package dependency-free apart from Npgsql.

## Why not CAP or MassTransit

`dotnetcore/CAP` (7k stars) does outbox plus transports, routing, and
dashboards. If you run that stack already, stay there. This package is one
table, one worker, no framework adoption.

## Tests

`dotnet test` runs 7 tests against in-memory fakes. Set `OUTBOX_PG` to a
Postgres connection string and the live round-trip tests join in.

Part of a trilogy: webhook-guard (verify at ingress) → idempotency-keys
(dedupe) → pg-outbox (publish at egress).

## License

MIT.
