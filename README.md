# pg-outbox

Transactional outbox for .NET + Postgres. No framework, no broker SDK.

Write your business row and your event row in one transaction. A background
relay delivers the events afterwards. Your DB commit and your event publish
stop disagreeing.

```csharp
await using var tx = await conn.BeginTransactionAsync();
await new OrderRepository(conn, tx).InsertAsync(order);
await new Writer(conn, tx).WriteAsync("order", "order.placed", """{"id":7}""");
await tx.CommitAsync();

// elsewhere, at startup:
services.AddHostedService(sp => new RelayWorker(store, async msg =>
{
    await bus.PublishAsync(msg.Type, msg.Payload);
}));
```

Run `Schema.sql` once. Two tables: `outbox_messages`, `inbox_consumed`.

## How delivery works

The relay polls every second (`PollInterval`), claims a batch with
`FOR UPDATE SKIP LOCKED`, and calls your `dispatch` func per message.
Success marks it dispatched. Failure backs off exponentially (`BaseDelay`,
doubling, capped at `MaxDelay`) and dead-letters after `MaxAttempts`.
Crash mid-dispatch means the row stays pending, so the next poll redelivers.
Covered by `CrashRedelivers`, `PoisonDeadLetters`, `OrderedWithinBatch`.

Consumers dedupe with the inbox table in their own transaction:

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
- One relay instance per database in v0.1. Two relays would double-dispatch
  (consumers stay correct via dedup, but you pay for duplicate sends).
- Poll-based relay. No `LISTEN/NOTIFY` fast path yet; 1s default poll.
- Ordering holds within a relay batch (`created_at, id`), not across retries.
- No Kafka/Rabbit drivers ship with it. Your `dispatch` func owns the broker,
  which keeps this package dependency-free apart from Npgsql.

## Why not CAP or MassTransit

`dotnetcore/CAP` (7k stars) does outbox plus transports, routing, and dashboards.
If you run that stack already, stay there. This package is for shops that want
one table, one worker, and no framework adoption. Same idea as
`zehelein/pg-transactional-outbox` on npm, built for .NET.

## Tests

`dotnet test` runs 7 tests against in-memory fakes. Set `OUTBOX_PG` to a
Postgres connection string and the live round-trip tests join in.

## License

MIT.
