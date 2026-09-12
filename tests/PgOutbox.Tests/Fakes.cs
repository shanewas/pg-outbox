namespace PgOutbox.Tests;

public sealed class FakeStore : IOutboxStore
{
    private readonly List<OutboxMessage> _rows = [];
    private readonly Lock _lock = new();
    public bool FailClaimOnce { get; set; }

    public void Seed(params OutboxMessage[] msgs)
    {
        lock (_lock) _rows.AddRange(msgs);
    }

    public static OutboxMessage New(string agg = "order", string type = "created",
        int attempts = 0, DateTimeOffset? next = null, DateTimeOffset? created = null) => new(
        Guid.NewGuid(), agg, type, """{"a":1}""", "{}", created ?? DateTimeOffset.UtcNow,
        null, null, attempts, next ?? DateTimeOffset.UtcNow.AddMinutes(-1));

    public Task<IReadOnlyList<OutboxMessage>> ClaimAsync(int batchSize, TimeSpan lease, CancellationToken ct)
    {
        lock (_lock)
        {
            if (FailClaimOnce) { FailClaimOnce = false; throw new InvalidOperationException("crash"); }
            var due = _rows
                .Where(m => m.DispatchedAt is null && m.DeadLetteredAt is null
                    && m.NextAttemptAt <= DateTimeOffset.UtcNow)
                .OrderBy(m => m.CreatedAt).ThenBy(m => m.Id).Take(batchSize).ToList();
            foreach (var m in due) Mutate(m.Id, x => x with { NextAttemptAt = DateTimeOffset.UtcNow + lease });
            return Task.FromResult<IReadOnlyList<OutboxMessage>>(due);
        }
    }

    public Task MarkDispatchedAsync(Guid id, CancellationToken ct)
    {
        lock (_lock) Mutate(id, m => m with { DispatchedAt = DateTimeOffset.UtcNow });
        return Task.CompletedTask;
    }

    public Task MarkFailedAsync(Guid id, DateTimeOffset nextAttemptAt, CancellationToken ct)
    {
        lock (_lock) Mutate(id, m => m with { Attempts = m.Attempts + 1, NextAttemptAt = nextAttemptAt });
        return Task.CompletedTask;
    }

    public Task DeadLetterAsync(Guid id, CancellationToken ct)
    {
        lock (_lock) Mutate(id, m => m with { Attempts = m.Attempts + 1, DeadLetteredAt = DateTimeOffset.UtcNow });
        return Task.CompletedTask;
    }

    public OutboxMessage Get(Guid id)
    {
        lock (_lock) return _rows.Single(m => m.Id == id);
    }

    private void Mutate(Guid id, Func<OutboxMessage, OutboxMessage> f)
    {
        var i = _rows.FindIndex(m => m.Id == id);
        _rows[i] = f(_rows[i]);
    }
}

public sealed class FakeInbox
{
    private readonly HashSet<string> _seen = [];
    public Task<bool> CheckAndMarkAsync(string id)
    {
        lock (_seen) return Task.FromResult(_seen.Add(id));
    }
}

internal static class Pg
{
    public static string? Conn => Environment.GetEnvironmentVariable("OUTBOX_PG");
    public static bool Available => Conn is not null;
}
