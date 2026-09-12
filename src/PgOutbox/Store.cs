using Npgsql;

namespace PgOutbox;

public interface IOutboxStore
{
    Task<IReadOnlyList<OutboxMessage>> ClaimAsync(int batchSize, CancellationToken ct);
    Task MarkDispatchedAsync(Guid id, CancellationToken ct);
    Task MarkFailedAsync(Guid id, DateTimeOffset nextAttemptAt, CancellationToken ct);
    Task DeadLetterAsync(Guid id, CancellationToken ct);
}

public sealed class PgOutboxStore(NpgsqlDataSource dataSource) : IOutboxStore
{
    public async Task<IReadOnlyList<OutboxMessage>> ClaimAsync(int batchSize, CancellationToken ct)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        await using var cmd = new NpgsqlCommand("""
            SELECT id, aggregate, type, payload, headers, created_at, dispatched_at,
                   dead_lettered_at, attempts, next_attempt_at
            FROM outbox_messages
            WHERE dispatched_at IS NULL AND dead_lettered_at IS NULL
              AND next_attempt_at <= now()
            ORDER BY created_at, id
            LIMIT $1
            FOR UPDATE SKIP LOCKED
            """, conn, tx);
        cmd.Parameters.AddWithValue(batchSize);
        var rows = new List<OutboxMessage>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(new OutboxMessage(
                reader.GetGuid(0), reader.GetString(1), reader.GetString(2),
                reader.GetFieldValue<string>(3), reader.GetFieldValue<string>(4),
                reader.GetFieldValue<DateTimeOffset>(5),
                await reader.IsDBNullAsync(6, ct) ? null : reader.GetFieldValue<DateTimeOffset>(6),
                await reader.IsDBNullAsync(7, ct) ? null : reader.GetFieldValue<DateTimeOffset>(7),
                reader.GetInt32(8), reader.GetFieldValue<DateTimeOffset>(9)));
        }
        await reader.CloseAsync();
        await tx.CommitAsync(ct);
        return rows;
    }

    public Task MarkDispatchedAsync(Guid id, CancellationToken ct)
        => ExecAsync("UPDATE outbox_messages SET dispatched_at = now() WHERE id = $1",
            c => c.Parameters.AddWithValue(id), ct);

    public Task MarkFailedAsync(Guid id, DateTimeOffset nextAttemptAt, CancellationToken ct)
        => ExecAsync("UPDATE outbox_messages SET attempts = attempts + 1, next_attempt_at = $2 WHERE id = $1",
            c => { c.Parameters.AddWithValue(id); c.Parameters.AddWithValue(nextAttemptAt); }, ct);

    public Task DeadLetterAsync(Guid id, CancellationToken ct)
        => ExecAsync("UPDATE outbox_messages SET attempts = attempts + 1, dead_lettered_at = now() WHERE id = $1",
            c => c.Parameters.AddWithValue(id), ct);

    private async Task ExecAsync(string sql, Action<NpgsqlCommand> bind, CancellationToken ct)
    {
        await using var cmd = dataSource.CreateCommand(sql);
        bind(cmd);
        await cmd.ExecuteNonQueryAsync(ct);
    }
}

public sealed class Writer(NpgsqlConnection connection, NpgsqlTransaction? transaction = null)
{
    public async Task<Guid> WriteAsync(string aggregate, string type, string payloadJson,
        string? headersJson = null, CancellationToken ct = default)
    {
        var id = Guid.NewGuid();
        await using var cmd = new NpgsqlCommand(
            "INSERT INTO outbox_messages (id, aggregate, type, payload, headers) VALUES ($1, $2, $3, $4::jsonb, $5::jsonb)",
            connection, transaction);
        cmd.Parameters.AddWithValue(id);
        cmd.Parameters.AddWithValue(aggregate);
        cmd.Parameters.AddWithValue(type);
        cmd.Parameters.AddWithValue(payloadJson);
        cmd.Parameters.AddWithValue(headersJson ?? "{}");
        await cmd.ExecuteNonQueryAsync(ct);
        return id;
    }
}

public sealed class InboxDedupe(NpgsqlConnection connection, NpgsqlTransaction? transaction = null)
{
    public async Task<bool> CheckAndMarkAsync(string messageId, CancellationToken ct = default)
    {
        await using var cmd = new NpgsqlCommand(
            "INSERT INTO inbox_consumed (message_id) VALUES ($1) ON CONFLICT DO NOTHING", connection, transaction);
        cmd.Parameters.AddWithValue(messageId);
        return await cmd.ExecuteNonQueryAsync(ct) == 1;
    }
}
