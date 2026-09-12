using Npgsql;

namespace PgOutbox.Tests;

public sealed class DedupeTests
{
    [Fact]
    public async Task SecondMark_ReturnsFalse()
    {
        var inbox = new FakeInbox();
        Assert.True(await inbox.CheckAndMarkAsync("m-1"));
        Assert.False(await inbox.CheckAndMarkAsync("m-1"));
        Assert.True(await inbox.CheckAndMarkAsync("m-2"));
    }

    [Fact]
    public async Task Pg_CheckAndMark_Dedupes()
    {
        if (!Pg.Available) return;
        await using var ds = NpgsqlDataSource.Create(Pg.Conn!);
        await using var conn = await ds.OpenConnectionAsync();
        await using (var cmd = new NpgsqlCommand(
            await File.ReadAllTextAsync(SchemaPath()), conn))
            await cmd.ExecuteNonQueryAsync();
        await using (var wipe = new NpgsqlCommand("TRUNCATE inbox_consumed", conn))
            await wipe.ExecuteNonQueryAsync();
        var dedupe = new InboxDedupe(conn);
        Assert.True(await dedupe.CheckAndMarkAsync("m-1"));
        Assert.False(await dedupe.CheckAndMarkAsync("m-1"));
    }

    [Fact]
    public async Task Pg_Redelivery()
    {
        if (!Pg.Available) return;
        await using var ds = NpgsqlDataSource.Create(Pg.Conn!);
        await using var conn = await ds.OpenConnectionAsync();
        await using (var cmd = new NpgsqlCommand(
            await File.ReadAllTextAsync(SchemaPath()), conn))
            await cmd.ExecuteNonQueryAsync();
        await using (var wipe = new NpgsqlCommand("TRUNCATE outbox_messages, inbox_consumed", conn))
            await wipe.ExecuteNonQueryAsync();
        var writer = new Writer(conn);
        var id = await writer.WriteAsync("order", "created", """{"id":1}""");
        var store = new PgOutboxStore(ds);
        var seen = new List<Guid>();
        var crashed = false;
        var opts = new OutboxOptions { BaseDelay = TimeSpan.Zero };
        var worker = new RelayWorker(store, m =>
        {
            if (!crashed) { crashed = true; throw new IOException("crash"); }
            seen.Add(m.Id);
            return Task.CompletedTask;
        }, opts);
        await worker.RelayOnceAsync(CancellationToken.None);
        await worker.RelayOnceAsync(CancellationToken.None);
        Assert.Contains(id, seen);
    }

    private static string SchemaPath() => Path.GetFullPath(Path.Combine(
        AppContext.BaseDirectory, "..", "..", "..", "..", "..",
        "src", "PgOutbox", "Schema.sql"));
}
