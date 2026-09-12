using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace PgOutbox;

public sealed class RelayWorker(
    IOutboxStore store,
    Func<OutboxMessage, Task> dispatch,
    OutboxOptions? options = null,
    ILogger<RelayWorker>? log = null) : BackgroundService
{
    private readonly OutboxOptions _options = options ?? new();

    // Single relay instance per database in v0.1. Claim locks release at
    // SELECT commit, so two relays would double-dispatch (consumers stay safe
    // via InboxDedupe, but you pay duplicate sends). One relay is the config.

    public static TimeSpan Backoff(int attempts, TimeSpan @base, TimeSpan max)
    {
        var ms = @base.TotalMilliseconds * Math.Pow(2, attempts);
        return TimeSpan.FromMilliseconds(Math.Min(ms, max.TotalMilliseconds));
    }

    public async Task RelayOnceAsync(CancellationToken ct)
    {
        var batch = await store.ClaimAsync(_options.BatchSize, ct);
        foreach (var msg in batch)
        {
            try
            {
                await dispatch(msg);
                await store.MarkDispatchedAsync(msg.Id, ct);
            }
            catch (Exception ex)
            {
                log?.LogWarning(ex, "Dispatch failed for {Id} (attempt {Attempts})", msg.Id, msg.Attempts);
                var attempts = msg.Attempts + 1;
                if (attempts >= _options.MaxAttempts)
                    await store.DeadLetterAsync(msg.Id, ct);
                else
                    await store.MarkFailedAsync(msg.Id,
                        DateTimeOffset.UtcNow + Backoff(msg.Attempts, _options.BaseDelay, _options.MaxDelay), ct);
            }
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            await RelayOnceAsync(stoppingToken);
            await Task.Delay(_options.PollInterval, stoppingToken);
        }
    }
}
