namespace PgOutbox;

public sealed record OutboxMessage(
    Guid Id,
    string Aggregate,
    string Type,
    string Payload,
    string Headers,
    DateTimeOffset CreatedAt,
    DateTimeOffset? DispatchedAt,
    DateTimeOffset? DeadLetteredAt,
    int Attempts,
    DateTimeOffset NextAttemptAt);

public sealed class OutboxOptions
{
    public int BatchSize { get; set; } = 50;
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(1);
    public int MaxAttempts { get; set; } = 10;
    public TimeSpan BaseDelay { get; set; } = TimeSpan.FromSeconds(1);
    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromMinutes(5);
}
