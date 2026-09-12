namespace PgOutbox.Tests;

public sealed class DeadLetterTests
{
    [Fact]
    public async Task PoisonMessage_DeadLettersAfterMaxAttempts()
    {
        var store = new FakeStore();
        var seeded = FakeStore.New(attempts: 2);
        store.Seed(seeded);
        var opts = new OutboxOptions { MaxAttempts = 3, BaseDelay = TimeSpan.Zero };
        var worker = new RelayWorker(store, _ => throw new InvalidOperationException("poison"), opts);
        await worker.RelayOnceAsync(CancellationToken.None);
        var row = store.Get(seeded.Id);
        Assert.NotNull(row.DeadLetteredAt);
        Assert.Equal(3, row.Attempts);
    }

    [Fact]
    public void Backoff_IsExponentialCapped()
    {
        Assert.Equal(TimeSpan.FromSeconds(2),
            RelayWorker.Backoff(1, TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(5)));
        Assert.Equal(TimeSpan.FromSeconds(4),
            RelayWorker.Backoff(2, TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(5)));
        Assert.Equal(TimeSpan.FromSeconds(30),
            RelayWorker.Backoff(10, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30)));
    }
}
