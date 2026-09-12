namespace PgOutbox.Tests;

public sealed class RedeliveryTests
{
    [Fact]
    public async Task CrashMidDispatch_Redelivers()
    {
        var store = new FakeStore();
        var msg = FakeStore.New();
        store.Seed(msg);
        var dispatched = new List<Guid>();
        var crashed = false;
        var opts = new OutboxOptions { BaseDelay = TimeSpan.Zero };
        var worker = new RelayWorker(store, m =>
        {
            if (!crashed) { crashed = true; throw new IOException("crash"); }
            dispatched.Add(m.Id);
            return Task.CompletedTask;
        }, opts);

        store.FailClaimOnce = true;
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => store.ClaimAsync(10, CancellationToken.None));

        await worker.RelayOnceAsync(CancellationToken.None);
        Assert.Empty(dispatched);
        Assert.Null(store.Get(msg.Id).DispatchedAt);

        await worker.RelayOnceAsync(CancellationToken.None);
        Assert.Equal([msg.Id], dispatched);
        Assert.NotNull(store.Get(msg.Id).DispatchedAt);
    }
}
