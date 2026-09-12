namespace PgOutbox.Tests;

public sealed class OrderingTests
{
    [Fact]
    public async Task Claim_ReturnsCreatedAtThenIdOrder()
    {
        var store = new FakeStore();
        var base_ = DateTimeOffset.UtcNow.AddMinutes(-5);
        OutboxMessage mk(int s) => FakeStore.New(next: base_, created: base_.AddSeconds(s));
        var m2 = mk(2); var m1 = mk(1); var m3 = mk(3);
        store.Seed(m2, m1, m3);
        var got = await store.ClaimAsync(10, TimeSpan.FromSeconds(30), CancellationToken.None);
        Assert.Equal([m1.Id, m2.Id, m3.Id], got.Select(m => m.Id));
    }
}
