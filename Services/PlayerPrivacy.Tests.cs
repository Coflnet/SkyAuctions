using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using Cassandra;
using Coflnet.Sky.Auctions.Models;
using Coflnet.Sky.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using NUnit.Framework;

namespace Coflnet.Sky.Auctions.Services;

public class PlayerPrivacyTests
{
    private static readonly Guid OptedOut = Guid.Parse(PlayerOptOut.LegacyPlayerUuids[0]);
    private static readonly Guid Other = Guid.Parse("11111111111111111111111111111111");
    private static readonly Guid Other2 = Guid.Parse("22222222222222222222222222222222");

    [SetUp]
    public void SetUp() => PlayerOptOut.ResetMemory();

    [TearDown]
    public void TearDown() => PlayerOptOut.ResetMemory();

    private static SaveAuction NewAuction(string seller, params string[] bidders)
    {
        var uuid = Guid.NewGuid();
        return new SaveAuction
        {
            Uuid = uuid.ToString("N"),
            UId = Random.Shared.NextInt64(1, long.MaxValue),
            Tag = "TEST_ITEM",
            AuctioneerId = seller,
            ProfileId = seller,
            End = new DateTime(2024, 3, 5, 12, 0, 0, DateTimeKind.Utc),
            Start = new DateTime(2024, 3, 4, 12, 0, 0, DateTimeKind.Utc),
            HighestBidAmount = bidders.Length * 100,
            Enchantments = new(),
            Bids = bidders.Select((b, i) => new SaveBids { Bidder = b, ProfileId = b, Amount = (i + 1) * 100, Timestamp = DateTime.UtcNow, AuctionId = uuid.ToString("N") }).ToList(),
            CoopMembers = bidders.Select(b => new UuId(b)).ToList()
        };
    }

    private static bool Mentions(SaveAuction a, Guid player)
    {
        var n = player.ToString("N");
        return a.AuctioneerId == n || a.ProfileId == n || a.Bids.Any(b => b.Bidder == n || b.ProfileId == n) || a.CoopMembers.Any(m => m.value == n);
    }

    // ---------- ingestion ----------

    [Test]
    public async Task InsertSellsMasksOptedOutSellerAndBidder()
    {
        var session = new Mock<ISession>();
        session.Setup(s => s.ExecuteAsync(It.IsAny<IStatement>())).ReturnsAsync(new RowSet());
        var scylla = new ScyllaService(session.Object, NullLogger<ScyllaService>.Instance, null);
        var config = new ConfigurationBuilder().Build();
        var collector = new SellsCollector(Mock.Of<IServiceScopeFactory>(), config, NullLogger<SellsCollector>.Instance, scylla);

        var withOptedOutSeller = NewAuction(OptedOut.ToString("N"), Other.ToString("N"));
        var withOptedOutBidder = NewAuction(Other2.ToString("N"), Other.ToString("N"), OptedOut.ToString("N"));
        var clean = NewAuction(Other.ToString("N"), Other2.ToString("N"));
        try
        {
            await collector.InsertSells(new[] { withOptedOutSeller, withOptedOutBidder, clean });
        }
        catch (Exception)
        {
            // the cassandra LINQ Table needs a live cluster; masking happens before any insert is attempted,
            // so the state of the auctions is what would have reached scylla and the S3 mirror
        }

        foreach (var auction in new[] { withOptedOutSeller, withOptedOutBidder, clean })
        {
            Assert.That(Mentions(auction, OptedOut), Is.False);
            var row = ScyllaService.ToCassandra(auction);
            Assert.That(row.Auctioneer, Is.Not.EqualTo(OptedOut));
            Assert.That(row.HighestBidder, Is.Not.EqualTo(OptedOut));
            Assert.That(row.ProfileId, Is.Not.EqualTo(OptedOut));
            Assert.That(row.Bids.Any(b => b.BidderUuid == OptedOut || b.ProfileId == OptedOut), Is.False);
            Assert.That(row.Coop?.Contains(OptedOut.ToString("N")) ?? false, Is.False);
        }
        Assert.That(PlayerOptOut.IsAnonymousUuid(withOptedOutSeller.AuctioneerId), Is.True);
        Assert.That(withOptedOutBidder.AuctioneerId, Is.EqualTo(Other2.ToString("N")));
        Assert.That(withOptedOutBidder.Bids.Select(b => b.Bidder).Count(b => b == Other.ToString("N")), Is.EqualTo(1));
        Assert.That(clean.AuctioneerId, Is.EqualTo(Other.ToString("N")));
        Assert.That(clean.Bids.Single().Bidder, Is.EqualTo(Other2.ToString("N")));
    }

    [Test]
    public void PlayerIndexIgnoresOptedOutAndAnonymousPlayers()
    {
        var puts = new List<string>();
        var s3 = FakeS3.Create(puts, new Dictionary<string, byte[]>());
        var index = new S3PlayerIndexService(s3, NullLogger<S3PlayerIndexService>.Instance);
        var entry = new PlayerParticipationEntry { AuctionUid = Guid.NewGuid(), End = DateTime.UtcNow, Tag = "T" };
        index.AddParticipations(OptedOut, new[] { entry });
        index.AddParticipations(PlayerPrivacyService.NewAnonymousGuid(), new[] { entry });
        index.AddParticipations(Other, new[] { entry });
        index.FlushAll().GetAwaiter().GetResult();
        Assert.That(puts, Has.Count.EqualTo(1));
        Assert.That(puts[0], Does.Contain(Other.ToString("N")));
    }

    [Test]
    public async Task GetParticipationOfOptedOutPlayerIsEmpty()
    {
        var puts = new List<string>();
        var s3 = FakeS3.Create(puts, new Dictionary<string, byte[]>());
        var index = new S3PlayerIndexService(s3, NullLogger<S3PlayerIndexService>.Instance);
        Assert.That(await index.GetParticipation(OptedOut, 2024), Is.Empty);
    }

    // ---------- row anonymization ----------

    private static ScyllaAuction Row(Guid seller, Guid highest, params CassandraBid[] bids) => new()
    {
        Tag = "T",
        TimeKey = 5,
        IsSold = true,
        End = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc),
        AuctionUid = 42,
        Uuid = Guid.NewGuid(),
        Auctioneer = seller,
        HighestBidder = highest,
        ProfileId = seller,
        ProfileName = "name",
        Coop = new List<string> { seller.ToString("N"), Other.ToString("N") },
        HighestBidAmount = 500,
        StartingBid = 100,
        ItemName = "sword",
        SerialisedBids = MessagePack.MessagePackSerializer.Serialize<IEnumerable<CassandraBid>>(bids.ToList())
    };

    private static CassandraBid Bid(Guid bidder, long amount) =>
        new() { AuctionUuid = Guid.NewGuid(), BidderUuid = bidder, Amount = amount, Timestamp = DateTime.UtcNow, BidderName = "n" + amount, ProfileId = bidder };

    [Test]
    public void AnonymizeRowReplacesSellerHighestBidderBidsAndCoop()
    {
        var anon = PlayerPrivacyService.NewAnonymousGuid();
        var row = Row(OptedOut, OptedOut, Bid(Other, 200), Bid(OptedOut, 500));

        Assert.That(PlayerPrivacyService.AnonymizeRow(row, OptedOut, anon), Is.True);

        Assert.That(row.Auctioneer, Is.EqualTo(anon));
        Assert.That(row.HighestBidder, Is.EqualTo(anon));
        Assert.That(row.ProfileId, Is.EqualTo(Guid.Empty));
        Assert.That(row.ProfileName, Is.Null);
        Assert.That(row.Coop, Is.EqualTo(new[] { Other.ToString("N") }));
        var bids = MessagePack.MessagePackSerializer.Deserialize<List<CassandraBid>>(row.SerialisedBids);
        Assert.That(bids.Select(b => b.BidderUuid), Is.EqualTo(new[] { Other, anon }));
        Assert.That(bids[0].BidderName, Is.EqualTo("n200"));
        Assert.That(bids[0].ProfileId, Is.EqualTo(Other));
        Assert.That(bids[1].BidderName, Is.Null);
        Assert.That(bids[1].ProfileId, Is.EqualTo(Guid.Empty));
        Assert.That(bids.Select(b => b.Amount), Is.EqualTo(new long[] { 200, 500 }));
        Assert.That(row.HighestBidAmount, Is.EqualTo(500));
        Assert.That(row.ItemName, Is.EqualTo("sword"));
    }

    [Test]
    public void AnonymizeRowKeepsOtherPlayersAndReportsNoChange()
    {
        var anon = PlayerPrivacyService.NewAnonymousGuid();
        var row = Row(Other, Other2, Bid(Other2, 300));
        row.Coop = new List<string>();
        var before = row.SerialisedBids.ToArray();

        Assert.That(PlayerPrivacyService.AnonymizeRow(row, OptedOut, anon), Is.False);
        Assert.That(row.Auctioneer, Is.EqualTo(Other));
        Assert.That(row.HighestBidder, Is.EqualTo(Other2));
        Assert.That(row.SerialisedBids, Is.EqualTo(before));
    }

    [Test]
    public void AnonymizeRowOnlyBidderLeavesSeller()
    {
        var anon = PlayerPrivacyService.NewAnonymousGuid();
        var row = Row(Other, Other2, Bid(OptedOut, 100), Bid(Other2, 300));
        row.Coop = null;
        Assert.That(PlayerPrivacyService.AnonymizeRow(row, OptedOut, anon), Is.True);
        Assert.That(row.Auctioneer, Is.EqualTo(Other));
        Assert.That(row.ProfileId, Is.EqualTo(Other));
        Assert.That(row.HighestBidder, Is.EqualTo(Other2));
        var bids = MessagePack.MessagePackSerializer.Deserialize<List<CassandraBid>>(row.SerialisedBids);
        Assert.That(bids.Select(b => b.BidderUuid), Is.EqualTo(new[] { anon, Other2 }));
    }

    [Test]
    public void AnonymizeRowClearsProfileIdOfPlayerOnOtherPeoplesRowsAndBids()
    {
        var anon = PlayerPrivacyService.NewAnonymousGuid();
        // coop member's profile: seller is somebody else, profile id is the erased player's
        var row = Row(Other, Other2, new CassandraBid { AuctionUuid = Guid.NewGuid(), BidderUuid = Other2, Amount = 5, Timestamp = DateTime.UtcNow, BidderName = "x", ProfileId = OptedOut });
        row.ProfileId = OptedOut;
        row.Coop = null;

        Assert.That(PlayerPrivacyService.AnonymizeRow(row, OptedOut, anon), Is.True);

        Assert.That(row.ProfileId, Is.EqualTo(Guid.Empty));
        Assert.That(row.ProfileName, Is.Null);
        Assert.That(row.Auctioneer, Is.EqualTo(Other));
        var bid = MessagePack.MessagePackSerializer.Deserialize<List<CassandraBid>>(row.SerialisedBids).Single();
        Assert.That(bid.BidderUuid, Is.EqualTo(Other2));
        Assert.That(bid.BidderName, Is.EqualTo("x"));
        Assert.That(bid.ProfileId, Is.EqualTo(Guid.Empty));
    }

    [Test]
    public void AnonymizeRowForOptedOutHandlesMultiplePlayersAndCoop()
    {
        var second = Guid.Parse(PlayerOptOut.LegacyPlayerUuids[1]);
        var row = Row(Other, Other2, Bid(Other, 100), Bid(second, 200), Bid(Other2, 300));
        row.Coop = new List<string> { Other.ToString("N"), OptedOut.ToString("N"), second.ToString("D") };
        row.ProfileId = Other;

        Assert.That(PlayerPrivacyService.AnonymizeRowForOptedOut(row), Is.True);

        Assert.That(row.Coop, Is.EqualTo(new[] { Other.ToString("N") }));
        Assert.That(row.Auctioneer, Is.EqualTo(Other));
        Assert.That(row.ProfileId, Is.EqualTo(Other));
        Assert.That(row.HighestBidder, Is.EqualTo(Other2));
        var bids = MessagePack.MessagePackSerializer.Deserialize<List<CassandraBid>>(row.SerialisedBids);
        Assert.That(bids.Select(b => b.BidderUuid).ToList(), Has.Member(Other).And.Member(Other2).And.Not.Member(second));
        Assert.That(PlayerOptOut.IsAnonymousUuid(bids[1].BidderUuid.ToString("N")), Is.True);
        Assert.That(bids.Select(b => b.Amount), Is.EqualTo(new long[] { 100, 200, 300 }));
    }

    [Test]
    public void AnonymizeRowForOptedOutCleanRowReportsNoChange()
    {
        var row = Row(Other, Other2, Bid(Other2, 300));
        row.Coop = new List<string> { Other.ToString("N") };
        var before = row.SerialisedBids.ToArray();
        Assert.That(PlayerPrivacyService.AnonymizeRowForOptedOut(row), Is.False);
        Assert.That(row.SerialisedBids, Is.EqualTo(before));
        Assert.That(row.Coop, Has.Count.EqualTo(1));
    }

    // ---------- read time erasure ----------

    private static (ScyllaService scylla, List<ScyllaAuction> writes) ReadHarness(Func<ScyllaAuction, Task> writeBack = null)
    {
        var writes = new List<ScyllaAuction>();
        var scylla = new ScyllaService(Mock.Of<ISession>(), NullLogger<ScyllaService>.Instance, null)
        {
            IdentityWriteBack = writeBack ?? (r => { writes.Add(r); return Task.CompletedTask; })
        };
        return (scylla, writes);
    }

    [Test]
    public void ReadRowWithOptedOutCoopMemberReturnsCleanAuctionAndWritesBackOnce()
    {
        var (scylla, writes) = ReadHarness();
        var row = Row(Other, Other2, Bid(Other2, 300));
        row.Coop = new List<string> { Other.ToString("N"), OptedOut.ToString("N") };

        var result = scylla.ReadRow(row);

        Assert.That(result.Coop, Is.EqualTo(new[] { Other.ToString("N") }));
        Assert.That(OptOutScrubber.ContainsOptedOut(result), Is.False);
        Assert.That(result.AuctioneerId, Is.EqualTo(Other.ToString("N")));
        Assert.That(writes, Has.Count.EqualTo(1));
        Assert.That(writes[0].Coop, Is.EqualTo(new[] { Other.ToString("N") }));
        // reading the (now clean) row again does not write again
        scylla.ReadRow(row);
        Assert.That(writes, Has.Count.EqualTo(1));
    }

    [Test]
    public void ReadRowWithOptedOutBidderOnlyInBidsIsAnonymizedAndWritten()
    {
        var (scylla, writes) = ReadHarness();
        var row = Row(Other, Other2, Bid(OptedOut, 100), Bid(Other2, 300));
        row.Coop = null;

        var result = scylla.ReadRow(row);

        Assert.That(result.Bids.Any(b => b.Bidder == OptedOut.ToString("N") || b.ProfileId == OptedOut.ToString("N")), Is.False);
        Assert.That(writes, Has.Count.EqualTo(1));
    }

    [Test]
    public void ReadRowCleanRowCausesNoWrite()
    {
        var (scylla, writes) = ReadHarness();
        var row = Row(Other, Other2, Bid(Other2, 300));
        row.Coop = new List<string> { Other.ToString("N") };

        var result = scylla.ReadRow(row);

        Assert.That(writes, Is.Empty);
        Assert.That(result.Coop, Is.EqualTo(new[] { Other.ToString("N") }));
        Assert.That(result.Bids.Single().Bidder, Is.EqualTo(Other2.ToString("N")));
    }

    [Test]
    public void ReadRowStillReturnsAnonymizedDataWhenWriteBackThrows()
    {
        var (scylla, _) = ReadHarness(_ => throw new InvalidOperationException("scylla down"));
        var async = ReadHarness(_ => Task.FromException(new InvalidOperationException("scylla down"))).scylla;
        foreach (var service in new[] { scylla, async })
        {
            var row = Row(Other, Other2, Bid(Other2, 300));
            row.Coop = new List<string> { OptedOut.ToString("N") };
            SaveAuction result = null;
            Assert.DoesNotThrow(() => result = service.ReadRow(row));
            Assert.That(result.Coop, Is.Empty);
        }
    }

    // ---------- erase validation ----------

    private class FakeStore : IPlayerPrivacyStore
    {
        public List<CassandraBid> Bids = new();
        public List<ScyllaAuction> Auctions = new();
        public List<string> Writes = new();
        public int S3Objects = 2;
        public int UuidLookups;
        public List<Guid> LookedUp = new();
        public int InFlight;
        public int MaxInFlight;
        public int LookupDelayMs;
        public Action<int> OnLookup;

        public Task<List<CassandraBid>> GetBids(Guid player, CancellationToken ct) => Task.FromResult(Bids.Where(b => b.BidderUuid == player).ToList());
        public Task<List<ScyllaAuction>> GetAuctionsBySeller(Guid player, CancellationToken ct) => Task.FromResult(Auctions.Where(a => a.Auctioneer == player).ToList());
        public async Task<List<ScyllaAuction>> GetAuctionsByAuctionUuid(Guid auctionUuid, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            int count;
            lock (LookedUp) { LookedUp.Add(auctionUuid); count = ++UuidLookups; }
            var now = Interlocked.Increment(ref InFlight);
            int seen;
            while ((seen = MaxInFlight) < now && Interlocked.CompareExchange(ref MaxInFlight, now, seen) != seen) { }
            try
            {
                OnLookup?.Invoke(count);
                if (LookupDelayMs > 0)
                    await Task.Delay(LookupDelayMs, ct);
                else
                    await Task.Yield();
            }
            finally { Interlocked.Decrement(ref InFlight); }
            return Auctions.Where(a => a.Uuid == auctionUuid).ToList();
        }
        public Task UpdateAuctionIdentity(ScyllaAuction row, CancellationToken ct) { lock (Writes) Writes.Add("update " + row.AuctionUid); return Task.CompletedTask; }
        public Task DeleteBids(Guid player, CancellationToken ct) { lock (Writes) Writes.Add("deleteBids"); Bids.RemoveAll(b => b.BidderUuid == player); return Task.CompletedTask; }
        public Task<Dictionary<int, List<PlayerParticipationEntry>>> GetS3Participation(Guid player, CancellationToken ct) => Task.FromResult(new Dictionary<int, List<PlayerParticipationEntry>> { [2024] = new() });
        public Task<int> DeleteS3PlayerIndex(Guid player, CancellationToken ct) { lock (Writes) Writes.Add("deleteS3"); return Task.FromResult(S3Objects); }
    }

    private static (PlayerPrivacyService service, FakeStore store) Setup()
    {
        var store = new FakeStore();
        var sellerRow = Row(OptedOut, Other, Bid(Other, 200));
        sellerRow.AuctionUid = 1;
        var bidRow = Row(Other2, OptedOut, Bid(OptedOut, 300));
        bidRow.AuctionUid = 2;
        var unrelated = Row(Other, Other2, Bid(Other2, 300));
        unrelated.AuctionUid = 3;
        store.Auctions.AddRange(new[] { sellerRow, bidRow, unrelated });
        store.Bids.Add(new CassandraBid { AuctionUuid = bidRow.Uuid, BidderUuid = OptedOut, Amount = 300, Timestamp = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc) });
        return (new PlayerPrivacyService(store, NullLogger<PlayerPrivacyService>.Instance), store);
    }

    [Test]
    public async Task ExportContainsKeysBidsAndNotCoveredNote()
    {
        var (service, store) = Setup();
        var export = await service.Export(OptedOut);
        Assert.That(export.PlayerUuid, Is.EqualTo(OptedOut.ToString("N")));
        Assert.That(export.Bids, Has.Count.EqualTo(1));
        Assert.That(export.Auctions.Select(a => a.AuctionUid), Is.EquivalentTo(new long[] { 1, 2 }));
        Assert.That(export.Auctions.All(a => a.Tag == "T" && a.TimeKey == 5 && a.IsSold && a.End.Year == 2024), Is.True);
        Assert.That(export.Auctions.First(a => a.AuctionUid == 2).Bids.Single().BidderUuid, Is.EqualTo(OptedOut));
        Assert.That(export.S3ParticipationYears, Is.EqualTo(new[] { 2024 }));
        Assert.That(export.NotCovered, Does.Contain("coop"));
        Assert.That(store.Writes, Is.Empty);
    }

    [Test]
    public async Task OtherSnapshotsOfBidAuctionsWithoutThePlayerAreNotExported()
    {
        // regression: the unsold snapshot row of an auction the player later bid on made the erase fail with "does not involve the player"
        var (service, store) = Setup();
        var bidRow = store.Auctions.Single(a => a.AuctionUid == 2);
        var snapshot = Row(Other2, Guid.Empty);
        snapshot.AuctionUid = 2;
        snapshot.Uuid = bidRow.Uuid;
        snapshot.IsSold = false;
        store.Auctions.Add(snapshot);

        var export = await service.Export(OptedOut);
        Assert.That(export.Auctions.Count(a => a.AuctionUid == 2), Is.EqualTo(1));
        Assert.That(export.Auctions.Single(a => a.AuctionUid == 2).IsSold, Is.True);

        var result = await service.Erase(OptedOut, export);
        Assert.That(result.AuctionsRewritten, Is.EqualTo(2));
    }

    [Test]
    public async Task EraseRewritesExportedRowsDeletesBidsAndS3()
    {
        var (service, store) = Setup();
        PlayerOptOut.ResetMemory();
        var export = await service.Export(OptedOut);

        var result = await service.Erase(OptedOut, export);

        Assert.That(result.BidsDeleted, Is.EqualTo(1));
        Assert.That(result.AuctionsRewritten, Is.EqualTo(2));
        Assert.That(result.S3PlayerIndexObjectsDeleted, Is.EqualTo(2));
        Assert.That(store.Writes, Is.EquivalentTo(new[] { "update 1", "update 2", "deleteS3", "deleteBids" }));
        Assert.That(store.Writes.Last(), Is.EqualTo("deleteBids"));
        Assert.That(store.Auctions.Where(a => PlayerPrivacyService.Involves(a, OptedOut)), Is.Empty);
    }

    private static void AddManyBids(FakeStore store, int distinctAuctions, int duplicatesEach)
    {
        for (var i = 0; i < distinctAuctions; i++)
        {
            var row = Row(Other, OptedOut, Bid(OptedOut, 100 + i));
            row.AuctionUid = 100 + i;
            store.Auctions.Add(row);
            for (var d = 0; d < duplicatesEach; d++)
                store.Bids.Add(new CassandraBid { AuctionUuid = row.Uuid, BidderUuid = OptedOut, Amount = 100 + d, Timestamp = new DateTime(2024, 1, 1, 0, 0, d, DateTimeKind.Utc) });
        }
    }

    [Test]
    public async Task DuplicateAuctionUuidsInBidsAreLookedUpOnce()
    {
        var (service, store) = Setup();
        store.Bids.Clear();
        AddManyBids(store, 5, 4);

        var export = await service.Export(OptedOut);

        Assert.That(store.Bids, Has.Count.EqualTo(20));
        Assert.That(store.UuidLookups, Is.EqualTo(5));
        Assert.That(store.LookedUp.Distinct().Count(), Is.EqualTo(5));
        Assert.That(export.Auctions.Count(a => a.AuctionUid >= 100), Is.EqualTo(5));
    }

    [Test]
    public async Task AuctionLookupsRunConcurrentlyButBounded()
    {
        var (service, store) = Setup();
        store.Bids.Clear();
        AddManyBids(store, 40, 1);
        store.LookupDelayMs = 20;

        await service.Export(OptedOut);

        Assert.That(store.UuidLookups, Is.EqualTo(40));
        Assert.That(store.MaxInFlight, Is.GreaterThan(1));
        Assert.That(store.MaxInFlight, Is.LessThanOrEqualTo(PlayerPrivacyService.MaxConcurrency));
    }

    [Test]
    public async Task EraseWithManyBidsKeepsResultAndBound()
    {
        var (service, store) = Setup();
        // the fixture bids stay: auctions the player only bid on (incl. as highest bidder) are found through the bids table,
        // there is no lookup by highest bidder (full table scan in production)
        AddManyBids(store, 30, 2);
        store.LookupDelayMs = 5;
        var export = await service.Export(OptedOut);
        store.MaxInFlight = 0;

        var result = await service.Erase(OptedOut, export);

        Assert.That(result.BidsDeleted, Is.EqualTo(export.Bids.Count));
        Assert.That(result.BidsDeleted, Is.GreaterThanOrEqualTo(60));
        Assert.That(result.AuctionsRewritten, Is.EqualTo(export.Auctions.Count));
        Assert.That(result.AuctionsRewritten, Is.GreaterThanOrEqualTo(31));
        Assert.That(store.MaxInFlight, Is.LessThanOrEqualTo(PlayerPrivacyService.MaxConcurrency));
        Assert.That(store.Auctions.Where(a => PlayerPrivacyService.Involves(a, OptedOut)), Is.Empty);
    }

    [Test]
    public void CancellationStopsFurtherLookups()
    {
        var (service, store) = Setup();
        store.Bids.Clear();
        AddManyBids(store, 50, 1);
        store.LookupDelayMs = 20;
        using var cts = new CancellationTokenSource();
        store.OnLookup = n => { if (n == 3) cts.Cancel(); };

        Assert.CatchAsync<OperationCanceledException>(() => service.Export(OptedOut, cts.Token));

        Assert.That(store.UuidLookups, Is.LessThan(50));
        Assert.That(store.UuidLookups, Is.LessThanOrEqualTo(3 + PlayerPrivacyService.MaxConcurrency));
    }

    private static void AssertRejectedWithoutWrites(Func<Task> act, FakeStore store, int status)
    {
        var e = Assert.ThrowsAsync<PrivacyException>(() => act());
        Assert.That(e.StatusCode, Is.EqualTo(status), e.Message);
        Assert.That(store.Writes, Is.Empty);
    }

    [Test]
    public async Task EraseRejectsWrongUuid()
    {
        var (service, store) = Setup();
        var export = await service.Export(OptedOut);
        export.PlayerUuid = Other.ToString("N");
        AssertRejectedWithoutWrites(() => service.Erase(OptedOut, export), store, 400);
    }

    [Test]
    public async Task EraseRejectsBidsOfAnotherPlayer()
    {
        var (service, store) = Setup();
        var export = await service.Export(OptedOut);
        export.Bids[0].BidderUuid = Other;
        AssertRejectedWithoutWrites(() => service.Erase(OptedOut, export), store, 400);
    }

    [Test]
    public async Task EraseRejectsForeignAuctionRow()
    {
        var (service, store) = Setup();
        var export = await service.Export(OptedOut);
        var foreign = store.Auctions.First(a => a.AuctionUid == 3);
        export.Auctions.Add(new ExportedAuction { Tag = foreign.Tag, TimeKey = foreign.TimeKey, IsSold = foreign.IsSold, End = foreign.End, AuctionUid = foreign.AuctionUid, Uuid = foreign.Uuid });
        AssertRejectedWithoutWrites(() => service.Erase(OptedOut, export), store, 409);
    }

    [Test]
    public async Task EraseRejectsPlayerThatDidNotOptOut()
    {
        var (service, store) = Setup();
        var export = await service.Export(Other);
        AssertRejectedWithoutWrites(() => service.Erase(Other, export), store, 409);
    }

    [Test]
    public async Task EraseRejectsChangedScope()
    {
        var (service, store) = Setup();
        var export = await service.Export(OptedOut);
        // a new auction appears after the export
        var added = Row(OptedOut, Other, Bid(Other, 1));
        added.AuctionUid = 99;
        store.Auctions.Add(added);
        AssertRejectedWithoutWrites(() => service.Erase(OptedOut, export), store, 409);

        store.Auctions.Remove(added);
        store.Bids.Add(new CassandraBid { AuctionUuid = Guid.NewGuid(), BidderUuid = OptedOut, Timestamp = DateTime.UtcNow });
        AssertRejectedWithoutWrites(() => service.Erase(OptedOut, export), store, 409);
    }

    // ---------- lazy S3 scrub ----------

    private class FakeS3
    {
        public static S3StorageService Create(List<string> puts, Dictionary<string, byte[]> objects)
        {
            var client = new Mock<IAmazonS3>();
            client.Setup(c => c.GetObjectMetadataAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Returns((string b, string key, CancellationToken t) => objects.ContainsKey(key)
                    ? Task.FromResult(new GetObjectMetadataResponse())
                    : throw new AmazonS3Exception("nf") { StatusCode = System.Net.HttpStatusCode.NotFound });
            client.Setup(c => c.GetObjectAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Returns((string b, string key, CancellationToken t) => objects.TryGetValue(key, out var data)
                    ? Task.FromResult(new GetObjectResponse { ResponseStream = new MemoryStream(data) })
                    : throw new AmazonS3Exception("nf") { StatusCode = System.Net.HttpStatusCode.NotFound });
            client.Setup(c => c.PutObjectAsync(It.IsAny<PutObjectRequest>(), It.IsAny<CancellationToken>()))
                .Returns((PutObjectRequest r, CancellationToken t) =>
                {
                    using var ms = new MemoryStream();
                    r.InputStream.CopyTo(ms);
                    objects[r.Key] = ms.ToArray();
                    puts.Add(r.Key);
                    return Task.FromResult(new PutObjectResponse());
                });
            return new S3StorageService(client.Object, new ConfigurationBuilder().Build(), NullLogger<S3StorageService>.Instance);
        }
    }

    [Test]
    public async Task ReadAuctionsMasksOptedOutAndWritesBackOnce()
    {
        var puts = new List<string>();
        var objects = new Dictionary<string, byte[]>();
        var serializer = new AuctionBlobSerializer();
        var month = new DateTime(2024, 3, 1);
        var key = S3AuctionBlobService.BlobKey("TEST_ITEM", month);
        var dirty = NewAuction(OptedOut.ToString("N"), Other.ToString("N"), OptedOut.ToString("N"));
        var clean = NewAuction(Other.ToString("N"), Other2.ToString("N"));
        objects[key] = serializer.Serialize(new[] { dirty, clean });
        var blobs = new S3AuctionBlobService(FakeS3.Create(puts, objects), serializer, NullLogger<S3AuctionBlobService>.Instance);

        var read = await blobs.ReadAuctions("TEST_ITEM", month);

        Assert.That(read.Any(a => Mentions(a, OptedOut)), Is.False);
        Assert.That(read.Single(a => a.Uuid == clean.Uuid).AuctioneerId, Is.EqualTo(Other.ToString("N")));
        Assert.That(puts, Is.EqualTo(new[] { key }));
        Assert.That(serializer.Deserialize(objects[key]).Any(a => Mentions(a, OptedOut)), Is.False);

        // second read finds nothing to scrub
        await blobs.ReadAuctions("TEST_ITEM", month);
        Assert.That(puts, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task ReadAuctionsDoesNotRewriteCleanBlob()
    {
        var puts = new List<string>();
        var objects = new Dictionary<string, byte[]>();
        var serializer = new AuctionBlobSerializer();
        var month = new DateTime(2024, 3, 1);
        objects[S3AuctionBlobService.BlobKey("TEST_ITEM", month)] = serializer.Serialize(new[] { NewAuction(Other.ToString("N"), Other2.ToString("N")) });
        var blobs = new S3AuctionBlobService(FakeS3.Create(puts, objects), serializer, NullLogger<S3AuctionBlobService>.Instance);

        var read = await blobs.ReadAuctions("TEST_ITEM", month);

        Assert.That(read, Has.Count.EqualTo(1));
        Assert.That(puts, Is.Empty);
    }

    [Test]
    public async Task WriteAuctionsScrubsExistingAndNewAuctions()
    {
        var puts = new List<string>();
        var objects = new Dictionary<string, byte[]>();
        var serializer = new AuctionBlobSerializer();
        var month = new DateTime(2024, 3, 1);
        var key = S3AuctionBlobService.BlobKey("TEST_ITEM", month);
        objects[key] = serializer.Serialize(new[] { NewAuction(OptedOut.ToString("N"), Other.ToString("N")) });
        var blobs = new S3AuctionBlobService(FakeS3.Create(puts, objects), serializer, NullLogger<S3AuctionBlobService>.Instance);

        await blobs.WriteAuctions("TEST_ITEM", month, new[] { NewAuction(Other.ToString("N"), OptedOut.ToString("N")) });

        var stored = serializer.Deserialize(objects[key]);
        Assert.That(stored, Has.Count.EqualTo(2));
        Assert.That(stored.Any(a => Mentions(a, OptedOut)), Is.False);
        Assert.That(puts, Has.Count.EqualTo(1));
    }
}
