using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.Auctions.Models;
using Coflnet.Sky.Auctions.Services;
using Coflnet.Sky.Core;
using Coflnet.Sky.Core.Archive;
using Microsoft.Extensions.Logging.Abstractions;
using NUnit.Framework;

namespace Coflnet.Sky.Auctions.Archive;

/// <summary>Reader fallbacks to the R2 archive v2 (uuid lookup, player auctions, player history, privacy export) for when old rows are deleted from MariaDB and Scylla.</summary>
public class ArchiveFallbackTests
{
    private const string Tag = "HYPERION";
    private const ushort TagId = 3;
    private const int Sep = 202009;
    private const uint PlayerId = 42;
    private static readonly Guid Player = Guid.Parse("00000000000000000000000000000abc");
    private static readonly DateTime SepStart = new(2020, 9, 1, 0, 0, 0, DateTimeKind.Utc);

    private InMemoryAuctionBlobStore store;
    private InMemoryLocationLookup lookup;
    private ArchiveReadService service;

    [SetUp]
    public void SetUp()
    {
        PlayerOptOut.ResetMemory();
        store = new InMemoryAuctionBlobStore();
        lookup = new InMemoryLocationLookup();
        lookup.Tags[TagId] = Tag;
        service = new ArchiveReadService(new ArchivedAuctionReader(store, lookup, NullLogger.Instance),
            (id, _) => Task.FromResult<string>(null),
            (uuid, _) => Task.FromResult<uint?>(uuid == Player ? PlayerId : null));
    }

    [TearDown]
    public void TearDown() => PlayerOptOut.ResetMemory();

    // AuctionService.GetId derives the uid from the first 17 characters, so the uid has to be at the front
    private static string Uuid(long uid) => uid.ToString("x16").PadRight(32, '0');

    private static SaveAuction Row(long uid, int day) => new()
    {
        Uuid = Uuid(uid),
        UId = AuctionService.Instance.GetId(Uuid(uid)),
        Tag = Tag,
        ItemName = "Hyperion",
        End = SepStart.AddDays(day),
        Start = SepStart.AddDays(day - 1),
        AuctioneerId = Player.ToString("N"),
        StartingBid = 100,
        HighestBidAmount = 500,
        Count = 1,
        Tier = Tier.LEGENDARY,
        Category = Category.WEAPON,
        ItemCreatedAt = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc),
        NbtData = new NbtData { data = new byte[] { 1, 2, 3 } },
        Bids = new List<SaveBids> { new() { Bidder = Uuid(5000), Amount = 500, Timestamp = SepStart.AddDays(day - 1).AddHours(1) } },
        Enchantments = new List<Enchantment>(),
        NBTLookup = new List<NBTLookup>(),
    };

    /// <summary>Archives the rows as part 0 of Hyperion 2020-09 with locations, and the player as their seller (and bidder for <paramref name="bidOn"/>).</summary>
    private List<SaveAuction> Archive(int[] days, int[] bidOn = null)
    {
        var rows = days.Select((d, i) => Row(i + 1, d)).ToList();
        var block = ArchivedAuctionBlock.FromAuctions(Tag, Sep, 0, ArchivedAuctionBlock.SourceMariaDb, rows);
        store.WriteBlockAsync(block).Wait();
        store.WriteIndexAsync(new ArchivedTagIndex { Tag = Tag }.Upsert(new ArchivedMonthEntry
        {
            Month = Sep, Parts = 1, Rows = rows.Count, SoldRows = rows.Count, PartSha256 = new List<string> { block.Sha256() },
            Source = ArchivedAuctionBlock.SourceMariaDb, VerifiedAt = SepStart.AddMonths(2),
        })).Wait();
        foreach (var r in rows)
        {
            lookup.Locations[r.UId] = new AuctionLocation { Uid = r.UId, TagId = TagId, Month = ArchiveMonths.ToIndex(Sep), Part = 0 };
            lookup.Sellers.Add(new AuctionSeller { SellerId = PlayerId, Uid = r.UId, End = (uint)new DateTimeOffset(r.End).ToUnixTimeSeconds() });
        }
        foreach (var i in bidOn ?? Array.Empty<int>())
            lookup.Bidders.Add(new AuctionBidder { BidderId = PlayerId, Uid = rows[i].UId, End = (uint)new DateTimeOffset(rows[i].End).ToUnixTimeSeconds() });
        return rows;
    }

    private class TieredLookup : UuidLookupService
    {
        public SaveAuction FromMaria, FromScylla;
        public readonly List<string> Calls = new();

        public TieredLookup(ArchiveReadService archive) : base(null, null, NullLogger<UuidLookupService>.Instance, archive) { }

        protected override Task<SaveAuction> FindInMariaDb(Guid uuid) { Calls.Add("maria"); return Task.FromResult(FromMaria); }
        protected override Task<SaveAuction> FindInScylla(Guid uuid) { Calls.Add("scylla"); return Task.FromResult(FromScylla); }
    }

    // ---------------------------------------------------------------- 1a uuid lookup

    [Test]
    public async Task Uuid_AllLiveTiersMiss_ArchivedAuctionIsReturnedWithBids()
    {
        var rows = Archive(new[] { 3, 5 });
        var lookupService = new TieredLookup(service);

        var found = await lookupService.ResolveAuction(Guid.Parse(rows[1].Uuid));

        Assert.That(lookupService.Calls, Is.EqualTo(new[] { "maria", "scylla" }));
        Assert.That(found, Is.Not.Null);
        Assert.That((found.Uuid, found.UId, found.Tag, found.End, found.HighestBidAmount), Is.EqualTo((rows[1].Uuid, rows[1].UId, Tag, rows[1].End, 500L)));
        Assert.That(found.Bids.Single().Amount, Is.EqualTo(500));
    }

    [Test]
    public async Task Uuid_UnknownAuction_StaysNull()
    {
        Archive(new[] { 3 });
        var lookupService = new TieredLookup(service);

        Assert.That(await lookupService.ResolveAuction(Guid.Parse(Uuid(999))), Is.Null);
    }

    [Test]
    public async Task Uuid_EarlierTierHit_DoesNotTouchTheArchive()
    {
        var rows = Archive(new[] { 3 });
        store.Events.Clear();
        lookup.Calls.Clear();
        var live = Row(1, 3);
        var viaMaria = new TieredLookup(service) { FromMaria = live };
        var viaScylla = new TieredLookup(service) { FromScylla = live };

        Assert.That(await viaMaria.ResolveAuction(Guid.Parse(rows[0].Uuid)), Is.SameAs(live));
        Assert.That(await viaScylla.ResolveAuction(Guid.Parse(rows[0].Uuid)), Is.SameAs(live));

        Assert.That(viaMaria.Calls, Is.EqualTo(new[] { "maria" }));
        Assert.That(store.Events, Is.Empty, "R2 not read");
        Assert.That(lookup.Calls, Is.Empty, "MariaDB archive index not read");
    }

    [Test]
    public async Task Uuid_WithoutArchive_BehavesAsBefore()
    {
        var lookupService = new TieredLookup(null);
        Assert.That(await lookupService.ResolveAuction(Guid.Parse(Uuid(1))), Is.Null);
    }

    [Test]
    public async Task Uuid_ArchiveFailure_IsLoggedAndTheAuctionIsNotFound()
    {
        var rows = Archive(new[] { 3 });
        store.Objects.Remove(AuctionBlobKeys.Block(Tag, Sep, 0)); // located but the part is gone
        var lookupService = new TieredLookup(service);

        Assert.That(await lookupService.ResolveAuction(Guid.Parse(rows[0].Uuid)), Is.Null);
    }

    // ---------------------------------------------------------------- 1b player auctions

    [Test]
    public async Task PlayerAuctions_ScyllaShort_ContinuesWithArchivedSellsOlderThanEnd_NewestFirst_WithoutDuplicates()
    {
        var rows = Archive(new[] { 3, 5, 8, 12 });
        var scylla = new List<SaveAuction> { rows[3] }; // Scylla still has the newest, which is also archived

        var result = await service.ContinueFromArchiveAsync(Player, SepStart.AddDays(20), 3, scylla);

        Assert.That(result.Select(a => a.Uuid), Is.EqualTo(new[] { rows[3].Uuid, rows[2].Uuid, rows[1].Uuid }), "scylla rows first, then archive newest first, no duplicate");
    }

    [Test]
    public async Task PlayerAuctions_CombinedResultIsSortedNewestFirst()
    {
        var rows = Archive(new[] { 3, 5, 8 });
        var older = Row(99, 1); // Scylla returned something older than the archived rows

        var result = await service.ContinueFromArchiveAsync(Player, SepStart.AddDays(20), 3, new[] { older });

        Assert.That(result.Select(a => a.End), Is.Ordered.Descending);
        Assert.That(result.Select(a => a.Uuid), Is.EqualTo(new[] { rows[2].Uuid, rows[1].Uuid, older.Uuid }));
    }

    [Test]
    public async Task PlayerAuctions_ArchiveThatTimesOutOrFails_ReturnsTheScyllaResultWithAWarning()
    {
        var scylla = new List<SaveAuction> { Row(99, 1) };
        var slow = new ArchiveReadService(new ArchivedAuctionReader(store, lookup, NullLogger.Instance),
            async (id, ct) => { await Task.Delay(Timeout.Infinite, ct); return null; }, (uuid, _) => Task.FromResult<uint?>(PlayerId));
        slow.ReadTimeout = TimeSpan.FromMilliseconds(50);
        var broken = new ArchiveReadService(new ArchivedAuctionReader(store, lookup, NullLogger.Instance),
            (id, _) => Task.FromResult<string>(null), (uuid, _) => throw new InvalidOperationException("mariadb gone"));

        var timedOut = await slow.ContinueFromArchiveOrFallbackAsync(Player, SepStart.AddDays(20), 3, scylla);
        var failed = await broken.ContinueFromArchiveOrFallbackAsync(Player, SepStart.AddDays(20), 3, scylla);

        Assert.That((timedOut.Warning, timedOut.Value), Is.EqualTo(("timeout", scylla)));
        Assert.That((failed.Warning, failed.Value), Is.EqualTo(("failed", scylla)));
    }

    [Test]
    public async Task ReadOrFallback_AnsweringInTime_HasNoWarning_AndACancelledCallerIsRethrown()
    {
        var ok = await service.ReadOrFallbackAsync(_ => Task.FromResult(5), -1, "test");
        Assert.That((ok.Value, ok.Warning), Is.EqualTo((5, (string)null)));
        Assert.That(service.ReadTimeout, Is.EqualTo(TimeSpan.FromMilliseconds(ArchiveReadService.DefaultReadTimeoutMs)));

        using var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.That(async () => await service.ReadOrFallbackAsync(ct => Task.Delay(Timeout.Infinite, ct).ContinueWith(_ => 1, ct), -1, "test", cts.Token), Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task PlayerAuctions_ScyllaFull_ArchiveIsNotRead()
    {
        var rows = Archive(new[] { 3, 5 });
        store.Events.Clear();
        lookup.Calls.Clear();

        var result = await service.ContinueFromArchiveAsync(Player, SepStart.AddDays(20), 2, new[] { rows[0], rows[1] });

        Assert.That(result.Count, Is.EqualTo(2));
        Assert.That(lookup.Calls, Is.Empty);
        Assert.That(store.Events, Is.Empty);
    }

    [Test]
    public async Task PlayerAuctions_OnlyAuctionsEndedBeforeEnd_UnknownAndOptedOutPlayersGetNothing()
    {
        var rows = Archive(new[] { 3, 5, 8 });

        var before = await service.ContinueFromArchiveAsync(Player, SepStart.AddDays(5), 10, new List<SaveAuction>());
        Assert.That(before.Select(a => a.Uuid), Is.EqualTo(new[] { rows[0].Uuid }), "End < end, the auction ending exactly at end is excluded");

        Assert.That(await service.ContinueFromArchiveAsync(Guid.Parse(Uuid(77)), SepStart.AddDays(20), 10, new List<SaveAuction>()), Is.Empty, "unknown player");

        var optedOut = new ArchiveReadService(new ArchivedAuctionReader(store, lookup, NullLogger.Instance),
            (id, _) => Task.FromResult(PlayerOptOut.LegacyPlayerUuids[0]), (uuid, _) => Task.FromResult<uint?>(PlayerId));
        Assert.That(await optedOut.ContinueFromArchiveAsync(Player, SepStart.AddDays(20), 10, new List<SaveAuction>()), Is.Empty, "opted out");
    }

    [Test]
    public async Task PlayerHistory_ParticipationsFromTheArchive_HaveTheV1EntryShape_AndRespectTypeAndRange()
    {
        var rows = Archive(new[] { 3, 5, 8 }, bidOn: new[] { 1 });

        var both = await service.GetPlayerParticipationsAsync(Player, null, new DateTime(2019, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(2021, 1, 1, 0, 0, 0, DateTimeKind.Utc), 100);
        var bids = await service.GetPlayerParticipationsAsync(Player, ParticipationType.Bidder, new DateTime(2019, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(2021, 1, 1, 0, 0, 0, DateTimeKind.Utc), 100);
        var outOfRange = await service.GetPlayerParticipationsAsync(Player, null, new DateTime(2021, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(2022, 1, 1, 0, 0, 0, DateTimeKind.Utc), 100);

        Assert.That(both.Count, Is.EqualTo(4));
        Assert.That(both.Select(e => e.End), Is.Ordered.Descending);
        Assert.That(both.Count(e => e.Type == ParticipationType.Seller), Is.EqualTo(3));
        var bid = bids.Single();
        Assert.That((bid.AuctionUid, bid.Tag, bid.End, bid.Type), Is.EqualTo((Guid.Parse(rows[1].Uuid), Tag, rows[1].End, ParticipationType.Bidder)));
        Assert.That(outOfRange, Is.Empty);
    }

    // ---------------------------------------------------------------- 1c privacy export

    private class ListingStore : IPlayerPrivacyStore
    {
        public Task<List<CassandraBid>> GetBids(Guid player, CancellationToken ct) => Task.FromResult(new List<CassandraBid>());
        public Task<List<ScyllaAuction>> GetAuctionsBySeller(Guid player, CancellationToken ct) => Task.FromResult(new List<ScyllaAuction>());
        public Task<List<ScyllaAuction>> GetAuctionsByAuctionUuid(Guid auctionUuid, CancellationToken ct) => Task.FromResult(new List<ScyllaAuction>());
        public Task UpdateAuctionIdentity(ScyllaAuction row, CancellationToken ct) => Task.CompletedTask;
        public Task DeleteBids(Guid player, CancellationToken ct) => Task.CompletedTask;
        public Task<Dictionary<int, List<PlayerParticipationEntry>>> GetS3Participation(Guid player, CancellationToken ct) => Task.FromResult(new Dictionary<int, List<PlayerParticipationEntry>>());
        public Task<int> DeleteS3PlayerIndex(Guid player, CancellationToken ct) => Task.FromResult(0);
        public (bool Truncated, string Reason) Incomplete;
        public Task<(List<ArchivedPlayerEntry> Entries, bool Truncated, string Reason)> GetArchiveV2Participation(Guid player, CancellationToken ct)
            => Task.FromResult((new List<ArchivedPlayerEntry> { new() { Role = "Seller", AuctionUid = 1, Tag = Tag } }, Incomplete.Truncated, Incomplete.Reason));
    }

    [Test]
    public async Task Export_ListsTheArchiveV2Entries_AndEraseIgnoresThem()
    {
        var privacy = new PlayerPrivacyService(new ListingStore(), NullLogger<PlayerPrivacyService>.Instance);
        var optedOut = Guid.Parse(PlayerOptOut.LegacyPlayerUuids[0]);

        var export = await privacy.Export(optedOut);

        Assert.That(export.ArchiveV2.Single().AuctionUid, Is.EqualTo(1));
        Assert.That(export.NotCovered, Does.Contain("ArchiveV2"));
        export.ArchiveV2 = new List<ArchivedPlayerEntry>(); // the erase scope check is on bids and auctions only
        Assert.That(async () => await privacy.Erase(optedOut, export), Throws.Nothing);
    }

    [Test]
    public async Task ArchiveStore_ListsSellerAndBidderIndexEntries_IncludingUnlocatedUids_WithoutReadingBlocksOrMasking()
    {
        Archive(new[] { 3, 5 }, bidOn: new[] { 0 });
        lookup.Sellers.Add(new AuctionSeller { SellerId = PlayerId, Uid = 123456, End = (uint)new DateTimeOffset(SepStart.AddDays(9)).ToUnixTimeSeconds() });
        store.Events.Clear();
        var privacyStore = new ScyllaPlayerPrivacyStore(null, archiveV2: service);

        var (entries, truncated, reason) = await privacyStore.GetArchiveV2Participation(Player, CancellationToken.None);

        Assert.That((truncated, reason), Is.EqualTo((false, (string)null)));
        Assert.That(entries.Count(e => e.Role == "Seller"), Is.EqualTo(3));
        Assert.That(entries.Single(e => e.Role == "Bidder").Tag, Is.EqualTo(Tag));
        var unlocated = entries.Single(e => e.AuctionUid == 123456);
        Assert.That((unlocated.Tag, unlocated.Month), Is.EqualTo((null as string, 0)));
        Assert.That(store.Events, Is.Empty, "only the MariaDB index is read");
        Assert.That((await new ScyllaPlayerPrivacyStore(null).GetArchiveV2Participation(Player, CancellationToken.None)).Entries, Is.Empty, "archive disabled");
    }

    [Test]
    public async Task ArchiveStore_ListingThatFails_ReturnsWhatWasReadAsTruncatedWithAReason_InsteadOfFailing()
    {
        Archive(new[] { 3, 5 }, bidOn: new[] { 0 });
        lookup.BidderRowsFailure = new TimeoutException("mariadb gone");
        var privacyStore = new ScyllaPlayerPrivacyStore(null, archiveV2: service);

        var (entries, truncated, reason) = await privacyStore.GetArchiveV2Participation(Player, CancellationToken.None);

        Assert.That(entries.Count(e => e.Role == "Seller"), Is.EqualTo(2), "the seller entries read before the failure are kept");
        Assert.That(truncated, Is.True);
        Assert.That(reason, Does.Contain("TimeoutException").And.Contain("mariadb gone").And.Contain("2 entries"));
    }

    [Test]
    public async Task ArchiveStore_CapIs200kPerRole_BelowItTheListingIsComplete()
    {
        Assert.That(ScyllaPlayerPrivacyStore.ArchiveV2MaxEntriesPerRole, Is.EqualTo(200_000));
        for (var i = 1; i <= 2 * ScyllaPlayerPrivacyStore.ArchiveV2PageSize; i++)
            lookup.Sellers.Add(new AuctionSeller { SellerId = PlayerId, Uid = i, End = (uint)(1_600_000_000 + i) });
        var privacyStore = new ScyllaPlayerPrivacyStore(null, archiveV2: service);

        var (entries, truncated, reason) = await privacyStore.GetArchiveV2Participation(Player, CancellationToken.None);

        Assert.That((entries.Count, truncated, reason), Is.EqualTo((2 * ScyllaPlayerPrivacyStore.ArchiveV2PageSize, false, (string)null)), "below the cap: complete");
    }

    [Test]
    public async Task ArchiveStore_ReportsTruncatedOnlyWhenMoreThanTheCapExists()
    {
        var privacyStore = new ScyllaPlayerPrivacyStore(null, archiveV2: service) { ArchiveV2Cap = 2500 };
        for (var i = 1; i <= 2500; i++)
            lookup.Sellers.Add(new AuctionSeller { SellerId = PlayerId, Uid = i, End = (uint)(1_600_000_000 + i) });

        var exact = await privacyStore.GetArchiveV2Participation(Player, CancellationToken.None);

        Assert.That((exact.Entries.Count, exact.Truncated), Is.EqualTo((2500, false)), "exactly the cap: nothing is left out");

        lookup.Sellers.Add(new AuctionSeller { SellerId = PlayerId, Uid = 2501, End = (uint)(1_600_000_000 + 2501) });
        var over = await privacyStore.GetArchiveV2Participation(Player, CancellationToken.None);

        Assert.That((over.Entries.Count, over.Truncated), Is.EqualTo((2500, true)));
        Assert.That(over.Reason, Does.Contain("2500"));
    }

    [Test]
    public async Task Export_WithAnIncompleteArchiveListing_SaysSoInNotCovered_AndStillSucceeds()
    {
        var store = new ListingStore { Incomplete = (true, "listing failed after 5 entries (TimeoutException: x)") };
        var privacy = new PlayerPrivacyService(store, NullLogger<PlayerPrivacyService>.Instance);

        var export = await privacy.Export(Guid.Parse(PlayerOptOut.LegacyPlayerUuids[0]));

        Assert.That(export.ArchiveV2Truncated, Is.True);
        Assert.That(export.NotCovered, Does.Contain("INCOMPLETE").And.Contain("TimeoutException"));
        Assert.That(export.ArchiveV2, Has.Count.EqualTo(1));
    }
}
