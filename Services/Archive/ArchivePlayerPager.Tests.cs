using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.Core;
using Coflnet.Sky.Core.Archive;
using Microsoft.Extensions.Logging.Abstractions;
using NUnit.Framework;

namespace Coflnet.Sky.Auctions.Archive;

public class ArchivePlayerPagerTests
{
    private const string Tag = "HYPERION";
    private const ushort TagId = 3;
    private const uint Player = 9;
    private static readonly DateTime SepStart = new(2020, 9, 1, 0, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime OctStart = new(2020, 10, 1, 0, 0, 0, DateTimeKind.Utc);

    private InMemoryAuctionBlobStore store;
    private InMemoryLocationLookup lookup;
    private HashSet<int> deleted;
    private int deletedLookups;
    private DateTime now;
    private ArchivePlayerPager pager;

    [SetUp]
    public void SetUp()
    {
        store = new InMemoryAuctionBlobStore();
        lookup = new InMemoryLocationLookup();
        lookup.Tags[TagId] = Tag;
        deleted = new HashSet<int>();
        deletedLookups = 0;
        now = new DateTime(2026, 10, 31, 12, 0, 0, DateTimeKind.Utc);
        var reader = new ArchivedAuctionReader(store, lookup, NullLogger.Instance);
        pager = new ArchivePlayerPager(reader, lookup, _ =>
        {
            deletedLookups++;
            return Task.FromResult(deleted.ToHashSet());
        }, utcNow: () => now);
    }

    private static uint Seconds(DateTime utc) => (uint)new DateTimeOffset(utc).ToUnixTimeSeconds();

    private void Archive(long uid, DateTime end)
    {
        var month = ArchiveMonths.MonthOf(end);
        var row = new SaveAuction
        {
            Uuid = uid.ToString("x32"), UId = uid, Tag = Tag, ItemName = "x", End = end, Start = end.AddDays(-1), AuctioneerId = (1000 + uid).ToString("x32"),
            StartingBid = 1, HighestBidAmount = 0, Count = 1, Tier = Tier.LEGENDARY, Category = Category.WEAPON, ItemCreatedAt = end.AddDays(-30),
            NbtData = new NbtData { data = new byte[] { 1 } }, Bids = new List<SaveBids>(), Enchantments = new List<Enchantment>(), NBTLookup = new List<NBTLookup>(),
        };
        store.WriteBlockAsync(ArchivedAuctionBlock.FromAuctions(Tag, month, (int)uid, ArchivedAuctionBlock.SourceMariaDb, new[] { row })).Wait();
        lookup.Locations[uid] = new AuctionLocation { Uid = uid, TagId = TagId, Month = ArchiveMonths.ToIndex(month), Part = (byte)uid };
        lookup.Sellers.Add(new AuctionSeller { SellerId = Player, Uid = uid, End = Seconds(end) });
    }

    private Task<ArchivedPlayerPage> Page(int take, AuctionIndexCursor before = null, bool deletedOnly = false)
        => pager.GetPageAsync(Player, true, null, null, take, before, deletedOnly, CancellationToken.None);

    private int SellerQueries => lookup.Calls.Count(c => c.StartsWith("sellers:"));

    [Test]
    public async Task NoDeletedMonthsReturnsAnEmptyPageWithoutIndexOrBlockQueries()
    {
        Archive(1, SepStart.AddDays(2));
        store.Events.Clear();
        lookup.Calls.Clear();

        var page = await Page(10, deletedOnly: true);

        Assert.That(page.Auctions, Is.Empty);
        Assert.That(page.Next, Is.Null, "nothing to page");
        Assert.That(lookup.Calls, Is.Empty);
        Assert.That(store.Events.Where(e => e.StartsWith("read:")), Is.Empty);
    }

    [Test]
    public async Task DeletedOnlyRestrictsTheIndexQueryToTheDeletedMonthRanges()
    {
        Archive(1, SepStart.AddDays(2));
        Archive(2, OctStart.AddDays(2));
        deleted.Add(202009);
        lookup.Calls.Clear();

        var page = await Page(10, deletedOnly: true);

        Assert.That(page.Auctions.Select(a => a.Uid), Is.EqualTo(new long[] { 1 }));
        Assert.That(lookup.Calls.Where(c => c.StartsWith("sellers:")), Is.EqualTo(new[] { $"sellers:{Player}:{Seconds(SepStart)}:{Seconds(OctStart)}:11" }));
    }

    [Test]
    public async Task EachPageRunsTheIndexQueryOnce()
    {
        for (var i = 1; i <= 4; i++)
            Archive(i, SepStart.AddDays(i));
        lookup.Calls.Clear();

        var page = await Page(2);

        Assert.That(page.Auctions.Select(a => a.Uid), Is.EqualTo(new long[] { 4, 3 }));
        Assert.That(SellerQueries, Is.EqualTo(1));
        deleted.Add(202009);
        lookup.Calls.Clear();
        await Page(2, deletedOnly: true);
        Assert.That(SellerQueries, Is.EqualTo(1));
    }

    [Test]
    public async Task RowsInsertedBetweenTheIndexQueryAndTheReadCannotBeSkipped()
    {
        for (var i = 1; i <= 4; i++)
            Archive(i, SepStart.AddDays(i));
        var inserted = false;
        lookup.AfterSellerRows = () =>
        {
            if (inserted)
                return;
            inserted = true;
            Archive(10, SepStart.AddDays(20)); // newest, appears after the page's index rows were read
        };

        var first = await Page(2);
        var second = await Page(2, first.Next);

        var uids = first.Auctions.Concat(second.Auctions).Select(a => a.Uid).ToList();
        Assert.That(uids, Is.EqualTo(new long[] { 4, 3, 2, 1 }), "cursor and content come from the same index rows");
        Assert.That(second.Next, Is.Null);
    }

    [Test]
    public async Task DeletedOnlyContinuesAcrossMonthRangesWithTheLastRowAsCursor()
    {
        Archive(1, SepStart.AddDays(2));
        Archive(2, SepStart.AddDays(5));
        Archive(3, OctStart.AddDays(2));
        Archive(4, OctStart.AddDays(5));
        deleted.Add(202009);
        deleted.Add(202010);

        var first = await Page(3, deletedOnly: true);
        var second = await Page(3, first.Next, deletedOnly: true);

        Assert.That(first.Auctions.Select(a => a.Uid), Is.EqualTo(new long[] { 4, 3, 2 }));
        Assert.That(first.Next, Is.EqualTo(new AuctionIndexCursor(Seconds(SepStart.AddDays(5)), 2)));
        Assert.That(second.Auctions.Select(a => a.Uid), Is.EqualTo(new long[] { 1 }));
        Assert.That(second.Next, Is.Null);
    }

    [Test]
    public async Task DeletedMonthsAreCachedForAMinute()
    {
        Archive(1, SepStart.AddDays(2));
        deleted.Add(202009);

        await Page(10, deletedOnly: true);
        now = now.AddSeconds(30);
        await Page(10, deletedOnly: true);
        Assert.That(deletedLookups, Is.EqualTo(1));

        now = now.AddSeconds(31);
        await Page(10, deletedOnly: true);
        Assert.That(deletedLookups, Is.EqualTo(2));
    }
}
