using System;
using System.Collections.Generic;
using System.Linq;
using Coflnet.Sky.Core;
using Coflnet.Sky.Core.Archive;
using fNbt;
using fNbt.Tags;
using NUnit.Framework;

namespace Coflnet.Sky.Auctions.Archive;

public class ScyllaArchiveRowsTests
{
    private const string Tag = "HYPERION";
    private const int Sep = 202009;

    private static string Hex(long value) => value.ToString("x32");

    /// <summary>A MariaDB-shaped auction with every column the archive keeps (valid NBT, so ToCassandra can parse it).</summary>
    private static SaveAuction FullAuction()
    {
        var end = new DateTime(2020, 9, 10, 3, 4, 5, 678, DateTimeKind.Utc);
        var root = new NbtCompound("i") { new NbtInt("anvil_uses", 4), new NbtString("modifier", "chomp"), new NbtString("id", Tag) };
        return new SaveAuction
        {
            Uuid = Hex(0xabc),
            UId = 0xabc,
            Tag = Tag,
            ItemName = "Heroic Hyperion",
            End = end,
            Start = end.AddDays(-2).AddMilliseconds(-17),
            AuctioneerId = Hex(1000),
            ProfileId = Hex(2000),
            Bin = false,
            HighestBidAmount = 900,
            StartingBid = 500,
            Count = 3,
            Tier = Tier.MYTHIC,
            Category = Category.WEAPON,
            Reforge = ItemReferences.Reforge.chomp,
            AnvilUses = 4,
            Claimed = true,
            ItemCreatedAt = new DateTime(2020, 1, 2, 3, 4, 5, DateTimeKind.Utc),
            NbtData = new NbtData { data = NBT.Bytes(root) },
            Bids = new List<SaveBids>
            {
                new() { Bidder = Hex(3000), ProfileId = Hex(3001), Amount = 700, Timestamp = new DateTime(2020, 9, 9, 1, 2, 3, DateTimeKind.Utc), BidderId = 31 },
                new() { Bidder = Hex(4000), ProfileId = "unknown", Amount = 900, Timestamp = new DateTime(2020, 9, 9, 1, 5, 3, DateTimeKind.Utc), BidderId = 41 },
                new() { Bidder = Hex(3000), ProfileId = Hex(3001), Amount = 800, Timestamp = new DateTime(2020, 9, 9, 1, 4, 3, DateTimeKind.Utc), BidderId = 31 },
            },
            Enchantments = new List<Enchantment>
            {
                new(Enchantment.EnchantmentType.sharpness, 7),
                new(Enchantment.EnchantmentType.transylvanian, 5),
            },
            NBTLookup = new List<NBTLookup> { new(12, 99) },
            CoopMembers = new List<UuId> { new(Hex(5000)), new("SOME-Odd-Id") },
            ClaimedBids = new List<UuId> { new(Hex(4000)) },
        };
    }

    /// <summary>
    /// The Scylla copy of an auction (as ScyllaService.ToCassandra writes it), mapped by the sweep, archives to the same block
    /// as the MariaDB row, except for the columns Scylla does not store (see MIGRATION.md, "known lossy fields").
    /// </summary>
    [Test]
    public void ScyllaMappingMatchesMariaDbMapping()
    {
        var original = FullAuction();
        var scylla = ScyllaService.ToCassandra(FullAuction());
        var (mapped, extras) = ScyllaArchiveRows.Map(scylla, Tag);

        var fromMariaDb = ArchivedAuctionBlock.FromAuctions(Tag, Sep, 0, ArchivedAuctionBlock.SourceMariaDb, new[] { original });
        var fromScylla = ArchivedAuctionBlock.FromAuctions(Tag, Sep, 0, ArchivedAuctionBlock.SourceMariaDb, new[] { mapped });

        // Not stored in weekly_auctions_2, so phase 2 can not restore them for Scylla-only rows:
        foreach (var block in new[] { fromMariaDb, fromScylla })
        {
            block.Reforge = null;        // no column (CassandraToOld derives it from nbtlookup "modifier" when present)
            block.AnvilUses = null;      // no column (derived from nbtlookup "anvil_uses" when present)
            block.Claimed = null;        // not stored
            block.ClaimedBidders = null; // not stored
            block.NbtLookup = null;      // MariaDB's short-keyed NBTLookups; Scylla's string-keyed map goes to NbtLookupStrings instead
        }
        // SaveBids.BidderId (Players.Id) is not a block column at all; Scylla has no player ids, the sweep resolves them for auction_bidders.
        Assert.That(fromMariaDb.FirstDifference(fromScylla), Is.Null);
        Assert.That(fromScylla.ItemBytes[0], Is.EqualTo(original.NbtData.data), "raw item bytes kept");
        Assert.That(mapped.Bids.Select(b => b.ProfileId), Is.EqualTo(new[] { Hex(3001), "unknown", Hex(3001) }), "bid profile 'unknown' restored");
        Assert.That(mapped.CoopMembers.Select(c => c.value), Is.EqualTo(new[] { Hex(5000), "SOME-Odd-Id" }));
        Assert.That(extras.NbtLookupStrings["anvil_uses"], Is.EqualTo("4"));
        Assert.That(extras.UnknownEnchantments, Is.Empty);
    }

    private static ScyllaAuction Row(Action<ScyllaAuction> change = null)
    {
        var row = new ScyllaAuction
        {
            Uuid = Guid.Parse(Hex(77)),
            AuctionUid = 77,
            Tag = Tag,
            End = new DateTime(2020, 9, 3, 4, 5, 6, DateTimeKind.Unspecified),
            Start = new DateTime(2020, 9, 1, 0, 0, 0, DateTimeKind.Unspecified),
            Auctioneer = Guid.Parse(Hex(1)),
            ProfileId = Guid.Empty,
            Tier = "LEGENDARY",
            Category = "WEAPON",
            ItemBytes = new byte[] { 1, 2, 3 },
            Enchantments = new Dictionary<string, int>(),
        };
        row.Bids = new List<CassandraBid>();
        change?.Invoke(row);
        return row;
    }

    [Test]
    public void ScyllaOnlyColumnsGoToTheExtras_UnknownEnchantmentsAreKept()
    {
        var itemId = Guid.NewGuid();
        var row = Row(r =>
        {
            r.HighestBidderName = "Bidder";
            r.ProfileName = "Mango";
            r.CoopName = "Coop";
            r.Color = 0x00ff00;
            r.ItemUid = 0x1234;
            r.ItemId = itemId;
            r.NbtLookup = new Dictionary<string, string> { ["uid"] = "1234" };
            r.Enchantments = new Dictionary<string, int> { ["sharpness"] = 5, ["unknown7"] = 1, ["future_enchant"] = 2, ["growth"] = 300, ["12"] = 1 };
        });
        var (mapped, extras) = ScyllaArchiveRows.Map(row, Tag);
        Assert.Multiple(() =>
        {
            Assert.That(mapped.Enchantments.Select(e => (e.Type, e.Level)), Is.EqualTo(new[] { (Enchantment.EnchantmentType.sharpness, (byte)5) }));
            Assert.That(extras.UnknownEnchantments, Is.EquivalentTo(new Dictionary<string, int> { ["unknown7"] = 1, ["future_enchant"] = 2, ["growth"] = 300, ["12"] = 1 }));
            Assert.That(extras.HighestBidderName, Is.EqualTo("Bidder"));
            Assert.That(extras.ProfileName, Is.EqualTo("Mango"));
            Assert.That(extras.CoopName, Is.EqualTo("Coop"));
            Assert.That(extras.Color, Is.EqualTo(0x00ff00));
            Assert.That(extras.ItemUid, Is.EqualTo(0x1234));
            Assert.That(ArchiveIds.Decode(extras.ItemId), Is.EqualTo(itemId.ToString("N")));
            Assert.That(extras.NbtLookupStrings["uid"], Is.EqualTo("1234"));
            Assert.That(mapped.ProfileId, Is.Null, "Guid.Empty profile is no profile");
            Assert.That(mapped.End.Kind, Is.EqualTo(DateTimeKind.Utc));
            Assert.That(mapped.NBTLookup, Is.Empty);
        });
        var block = ArchivedAuctionBlock.FromAuctions(Tag, Sep, 0, ArchivedAuctionBlock.SourceScylla, new[] { (mapped, extras) });
        Assert.That(block.ToExtras()[0].UnknownEnchantments, Has.Count.EqualTo(4));
    }

    /// <summary>Regression: CassandraToOld reads bids through the Bids getter, which throws on a row without serialisedbids.</summary>
    [Test]
    public void RowWithoutSerialisedBidsMapsToNoBids()
    {
        var row = Row(r => r.SerialisedBids = null);
        var (mapped, _) = ScyllaArchiveRows.Map(row, Tag);
        Assert.That(mapped.Bids, Is.Empty);
        Assert.DoesNotThrow(() => ArchivedAuctionBlock.FromAuctions(Tag, Sep, 0, ArchivedAuctionBlock.SourceScylla, new[] { (mapped, (ScyllaAuctionExtras)null) }));
    }

    [Test]
    public void SubMillisecondTimesAreTruncatedAndNullItemBytesStayNull()
    {
        var row = Row(r =>
        {
            r.End = new DateTime(2020, 9, 3, 4, 5, 6, DateTimeKind.Utc).AddTicks(1234);
            r.Start = r.End.AddDays(-1);
            r.ItemBytes = null;
        });
        var (mapped, _) = ScyllaArchiveRows.Map(row, Tag);
        Assert.That(mapped.End.Ticks % TimeSpan.TicksPerMillisecond, Is.Zero);
        Assert.That(mapped.Start.Ticks % TimeSpan.TicksPerMillisecond, Is.Zero);
        Assert.That(mapped.NbtData, Is.Null);
    }

    [Test]
    public void UnknownTierIsReportedOnce()
    {
        var reported = new List<(string, string)>();
        ScyllaArchiveRows.Map(Row(r => r.Tier = "COSMIC"), Tag, (c, v) => reported.Add((c, v)));
        ScyllaArchiveRows.Map(Row(r => r.Tier = "SUPREME"), Tag, (c, v) => reported.Add((c, v)));
        Assert.That(reported, Is.EqualTo(new[] { ("tier", "COSMIC") }), "SUPREME is mapped to DIVINE by CassandraToOld");
    }

    [Test]
    public void DedupeKeepsSoldThenHigherBidThenLaterEnd()
    {
        var end = new DateTime(2020, 9, 3, 0, 0, 0, DateTimeKind.Utc);
        ScyllaAuction R(long uid, bool sold, long bid, int endOffsetMinutes) => Row(r =>
        {
            r.AuctionUid = uid;
            r.IsSold = sold;
            r.HighestBidAmount = bid;
            r.End = end.AddMinutes(endOffsetMinutes);
        });
        var rows = new[]
        {
            R(1, false, 900, 50), R(1, true, 100, 0),          // sold wins over a higher unsold bid
            R(2, false, 100, 0), R(2, false, 300, -10),       // then the higher bid
            R(3, true, 500, 0), R(3, true, 500, 30),           // then the later End
            R(4, false, 0, 0),                                 // single row stays
            R(5, true, 500, 0), R(5, true, 500, 0),            // full tie keeps the first
        };
        var kept = ScyllaArchiveRows.DedupeScyllaRows(rows);
        Assert.That(kept.Select(r => r.AuctionUid), Is.EqualTo(new long[] { 1, 2, 3, 4, 5 }));
        Assert.That(kept[0], Is.SameAs(rows[1]));
        Assert.That(kept[1], Is.SameAs(rows[3]));
        Assert.That(kept[2], Is.SameAs(rows[5]));
        Assert.That(kept[4], Is.SameAs(rows[7]));
    }

    [Test]
    public void TimeKeysForMonth_WeeklyAndTwelveHourTags()
    {
        // 2020-09-01 is day 609 since 2019-01-01 = start of week 87; 2020-10-01 is day 639
        Assert.That(ScyllaArchiveRows.TimeKeysForMonth(Tag, 202009), Is.EqualTo(((short)87, (short)91)));
        // 2020-08-01 is day 578 (week 82 started 2020-07-28): the first week spans the month boundary
        Assert.That(ScyllaArchiveRows.TimeKeysForMonth(Tag, 202008), Is.EqualTo(((short)82, (short)86)));
        Assert.That(ScyllaArchiveRows.TimeKeysForMonth(Tag, 202007).Last, Is.EqualTo(82), "week 82 overlaps July and August");
        var (weekStart, weekEnd) = ScyllaArchiveRows.PartitionRange(Tag, 82);
        Assert.That(weekStart, Is.EqualTo(new DateTime(2020, 7, 28, 0, 0, 0, DateTimeKind.Utc)));
        Assert.That(weekEnd, Is.EqualTo(new DateTime(2020, 8, 4, 0, 0, 0, DateTimeKind.Utc)));

        // 12-hour buckets: 60 keys for September, 58 for February 2020
        var (first, last) = ScyllaArchiveRows.TimeKeysForMonth("ENCHANTED_BOOK", 202009);
        Assert.That((first, last), Is.EqualTo(((short)1218, (short)1277)));
        var feb = ScyllaArchiveRows.TimeKeysForMonth("ENCHANTED_BOOK", 202002);
        Assert.That(feb.Last - feb.First + 1, Is.EqualTo(58));
        Assert.That(ScyllaArchiveRows.TimeKeysForMonth("unknown", 202009), Is.EqualTo((first, last)));
        Assert.That(ScyllaArchiveRows.BucketLength("ENCHANTED_BOOK"), Is.EqualTo(TimeSpan.FromHours(12)));
        Assert.That(ScyllaArchiveRows.BucketLength(Tag), Is.EqualTo(TimeSpan.FromDays(7)));
        // every key in the range really maps back into the month
        for (var key = first; key <= last; key++)
        {
            var (s, e) = ScyllaArchiveRows.PartitionRange("ENCHANTED_BOOK", key);
            Assert.That(ArchiveMonths.MonthOf(s), Is.EqualTo(202009));
            Assert.That(ScyllaService.GetWeekOrDaysSinceStart("ENCHANTED_BOOK", s), Is.EqualTo(key));
            Assert.That(ScyllaService.GetWeekOrDaysSinceStart("ENCHANTED_BOOK", e.AddTicks(-1)), Is.EqualTo(key));
        }
    }

    [Test]
    public void PartitionScopeExcludesGarbageKeysOfTwelveHourTags()
    {
        // random keys 0..29 of rows dated before 2000 cover 2019-01-01..2019-01-16
        for (short key = 0; key < 30; key++)
            Assert.That(ScyllaArchiveRows.OverlapsMonths("ENCHANTED_BOOK", key, 201907, 202103), Is.False);
        Assert.That(ScyllaArchiveRows.OverlapsMonths(Tag, 87, 201907, 202103), Is.True);
        Assert.That(ScyllaArchiveRows.OverlapsMonths(Tag, 87, 201907, 202008), Is.False, "week 87 starts in September");
        Assert.That(ScyllaArchiveRows.OverlapsMonths(Tag, 82, 202008, 202008), Is.True, "week 82 reaches into August");
    }

    [Test]
    public void MergeMonthEntryAppendsPartsAndPicksTheSource()
    {
        var now = new DateTime(2026, 10, 23, 0, 0, 0, DateTimeKind.Utc);
        var phaseOne = new ArchivedMonthEntry { Month = Sep, Parts = 2, Rows = 10, SoldRows = 4, PartSha256 = new() { "a", "b" }, Source = ArchivedAuctionBlock.SourceMariaDb };
        var merged = ScyllaArchiveRows.MergeMonthEntry(phaseOne, Sep, new[] { "c" }, 5, 1, now);
        Assert.That((merged.Parts, merged.Rows, merged.SoldRows, merged.Source), Is.EqualTo((3, 15, 5, ArchivedAuctionBlock.SourceMerged)));
        Assert.That(merged.PartSha256, Is.EqualTo(new[] { "a", "b", "c" }));
        Assert.That(phaseOne.PartSha256, Is.EqualTo(new[] { "a", "b" }), "input not modified");
        var scyllaOnly = ScyllaArchiveRows.MergeMonthEntry(null, Sep, new[] { "c" }, 5, 1, now);
        Assert.That((scyllaOnly.Parts, scyllaOnly.Source, scyllaOnly.VerifiedAt), Is.EqualTo((1, ArchivedAuctionBlock.SourceScylla, now)));
    }

    [Test]
    public void FingerprintComparesTheCrossCheckFields()
    {
        var a = new Fingerprint(100, true, 5, 3, 7);
        Assert.That(Fingerprint.Compare(a, a), Is.Empty);
        Assert.That(Fingerprint.Compare(a, a with { HighestBidAmount = 101, ItemBytesLength = 4 }), Is.EqualTo(new[] { "highest_bid_amount", "item_bytes_length" }));
        var block = ArchivedAuctionBlock.FromAuctions(Tag, Sep, 0, ArchivedAuctionBlock.SourceMariaDb, new[] { FullAuction() });
        var fromBlock = Fingerprint.Of(block, 0);
        Assert.That(fromBlock, Is.EqualTo(new Fingerprint(900, true, FullAuction().End.Ticks, FullAuction().NbtData.data.Length, 500)));
        Assert.That(Fingerprint.Of(ScyllaService.ToCassandra(FullAuction())), Is.EqualTo(fromBlock));
    }
}
