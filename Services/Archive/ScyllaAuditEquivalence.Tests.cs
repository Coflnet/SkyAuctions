using System;
using System.Collections.Generic;
using System.Linq;
using Coflnet.Sky.Core;
using Coflnet.Sky.Core.Archive;
using fNbt;
using fNbt.Tags;
using NUnit.Framework;

namespace Coflnet.Sky.Auctions.Archive;

/// <summary>One test per rule of <see cref="ScyllaAuditEquivalence.Rules"/> (E01..E15) plus the table itself.</summary>
public class ScyllaAuditEquivalenceTests
{
    private static readonly DateTime End = new(2020, 9, 10, 12, 0, 0, DateTimeKind.Utc);
    private const string Tag = "HYPERION";

    [SetUp]
    public void SetUp() => PlayerOptOut.ResetMemory();

    internal static string Uuid(long uid) => uid.ToString("x16").PadRight(32, '0');
    // real player uuids do not start with the zeros of the anonymous / filler uuids
    internal static string Hex(long v) => "a1b2c3d4e5f60718" + v.ToString("x16");

    internal static byte[] Nbt(long uid, params NbtTag[] more)
    {
        var root = new NbtCompound("i") { new NbtString("uid", uid.ToString("x12")) };
        foreach (var tag in more)
            root.Add(tag);
        return NBT.Bytes(root);
    }

    /// <summary>A MariaDB style archive row (no extras).</summary>
    internal static SaveAuction Archived(long uid = 1, long bid = 0, DateTime? end = null, string tag = Tag)
    {
        var uuid = Uuid(uid);
        return new SaveAuction
        {
            Uuid = uuid,
            UId = AuctionService.Instance.GetId(uuid),
            Tag = tag,
            ItemName = "Item " + uid,
            End = end ?? End,
            Start = (end ?? End).AddDays(-1),
            AuctioneerId = Hex(1000 + uid),
            ProfileId = Hex(1000 + uid),
            StartingBid = 10,
            HighestBidAmount = bid,
            Count = 1,
            Tier = Tier.LEGENDARY,
            Category = Category.WEAPON,
            ItemCreatedAt = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            NbtData = new NbtData { data = Nbt(uid) },
            Bids = bid > 0 ? new List<SaveBids> { new() { Bidder = Hex(5000 + uid), ProfileId = Hex(5000 + uid), Amount = bid, Timestamp = (end ?? End).AddHours(-1), AuctionId = uuid } } : new List<SaveBids>(),
            Enchantments = new List<Enchantment> { new(Enchantment.EnchantmentType.sharpness, 5) },
            NBTLookup = new List<NBTLookup>(),
        };
    }

    /// <summary>The raw Scylla row the live writer stored for the same auction.</summary>
    internal static ScyllaAuction Stored(SaveAuction archived) => ScyllaService.ToCassandra(archived);

    private static ScyllaAuditComparison Compare(ScyllaAuction s, SaveAuction r, ScyllaAuctionExtras x = null, bool better = false)
        => ScyllaAuditEquivalence.Compare(s, r, x, better);

    [Test]
    public void TheTableListsEveryRuleOnce()
    {
        Assert.That(ScyllaAuditEquivalence.Rules.Select(r => r.Id), Is.EqualTo(Enumerable.Range(1, 15).Select(i => $"E{i:00}")));
        Assert.That(ScyllaAuditEquivalence.Rules.All(r => r.Description.Length > 20), Is.True);
    }

    [Test]
    public void FaithfulStoredRow_IsEquivalent_NothingDiffers()
    {
        var r = Archived(1, bid: 500);
        r.Coop = new List<string>();
        var result = Compare(Stored(r), r);
        Assert.That(result.Differing, Is.Empty);
    }

    [Test]
    public void E01_Uuid_DashesAndCaseDoNotMatter_ADifferentUuidDiffers()
    {
        var r = Archived(1);
        var s = Stored(r);
        r.Uuid = Guid.Parse(s.Uuid.ToString("N")).ToString("D").ToUpperInvariant();
        Assert.That(Compare(s, r).Differing, Does.Not.Contain("uuid"));
        s.Uuid = Guid.Parse(Hex(99));
        Assert.That(Compare(s, r).Differing, Does.Contain("uuid"));
    }

    [Test]
    public void E02_Tag_UnknownPartitionIsRetagged_OtherTagDiffers()
    {
        var r = Archived(1);
        var s = Stored(r);
        s.Tag = "unknown";
        Assert.That(Compare(s, r).Differing, Does.Not.Contain("tag"));
        s.Tag = "TERMINATOR";
        Assert.That(Compare(s, r).Differing, Does.Contain("tag"));
    }

    [Test]
    public void E03_AuctionUid_ZeroUidIsDerivedFromTheUuid()
    {
        var r = Archived(1);
        var s = Stored(r);
        s.AuctionUid = 0; // old rows: derived from the uuid
        Assert.That(Compare(s, r).Differing, Does.Not.Contain("auctionuid"));
        s.AuctionUid = r.UId + 1;
        Assert.That(Compare(s, r).Differing, Does.Contain("auctionuid"));
    }

    [Test]
    public void E04_Strings_UnparseableTierIsUnknown_SupremeIsDivine_EmptyScyllaItemNameIsNotADifference()
    {
        var r = Archived(1);
        r.Tier = Tier.DIVINE;
        var s = Stored(r);
        s.Tier = "SUPREME";
        Assert.That(Compare(s, r).Differing, Does.Not.Contain("tier"));
        r.Tier = Tier.UNKNOWN;
        s.Tier = "UNKNOWN";
        Assert.That(Compare(s, r).Differing, Does.Not.Contain("tier"));
        s.Tier = "LEGENDARY";
        Assert.That(Compare(s, r).Differing, Does.Contain("tier"), "a real tier R2 lost");
        r = Archived(1);
        s = Stored(r);
        s.ItemName = null;
        var result = Compare(s, r);
        Assert.That(result.Differing, Does.Not.Contain("itemname"));
        Assert.That(result.R2Only, Does.Contain("itemname"));
        s.ItemName = "other";
        Assert.That(Compare(s, r).Differing, Does.Contain("itemname"));
    }

    [Test]
    public void E05_Numbers_ZeroIsEmpty_BetterR2RowMayHaveAHigherBidAndASoldFlag()
    {
        var r = Archived(1, bid: 500);
        var s = Stored(r);
        s.StartingBid = 0;
        Assert.That(Compare(s, r).Differing, Is.Empty);
        s.StartingBid = 99;
        Assert.That(Compare(s, r).Differing, Does.Contain("startingbid"));
        s = Stored(r);
        s.HighestBidAmount = 300;
        Assert.That(Compare(s, r).Differing, Does.Contain("highestbidamount"));
        Assert.That(Compare(s, r, better: true).Differing, Does.Not.Contain("highestbidamount"), "R2 holds the higher bid");
        s.HighestBidAmount = 900;
        Assert.That(Compare(s, r, better: true).Differing, Does.Contain("highestbidamount"), "Scylla has the higher bid R2 lacks");
        s = Stored(r);
        s.IsSold = false;
        Assert.That(Compare(s, r).Differing, Is.Empty, "R2 sold, Scylla not: R2 is richer");
        s.IsSold = true;
        var unsold = Archived(1);
        Assert.That(Compare(s, unsold).Differing, Does.Contain("issold"));
    }

    [Test]
    public void E06_Dates_MillisecondsAreCompared_DefaultIsEmpty_EndMayDifferWhenR2IsBetter()
    {
        var r = Archived(1);
        var s = Stored(r);
        s.End = r.End.AddTicks(5000); // sub millisecond is cut
        Assert.That(Compare(s, r).Differing, Is.Empty);
        s.End = r.End.AddSeconds(1);
        Assert.That(Compare(s, r).Differing, Does.Contain("end"));
        Assert.That(Compare(s, r, better: true).Differing, Does.Contain("end"), "R2 is only better by End: same sold state and bid, a different End is a difference");
        var sold = Archived(1, bid: 500);
        var snapshot = Stored(Archived(1));
        snapshot.End = sold.End.AddSeconds(1);
        Assert.That(Compare(snapshot, sold, better: true).Differing, Does.Not.Contain("end"), "R2 strictly dominates (sold against unsold)");
        s = Stored(r);
        s.Start = default;
        Assert.That(Compare(s, r).Differing, Is.Empty);
        s.Start = r.Start.AddHours(1);
        Assert.That(Compare(s, r).Differing, Does.Contain("start"));
        s = Stored(r);
        s.ItemCreatedAt = r.ItemCreatedAt.AddDays(1);
        Assert.That(Compare(s, r).Differing, Does.Contain("itemcreatedat"));
    }

    [Test]
    public void E07_Identities_AnonymousAreEqual_OptedOutScyllaIdentityIsIgnorable_ProfileDefaultsToTheAuctioneer()
    {
        var r = Archived(1);
        r.AuctioneerId = PlayerOptOut.AnonymousUuidPrefix + "0A";
        r.ProfileId = null;
        var s = Stored(r);
        s.Auctioneer = Guid.Parse(PlayerOptOut.AnonymousUuidPrefix + "F3");
        s.ProfileId = s.Auctioneer;
        Assert.That(Compare(s, r).Differing, Is.Empty, "two anonymous uuids differ only in the random suffix");
        var optedOut = Guid.Parse(PlayerOptOut.LegacyPlayerUuids[0]);
        s.Auctioneer = optedOut;
        Assert.That(Compare(s, r).Differing, Is.Empty, "erased from R2, still in Scylla");
        s.Auctioneer = Guid.Parse(Hex(777));
        Assert.That(Compare(s, r).Differing, Does.Contain("auctioneer"));
        var plain = Archived(2);
        plain.ProfileId = null;
        var stored = Stored(plain);
        Assert.That(stored.ProfileId.ToString("N"), Is.EqualTo(plain.AuctioneerId), "ToCassandra defaults the profile to the auctioneer");
        Assert.That(Compare(stored, plain).Differing, Is.Empty);
    }

    [Test]
    public void E07_Coop_MembersMustBeInR2_OptedOutMembersAreIgnorable()
    {
        var r = Archived(1);
        r.CoopMembers = new List<UuId> { new(Hex(31)) };
        var s = Stored(r);
        Assert.That(Compare(s, r).Differing, Is.Empty);
        s.Coop = new List<string> { Hex(31), Hex(32) };
        Assert.That(Compare(s, r).Differing, Does.Contain("coop"));
        s.Coop = new List<string> { Hex(31), PlayerOptOut.LegacyPlayerUuids[0] };
        Assert.That(Compare(s, r).Differing, Is.Empty);
    }

    [Test]
    public void E08_HighestBidder_FillerIsEmpty_RealBidderMustBeInR2()
    {
        var unsold = Archived(1);
        var s = Stored(unsold);
        Assert.That(s.HighestBidder.ToString("N"), Does.StartWith(ScyllaAuditEquivalence.FillerPrefix));
        Assert.That(Compare(s, unsold).Differing, Is.Empty, "random 0000..xxxx filler of an unsold auction");
        var sold = Archived(2, bid: 500);
        var stored = Stored(sold);
        Assert.That(Compare(stored, sold).Differing, Is.Empty);
        stored.HighestBidder = Guid.Parse(Hex(6000));
        Assert.That(Compare(stored, sold).Differing, Does.Contain("highestbidder"));
        Assert.That(Compare(stored, sold, better: true).Differing, Does.Contain("highestbidder"), "not dominated: same bid");
        stored.HighestBidAmount = 200;
        sold.Bids.Add(new SaveBids { Bidder = Hex(6000), ProfileId = Hex(6000), Amount = 200, Timestamp = End, AuctionId = sold.Uuid });
        Assert.That(Compare(stored, sold, better: true).Differing, Does.Not.Contain("highestbidder"), "an earlier highest bidder of the better row");
    }

    [Test]
    public void E09_ItemUid_SmallRandomWithoutNbtUidIsFiller_ItemIdAndColorAreCompared()
    {
        var r = Archived(1);
        r.NbtData = null;
        var s = Stored(r);
        Assert.That(s.ItemUid, Is.LessThanOrEqualTo(ScyllaService.MaxRandomItemUid));
        s.ItemUid = 4242;
        Assert.That(Compare(s, r).Differing, Does.Not.Contain("itemuid"), "random filler < 10000");
        var withUid = Archived(2);
        var stored = Stored(withUid);
        Assert.That(Compare(stored, withUid).Differing, Is.Empty);
        stored.ItemUid += 1;
        Assert.That(Compare(stored, withUid).Differing, Does.Contain("itemuid"));
        stored = Stored(withUid);
        stored.ItemId = Guid.Parse(Hex(55));
        Assert.That(Compare(stored, withUid).Differing, Does.Contain("itemid"));
        stored = Stored(withUid);
        stored.Color = 12345;
        Assert.That(Compare(stored, withUid).Differing, Does.Contain("color"));
    }

    [Test]
    public void E10_CorruptSerialisedBids_AreADifferenceOfTheirOwn_NotAFailure()
    {
        var r = Archived(1, bid: 500);
        var s = Stored(r);
        s.SerialisedBids = new byte[] { 0xc1, 0xff, 0x00, 0x13, 0x37 };

        var result = Compare(s, r);

        Assert.That(result.Differing, Does.Contain("serialisedbids_unreadable"));
        Assert.That(result.Differing, Does.Not.Contain("serialisedbids"));
    }

    [Test]
    public void E10_Bids_EveryScyllaBidMustBeInR2_BetterRowMayDifferInTimestamps()
    {
        var r = Archived(1, bid: 500);
        var s = Stored(r);
        Assert.That(Compare(s, r).Differing, Is.Empty);
        var extra = new List<CassandraBid>(s.Bids) { new() { AuctionUuid = s.Uuid, BidderUuid = Guid.Parse(Hex(8000)), ProfileId = Guid.Parse(Hex(8000)), Amount = 400, Timestamp = End } };
        s.SerialisedBids = MessagePack.MessagePackSerializer.Serialize<IEnumerable<CassandraBid>>(extra);
        Assert.That(Compare(s, r).Differing, Does.Contain("serialisedbids"), "a bid R2 lacks");
        s = Stored(r);
        var shifted = s.Bids;
        shifted[0].Timestamp = shifted[0].Timestamp.AddMinutes(1);
        s.SerialisedBids = MessagePack.MessagePackSerializer.Serialize<IEnumerable<CassandraBid>>(shifted);
        Assert.That(Compare(s, r).Differing, Does.Contain("serialisedbids"));
        Assert.That(Compare(s, r, better: true).Differing, Does.Contain("serialisedbids"), "bid timestamps are compared even when R2 is better");
        s = Stored(r);
        s.SerialisedBids = null;
        var result = Compare(s, r);
        Assert.That(result.Differing, Is.Empty, "no bids in Scylla loses nothing");
        Assert.That(result.R2Only, Does.Contain("serialisedbids"));
        var unknownProfile = Archived(3, bid: 100);
        unknownProfile.Bids[0].ProfileId = "unknown";
        Assert.That(Compare(Stored(unknownProfile), unknownProfile).Differing, Is.Empty, "profile 'unknown' maps to ...0001");
    }

    [Test]
    public void E11_Enchantments_UnknownNumberedNamesAreFiller_OthersMustMatch()
    {
        var r = Archived(1);
        r.Enchantments.Add(new Enchantment(Enchantment.EnchantmentType.unknown, 3));
        var s = Stored(r);
        s.Enchantments["unknown7"] = 3;
        Assert.That(Compare(s, r).Differing, Is.Empty);
        s.Enchantments["sharpness"] = 6;
        Assert.That(Compare(s, r).Differing, Does.Contain("enchantments"));
        s = Stored(r);
        s.Enchantments["cleave"] = 1;
        Assert.That(Compare(s, r).Differing, Does.Contain("enchantments"));
        var extras = new ScyllaAuctionExtras { UnknownEnchantments = new Dictionary<string, int> { ["cleave"] = 1 } };
        Assert.That(Compare(s, r, extras).Differing, Does.Not.Contain("enchantments"), "kept in the extras");
    }

    [Test]
    public void E12_ItemBytes_InjectedAnvilUsesAndModifierAreIgnored_AnythingElseDiffers()
    {
        var r = Archived(1);
        r.AnvilUses = 3;
        r.Reforge = ItemReferences.Reforge.chomp;
        var s = Stored(r);
        Assert.That(s.ItemBytes, Is.Not.EqualTo(r.NbtData.data), "ToCassandra injected anvil_uses and modifier");
        Assert.That(Compare(s, r).Differing, Is.Empty);
        s.ItemBytes = Nbt(1, new NbtString("extra", "x"));
        Assert.That(Compare(s, r).Differing, Does.Contain("itembytes"));
        s.ItemBytes = null;
        Assert.That(Compare(s, r).Differing, Is.Empty);
        s.ItemBytes = new byte[] { 1, 2, 3 };
        r.NbtData = new NbtData { data = new byte[] { 1, 2, 3 } };
        Assert.That(Compare(s, r).Differing, Does.Not.Contain("itembytes"), "unparseable bytes are compared raw");
    }

    [Test]
    public void E04_UnparseableNonEmptyTierOrCategory_IsADifference_CaseInsensitiveNamesAreNot()
    {
        var r = Archived(1);
        r.Tier = Tier.UNKNOWN;
        r.Category = Category.UNKNOWN;
        var s = Stored(r);
        s.Tier = "NOT_A_TIER";
        s.Category = "NOT_A_CATEGORY";
        Assert.That(Compare(s, r).Differing, Is.EquivalentTo(new[] { "tier", "category" }), "R2 stores UNKNOWN, the text would be lost");
        s.Tier = "unknown";
        s.Category = "";
        Assert.That(Compare(s, r).Differing, Is.Empty);
        r = Archived(1);
        s = Stored(r);
        s.Tier = "legendary";
        s.Category = "weapon";
        Assert.That(Compare(s, r).Differing, Is.Empty, "case-insensitive parse, equal to the R2 enum");
        s.Tier = "mythic";
        Assert.That(Compare(s, r).Differing, Does.Contain("tier"));
    }

    [Test]
    public void E05_SameBidAndSoldStateWithAnotherEnd_IsADifferenceEvenIfR2IsBetter()
    {
        var r = Archived(1, bid: 500);
        var s = Stored(r);
        s.IsSold = false;
        s.End = r.End.AddMinutes(1);
        var result = Compare(s, r, better: true);
        Assert.That(result.Differing, Does.Contain("end"));
        Assert.That(Compare(s, r, better: false).Differing, Does.Contain("end"));
        s.End = r.End;
        Assert.That(Compare(s, r, better: true).Differing, Is.Empty, "same end and bid: only the raw issold flag differs and R2 is richer");
    }

    [Test]
    public void E10_BidTimestamps_UnspecifiedKindIsUtc_NoSystematicOffset()
    {
        var r = Archived(1, bid: 500);
        var s = Stored(r);
        var bids = s.Bids;
        bids[0].Timestamp = DateTime.SpecifyKind(r.Bids[0].Timestamp, DateTimeKind.Unspecified).AddTicks(7);
        s.SerialisedBids = MessagePack.MessagePackSerializer.Serialize<IEnumerable<CassandraBid>>(bids);
        Assert.That(Compare(s, r).Differing, Is.Empty, "unspecified kind is UTC, sub-millisecond ticks are cut");
        bids[0].Timestamp = bids[0].Timestamp.AddMilliseconds(1);
        s.SerialisedBids = MessagePack.MessagePackSerializer.Serialize<IEnumerable<CassandraBid>>(bids);
        Assert.That(Compare(s, r).Differing, Does.Contain("serialisedbids"), "one millisecond is a difference");
    }

    [Test]
    public void E12_ItemBytes_AnvilUsesAndModifierAreOnlyIgnoredWhenTheyEqualWhatR2ReInjects()
    {
        var r = Archived(1);
        r.AnvilUses = 3;
        r.Reforge = ItemReferences.Reforge.chomp;
        var s = Stored(r);
        Assert.That(Compare(s, r).Differing, Is.Empty);
        s.ItemBytes = Nbt(1, new NbtInt("anvil_uses", 4), new NbtString("modifier", "chomp"));
        Assert.That(Compare(s, r).Differing, Does.Contain("itembytes"), "another anvil_uses than R2 knows is data");
        s.ItemBytes = Nbt(1, new NbtInt("anvil_uses", 3), new NbtString("modifier", "sharp"));
        Assert.That(Compare(s, r).Differing, Does.Contain("itembytes"), "another modifier than R2 knows is data");
        s.ItemBytes = Nbt(1, new NbtInt("anvil_uses", 3), new NbtString("modifier", "chomp"));
        Assert.That(Compare(s, r).Differing, Is.Empty);
        var plain = Archived(2);
        var stored = Stored(plain);
        stored.ItemBytes = Nbt(2, new NbtInt("anvil_uses", 1));
        Assert.That(Compare(stored, plain).Differing, Does.Contain("itembytes"), "R2 knows no anvil use at all");
    }

    [Test]
    public void E13_NbtLookup_ScyllaKeysMustHaveTheSameValue()
    {
        var r = Archived(1);
        var s = Stored(r);
        Assert.That(Compare(s, r).Differing, Is.Empty);
        s.NbtLookup = new Dictionary<string, string>(s.NbtLookup) { ["extra_key"] = "v" };
        Assert.That(Compare(s, r).Differing, Does.Contain("nbtlookup"));
        var extras = new ScyllaAuctionExtras { NbtLookupStrings = s.NbtLookup };
        Assert.That(Compare(s, r, extras).Differing, Does.Not.Contain("nbtlookup"), "the extras map is what Scylla had");
    }

    [Test]
    public void E14_ScyllaOnlyNames_AreMissingFromMariaDbRows()
    {
        var r = Archived(1, bid: 500);
        var s = Stored(r);
        s.ProfileName = "Mango";
        s.CoopName = "Team";
        s.HighestBidderName = "Alice";
        var result = Compare(s, r);
        Assert.That(result.Differing, Is.EquivalentTo(new[] { "profilename", "coopname", "highestbiddername" }));
        var extras = new ScyllaAuctionExtras { ProfileName = "Mango", CoopName = "Team", HighestBidderName = "Alice", Color = s.Color };
        Assert.That(Compare(s, r, extras).Differing, Is.Empty);
        extras.HighestBidderName = "Bob";
        Assert.That(Compare(s, r, extras).Differing, Does.Contain("highestbiddername"));
        // an older highest bidder's name on a worse snapshot is not comparable
        s.HighestBidder = Guid.Parse(Hex(31337));
        s.HighestBidAmount = 100;
        r.Bids.Add(new SaveBids { Bidder = Hex(31337), ProfileId = Hex(31337), Amount = 100, Timestamp = End, AuctionId = r.Uuid });
        Assert.That(Compare(s, r, extras, better: true).Differing, Does.Not.Contain("highestbiddername"));
    }

    [Test]
    public void E15_ColumnsABlockCannotCarry_AreAlwaysADifference_EmptyStringIsNot()
    {
        var r = Archived(1);
        var s = Stored(r);
        s.ItemLore = "lore";
        s.Extra = "extra";
        s.ExtraAttributesJson = "{}";
        Assert.That(Compare(s, r).Differing, Is.EquivalentTo(new[] { "itemlore", "extra", "extraattributesjson" }));
        s.ItemLore = "";
        s.Extra = "";
        s.ExtraAttributesJson = "";
        Assert.That(Compare(s, r).Differing, Is.Empty);
    }

    [Test]
    public void RowsToCassandraCannotMap_AreReportedNotThrown()
    {
        var r = Archived(1);
        r.AuctioneerId = "not-a-guid";
        Assert.That(Compare(Stored(Archived(1)), r).Differing, Is.EqualTo(new[] { "to_cassandra_failed" }));
    }
}
