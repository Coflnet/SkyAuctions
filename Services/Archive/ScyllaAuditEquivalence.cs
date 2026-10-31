using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Coflnet.Sky.Core;
using Coflnet.Sky.Core.Archive;
using fNbt;
using fNbt.Tags;

namespace Coflnet.Sky.Auctions.Archive;

/// <summary>The outcome of comparing one raw Scylla row with its R2 archive row.</summary>
/// <param name="Differing">Columns where Scylla has a non-empty value that the R2 row lacks or holds differently (data that would be lost).</param>
/// <param name="R2Only">Columns where Scylla is empty and the R2 row has a value (R2 is richer; nothing is lost).</param>
public sealed record ScyllaAuditComparison(IReadOnlyList<string> Differing, IReadOnlyList<string> R2Only);

/// <summary>
/// The documented equivalence between a RAW <c>weekly_auctions_2</c> row and the R2 archive row (<see cref="SaveAuction"/> plus
/// <see cref="ScyllaAuctionExtras"/>), used by <see cref="ScyllaArchiveAudit"/>. The R2 row is mapped with
/// <see cref="ScyllaService.ToCassandra(SaveAuction)"/> (what the live writer would have stored) and the Scylla columns are compared with it
/// column by column under the rules in <see cref="Rules"/>. A column of the Scylla row is "empty" when it is null, an empty string/collection,
/// <c>0</c>, <c>false</c>, an empty guid or a default date; an empty Scylla column never counts as a difference (nothing would be lost), it
/// is reported in <see cref="ScyllaAuditComparison.R2Only"/> when the R2 row has a value. Everything non-empty has to be present and equal in the
/// R2 row, otherwise the column is in <see cref="ScyllaAuditComparison.Differing"/>.
/// </summary>
public static class ScyllaAuditEquivalence
{
    /// <summary>One rule of the equivalence table.</summary>
    public sealed record Rule(string Id, string Column, string Description);

    /// <summary>Prefix of the 28 leading zeros that both the random <c>highestbidder</c> filler and the anonymous uuids (<see cref="PlayerOptOut.AnonymousUuidPrefix"/>) share.</summary>
    public const string FillerPrefix = "0000000000000000000000000000";

    private static readonly Regex UnknownEnchantmentName = new("^unknown[0-9]+$", RegexOptions.Compiled);

    /// <summary>The equivalence table as data; <c>ScyllaAuditEquivalenceTests</c> has one test per rule id.</summary>
    public static readonly IReadOnlyList<Rule> Rules = new List<Rule>
    {
        new("E01", "uuid", "equal after removing dashes and lower-casing; an empty guid is empty"),
        new("E02", "tag", "equal, or the Scylla partition tag is 'unknown' (the sweep re-tags those rows by item name, the R2 tag is the resolved one)"),
        new("E03", "auctionuid", "ScyllaArchiveRows.UidOf(row) (auctionuid, or the uid derived from the uuid when 0) equals the R2 uid"),
        new("E04", "itemname/category/tier", "strings equal; category/tier are compared as their parsed enum (case-insensitive, SUPREME = DIVINE); a Scylla UNKNOWN or empty value is empty, a non-empty value that does not parse is a difference (R2 stores UNKNOWN, the text would be lost)"),
        new("E05", "startingbid/highestbidamount/count/bin/issold", "numbers equal, 0/false is empty. Only when the R2 row is strictly dominating (IsBetter and a strictly higher highest bid, which includes sold against unsold) are issold, end, highestbidder and highestbiddername of the Scylla row superseded; same sold state and same bid with another End is a difference"),
        new("E06", "end/start/itemcreatedat", "UTC whole milliseconds equal; a default date is empty; end may only differ when the R2 row strictly dominates (see E05)"),
        new("E07", "auctioneer/profileid/highestbidder/bidders/coop", "equal as 32 char hex; two anonymous uuids (PlayerOptOut.IsAnonymousUuid) are equal whatever their random suffix; a Scylla identity of an opted-out player is ignorable (it was erased from R2); profileid defaults to the auctioneer when R2 has none"),
        new("E08", "highestbidder", "the random filler 00000000-0000-0000-0000-00000000xxxx (unsold auctions) and an empty guid are empty; otherwise equal to the R2 highest bidder (a better R2 row: any bidder of its bids)"),
        new("E09", "itemuid/itemid/color", "itemuid below 10000 with no 'uid' in the item nbt is random filler and ignorable; otherwise equal to the extras / the value ToCassandra derives from the item nbt; itemid and color likewise"),
        new("E10", "serialisedbids", "every Scylla bid is in the R2 bids with equal bidder and amount and timestamp to the millisecond (always, also when the R2 row dominates; whole-millisecond UTC on both sides, no systematic offset exists in the mapping); the bid profile may be the bidder itself or 'unknown' (…0001); bids only R2 has are fine"),
        new("E11", "enchantments", "name/level equal; 'unknown{n}' names are random filler on both sides and ignored; names that are no enchantment type come from the R2 extras"),
        new("E12", "itembytes", "equal after both sides drop the root tags anvil_uses / modifier whose value equals what ToCassandra re-injects from the R2 AnvilUses / Reforge (any other value is compared) and are re-serialised; an empty Scylla blob is empty"),
        new("E13", "nbtlookup", "every Scylla key has the same value in the R2 extras map or, without one, in the flatten of the R2 item bytes"),
        new("E14", "profilename/coopname/highestbiddername", "Scylla-only names: equal to the R2 extras; a null extras (MariaDB source) lacks them. highestbiddername of an older bidder is ignorable when the R2 row is better"),
        new("E15", "itemlore/extra/extraattributesjson", "columns the block can not carry: a non-empty value is always a difference"),
    };

    /// <summary>
    /// Compare a raw Scylla row with its R2 row. <paramref name="r2Better"/> is <see cref="ScyllaArchiveRows.IsBetter(Fingerprint, Fingerprint)"/> of the R2 row
    /// over this Scylla row (a dedupe loser or older snapshot of the auction).
    /// </summary>
    public static ScyllaAuditComparison Compare(ScyllaAuction s, SaveAuction r, ScyllaAuctionExtras x, bool r2Better)
    {
        ScyllaAuction e;
        try
        {
            e = Expected(r, x);
        }
        catch (Exception)
        {
            return new ScyllaAuditComparison(new[] { "to_cassandra_failed" }, new List<string>());
        }
        return new RowComparer(s, r, e, r2Better).Run();
    }

    /// <summary>The state of one comparison: one method per rule (E01-E15), run in table order.</summary>
    private sealed class RowComparer
    {
        private readonly ScyllaAuction s;
        private readonly SaveAuction r;
        private readonly ScyllaAuction e;
        private readonly bool dominated;
        private readonly List<string> diff = new();
        private readonly List<string> r2Only = new();

        public RowComparer(ScyllaAuction s, SaveAuction r, ScyllaAuction e, bool r2Better)
        {
            this.s = s;
            this.r = r;
            this.e = e;
            dominated = IsDominated(s, e, r2Better);
        }

        public ScyllaAuditComparison Run()
        {
            Uuid();
            Tag();
            AuctionUid();
            ItemNameCategoryTier();
            NumbersAndDates();
            Identities();
            HighestBidder();
            Coop();
            ItemUid();
            ItemIdAndColor();
            Bids();
            Enchantments();
            ItemBytes();
            NbtLookup();
            Names();
            UnsupportedColumns();
            return new ScyllaAuditComparison(diff.Distinct().ToList(), r2Only.Distinct().ToList());
        }

        private void Str(string column, string sv, string ev)
        {
            if (string.IsNullOrEmpty(sv))
            {
                if (!string.IsNullOrEmpty(ev))
                    r2Only.Add(column);
                return;
            }
            if (!string.Equals(sv, ev, StringComparison.Ordinal))
                diff.Add(column);
        }

        private void Num(string column, long sv, long ev)
        {
            if (sv == 0)
            {
                if (ev != 0)
                    r2Only.Add(column);
                return;
            }
            if (sv != ev)
                diff.Add(column);
        }

        private void Date(string column, DateTime sv, DateTime ev)
        {
            if (IsDefault(sv))
            {
                if (!IsDefault(ev))
                    r2Only.Add(column);
                return;
            }
            if (ScyllaArchiveRows.TruncateToMs(ScyllaArchiveRows.ToUtc(sv)) != ScyllaArchiveRows.TruncateToMs(ScyllaArchiveRows.ToUtc(ev)))
                diff.Add(column);
        }

        private void Identity(string column, Guid sv, string ev)
        {
            if (sv == Guid.Empty)
            {
                if (!string.IsNullOrEmpty(ev) && Guid.TryParse(ev, out var parsed) && parsed != Guid.Empty)
                    r2Only.Add(column);
                return;
            }
            if (!IdentityEquivalent(sv.ToString("N"), ev))
                diff.Add(column);
        }

        private void Enumerated<T>(string column, string raw, T ev) where T : struct, Enum
        {
            switch (CompareEnumerated(raw, ev))
            {
                case EnumeratedOutcome.R2Only: r2Only.Add(column); break;
                case EnumeratedOutcome.Differs: diff.Add(column); break;
            }
        }

        /// <summary>E01</summary>
        private void Uuid()
        {
            if (s.Uuid != Guid.Empty && !string.Equals(s.Uuid.ToString("N"), ArchivedAuctionQuery.NormalizeUuid(r.Uuid), StringComparison.Ordinal))
                diff.Add("uuid");
        }

        /// <summary>E02</summary>
        private void Tag()
        {
            if (!string.Equals(s.Tag, r.Tag, StringComparison.Ordinal) && !string.Equals(s.Tag, ScyllaArchiveSweep.UnknownTag, StringComparison.Ordinal))
                diff.Add("tag");
        }

        /// <summary>E03</summary>
        private void AuctionUid()
        {
            if (ScyllaArchiveRows.UidOf(s) != r.UId)
                diff.Add("auctionuid");
        }

        /// <summary>E04</summary>
        private void ItemNameCategoryTier()
        {
            Str("itemname", s.ItemName, e.ItemName);
            Enumerated("category", s.Category, ParseCategory(e.Category));
            Enumerated("tier", TierText(s.Tier), ParseTier(e.Tier));
        }

        /// <summary>E05 / E06</summary>
        private void NumbersAndDates()
        {
            Num("startingbid", s.StartingBid, e.StartingBid);
            Num("count", s.Count, e.Count);
            if (s.Bin && !e.Bin)
                diff.Add("bin");
            else if (!s.Bin && e.Bin)
                r2Only.Add("bin");
            Date("start", s.Start, e.Start);
            Date("itemcreatedat", s.ItemCreatedAt, e.ItemCreatedAt);
            if (dominated)
                // a strictly higher bid (or a sold R2 row against an unsold Scylla row) is what makes R2 better: end and issold of the older snapshot are superseded
                r2Only.Add("highestbidamount");
            else
                BidAmountSoldAndEnd();
        }

        private void BidAmountSoldAndEnd()
        {
            Num("highestbidamount", s.HighestBidAmount, e.HighestBidAmount);
            if (s.IsSold && !e.IsSold)
                diff.Add("issold");
            else if (!s.IsSold && e.IsSold)
                r2Only.Add("issold");
            Date("end", s.End, e.End);
        }

        /// <summary>E07 (auctioneer, profileid)</summary>
        private void Identities()
        {
            Identity("auctioneer", s.Auctioneer, e.Auctioneer.ToString("N"));
            Identity("profileid", s.ProfileId, e.ProfileId.ToString("N"));
        }

        /// <summary>E08</summary>
        private void HighestBidder()
        {
            var sv = s.HighestBidder.ToString("N");
            if (s.HighestBidder == Guid.Empty || IsFiller(sv))
                return;
            if (dominated)
            {
                if (!r.Bids.Any(b => IdentityEquivalent(sv, b.Bidder)))
                    diff.Add("highestbidder");
                return;
            }
            if (!IdentityEquivalent(sv, e.HighestBidder.ToString("N")) || IsFiller(e.HighestBidder.ToString("N")))
                diff.Add("highestbidder");
        }

        /// <summary>E07 (coop members)</summary>
        private void Coop()
        {
            var expected = (e.Coop ?? new List<string>()).Select(Normalise).ToList();
            var missing = (s.Coop ?? new List<string>()).Select(Normalise).Where(c => !string.IsNullOrEmpty(c))
                .Any(c => !PlayerOptOut.IsOptedOut(c) && !expected.Any(m => IdentityEquivalent(c, m)));
            if (missing)
                diff.Add("coop");
            else if (expected.Count > 0 && (s.Coop == null || s.Coop.Count == 0))
                r2Only.Add("coop");
        }

        /// <summary>E09 (itemuid)</summary>
        private void ItemUid()
        {
            var hasUid = e.NbtLookup != null && e.NbtLookup.ContainsKey("uid");
            if (s.ItemUid == 0 || (s.ItemUid <= ScyllaService.MaxRandomItemUid && !hasUid))
                return;
            if (s.ItemUid != e.ItemUid)
                diff.Add("itemuid");
        }

        /// <summary>E09 (itemid, color)</summary>
        private void ItemIdAndColor()
        {
            if (s.ItemId != Guid.Empty && s.ItemId != e.ItemId)
                diff.Add("itemid");
            if (s.Color != null && s.Color != e.Color)
                diff.Add("color");
        }

        /// <summary>E10; a blob that can not be deserialised is a difference of its own (<c>serialisedbids_unreadable</c>), never a failure of the pass.</summary>
        private void Bids()
        {
            if (!TryReadBids(s.SerialisedBids, out var sBids))
            {
                diff.Add("serialisedbids_unreadable");
                return;
            }
            var eBids = e.Bids ?? new List<CassandraBid>();
            if (sBids.Any(sb => !BidIsCovered(sb, eBids)))
            {
                diff.Add("serialisedbids");
                return;
            }
            if (eBids.Count > sBids.Count)
                r2Only.Add("serialisedbids");
        }

        private static bool BidIsCovered(CassandraBid sb, List<CassandraBid> eBids)
        {
            var match = eBids.Any(eb => eb.Amount == sb.Amount && IdentityEquivalent(sb.BidderUuid.ToString("N"), eb.BidderUuid.ToString("N"))
                && SameBidTime(eb.Timestamp, sb.Timestamp)
                && (sb.ProfileId == sb.BidderUuid || sb.ProfileId == Guid.Empty || sb.ProfileId == eb.ProfileId || IdentityEquivalent(sb.ProfileId.ToString("N"), eb.ProfileId.ToString("N"))));
            return match || PlayerOptOut.IsOptedOut(sb.BidderUuid);
        }

        /// <summary>E11</summary>
        private void Enchantments()
        {
            var expected = e.Enchantments ?? new Dictionary<string, int>();
            var scylla = (s.Enchantments ?? new Dictionary<string, int>()).Where(kv => !UnknownEnchantmentName.IsMatch(kv.Key)).ToList();
            if (scylla.Any(kv => !expected.TryGetValue(kv.Key, out var level) || level != kv.Value))
                diff.Add("enchantments");
            else if (expected.Count(kv => !UnknownEnchantmentName.IsMatch(kv.Key)) > scylla.Count)
                r2Only.Add("enchantments");
        }

        /// <summary>E12</summary>
        private void ItemBytes()
        {
            if (s.ItemBytes == null || s.ItemBytes.Length == 0)
            {
                if (e.ItemBytes != null && e.ItemBytes.Length > 0 && r.NbtData?.data is { Length: > 0 })
                    r2Only.Add("itembytes");
                return;
            }
            if (!NormalisedItemBytes(s.ItemBytes, r).AsSpan().SequenceEqual(NormalisedItemBytes(r.NbtData?.data, r)))
                diff.Add("itembytes");
        }

        /// <summary>E13</summary>
        private void NbtLookup()
        {
            var expected = e.NbtLookup ?? new Dictionary<string, string>();
            var scylla = s.NbtLookup ?? new Dictionary<string, string>();
            if (scylla.Any(kv => !expected.TryGetValue(kv.Key, out var v) || !string.Equals(v, kv.Value, StringComparison.Ordinal)))
                diff.Add("nbtlookup");
            else if (expected.Count > scylla.Count)
                r2Only.Add("nbtlookup");
        }

        /// <summary>E14</summary>
        private void Names()
        {
            Str("profilename", s.ProfileName, e.ProfileName);
            Str("coopname", s.CoopName, e.CoopName);
            if (!string.IsNullOrEmpty(s.HighestBidderName) && dominated && !string.Equals(s.HighestBidderName, e.HighestBidderName, StringComparison.Ordinal))
            {
                // the name of an older highest bidder, only the name of the bidder of the better row can be checked
                if (string.Equals(s.HighestBidder.ToString("N"), e.HighestBidder.ToString("N"), StringComparison.Ordinal))
                    diff.Add("highestbiddername");
                return;
            }
            Str("highestbiddername", s.HighestBidderName, e.HighestBidderName);
        }

        /// <summary>E15</summary>
        private void UnsupportedColumns()
        {
            if (!string.IsNullOrEmpty(s.ItemLore)) diff.Add("itemlore");
            if (!string.IsNullOrEmpty(s.Extra)) diff.Add("extra");
            if (!string.IsNullOrEmpty(s.ExtraAttributesJson)) diff.Add("extraattributesjson");
        }
    }

    /// <summary>What the live writer would have stored for the R2 row: <see cref="ScyllaService.ToCassandra(SaveAuction)"/> with the Scylla-only extras laid over it.</summary>
    public static ScyllaAuction Expected(SaveAuction r, ScyllaAuctionExtras x)
    {
        // multiple 'unknown' enchantments make ToCassandra throw (random name collision) and are random filler anyway
        var copy = new SaveAuction(r) { Enchantments = (r.Enchantments ?? new List<Enchantment>()).Where(en => en.Type != Enchantment.EnchantmentType.unknown).ToList() };
        // ToCassandra parses the item bytes: rows without parseable bytes are mapped as rows without item
        if (copy.NbtData?.data is { Length: > 0 })
        {
            try
            {
                _ = copy.NbtData.Root();
            }
            catch (Exception)
            {
                copy.NbtData = null;
            }
        }
        else
        {
            copy.NbtData = null;
        }
        var e = ScyllaService.ToCassandra(copy);
        if (x == null)
            return e;
        e.HighestBidderName = x.HighestBidderName;
        e.ProfileName = x.ProfileName;
        e.CoopName = x.CoopName;
        e.Color = x.Color;
        if (x.ItemUid != 0)
            e.ItemUid = x.ItemUid;
        if (x.ItemId != null && x.ItemId.Length > 0 && Guid.TryParse(ArchiveIds.Decode(x.ItemId), out var itemId))
            e.ItemId = itemId;
        if (x.NbtLookupStrings is { Count: > 0 })
            e.NbtLookup = x.NbtLookupStrings.ToDictionary(kv => kv.Key, kv => kv.Value);
        if (x.UnknownEnchantments != null)
            foreach (var kv in x.UnknownEnchantments)
                e.Enchantments[kv.Key] = kv.Value;
        return e;
    }

    /// <summary>
    /// Item bytes without the root tags <see cref="ScyllaService.ToCassandra(SaveAuction)"/> injects, re-serialised so that two encodings of the same
    /// tags compare equal; the input itself when it can not be parsed. <c>anvil_uses</c> / <c>modifier</c> are only dropped when their value is exactly
    /// what R2 would re-inject (<see cref="SaveAuction.AnvilUses"/> / <see cref="SaveAuction.Reforge"/> of <paramref name="r2"/>), any other value is data and stays.
    /// </summary>
    public static byte[] NormalisedItemBytes(byte[] bytes, SaveAuction r2)
    {
        if (bytes == null || bytes.Length == 0)
            return Array.Empty<byte>();
        try
        {
            var root = new NbtData { data = bytes }.Root();
            RemoveIfInjectedValue(root, "anvil_uses", r2.AnvilUses > 0 ? r2.AnvilUses.ToString() : null);
            RemoveIfInjectedValue(root, "modifier", r2.Reforge != ItemReferences.Reforge.None ? r2.Reforge.ToString() : null);
            return NBT.Bytes(root);
        }
        catch (Exception)
        {
            return bytes;
        }
    }

    private static void RemoveIfInjectedValue(NbtCompound root, string name, string injectedValue)
    {
        if (injectedValue == null || !root.TryGet(name, out var tag))
            return;
        if (string.Equals(TagValue(tag), injectedValue, StringComparison.Ordinal))
            root.Remove(name);
    }

    private static string TagValue(NbtTag tag) => tag switch
    {
        NbtInt i => i.Value.ToString(),
        NbtString text => text.Value,
        _ => null,
    };

    /// <summary>True when R2 holds a strictly higher highest bid (which includes sold against unsold) and the dedupe picked it: only then are end and issold of the Scylla row superseded.</summary>
    public static bool IsDominated(ScyllaAuction s, ScyllaAuction expected, bool r2Better) => r2Better && expected.HighestBidAmount > s.HighestBidAmount;

    private static bool SameBidTime(DateTime r2, DateTime scylla)
        => ScyllaArchiveRows.TruncateToMs(ScyllaArchiveRows.ToUtc(r2)) == ScyllaArchiveRows.TruncateToMs(ScyllaArchiveRows.ToUtc(scylla));

    private enum EnumeratedOutcome { Equal, R2Only, Differs }

    /// <summary>Raw tier/category string against the R2 enum: empty / UNKNOWN is empty, a non-empty value that does not parse (case-insensitive) is data R2 can not hold.</summary>
    private static EnumeratedOutcome CompareEnumerated<T>(string raw, T expected) where T : struct, Enum
    {
        if (string.IsNullOrEmpty(raw) || string.Equals(raw, "UNKNOWN", StringComparison.OrdinalIgnoreCase))
            return expected.ToString() == "UNKNOWN" ? EnumeratedOutcome.Equal : EnumeratedOutcome.R2Only;
        if (!TryParseEnum<T>(raw, out var parsed))
            return EnumeratedOutcome.Differs;
        return EqualityComparer<T>.Default.Equals(parsed, expected) ? EnumeratedOutcome.Equal : EnumeratedOutcome.Differs;
    }

    private static bool TryParseEnum<T>(string raw, out T value) where T : struct, Enum
    {
        value = default;
        var text = raw.Trim();
        return text.Length > 0 && !char.IsDigit(text[0]) && Enum.TryParse(text, true, out value) && Enum.IsDefined(value);
    }

    /// <summary>True when the uuid is the random <c>highestbidder</c> filler of an unsold auction (or an anonymous uuid).</summary>
    public static bool IsFiller(string uuid) => uuid != null && uuid.Replace("-", "").StartsWith(FillerPrefix, StringComparison.Ordinal);

    /// <summary>Equal as hex, or both anonymous uuids, or the Scylla identity belongs to an opted-out player and R2 holds nothing or an anonymous one.</summary>
    public static bool IdentityEquivalent(string scylla, string r2)
    {
        var s = Normalise(scylla);
        var r = Normalise(r2);
        if (string.Equals(s, r, StringComparison.Ordinal))
            return true;
        if (PlayerOptOut.IsAnonymousUuid(s) && PlayerOptOut.IsAnonymousUuid(r))
            return true;
        return PlayerOptOut.IsOptedOut(s) && (string.IsNullOrEmpty(r) || PlayerOptOut.IsAnonymousUuid(r));
    }

    private static string Normalise(string uuid) => uuid?.Replace("-", "").ToLowerInvariant();

    private static bool IsDefault(DateTime value) => value == default || value.Year < 2000;

    private static Category ParseCategory(string value) => !string.IsNullOrEmpty(value) && TryParseEnum<Category>(value, out var parsed) ? parsed : Category.UNKNOWN;

    /// <summary>SUPREME is the old name of DIVINE (like <see cref="ScyllaService.CassandraToOld"/>).</summary>
    private static string TierText(string raw) => raw?.Replace("SUPREME", "DIVINE", StringComparison.OrdinalIgnoreCase);

    private static Tier ParseTier(string value) => !string.IsNullOrEmpty(value) && TryParseEnum<Tier>(TierText(value), out var parsed) ? parsed : Tier.UNKNOWN;

    /// <summary>The bids of a serialised blob (none for an empty one); false when the blob is corrupt.</summary>
    private static bool TryReadBids(byte[] serialised, out List<CassandraBid> bids)
    {
        bids = new List<CassandraBid>();
        if (serialised == null || serialised.Length == 0)
            return true;
        try
        {
            bids = MessagePack.MessagePackSerializer.Deserialize<List<CassandraBid>>(serialised) ?? new List<CassandraBid>();
            return true;
        }
        catch (Exception)
        {
            return false;
        }
    }
}
