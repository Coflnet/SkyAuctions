using System;
using System.Collections.Generic;
using System.Linq;
using Coflnet.Sky.Core;
using Coflnet.Sky.Core.Archive;

namespace Coflnet.Sky.Auctions.Archive
{
    /// <summary>
    /// The fields compared between a phase-1 (MariaDB) row and its Scylla copy. Phase-1 data always wins; differences are only counted and logged.
    /// </summary>
    public readonly record struct Fingerprint(long HighestBidAmount, bool IsSold, long EndTicks, int ItemBytesLength, long StartingBid)
    {
        /// <summary>Metric label of each field.</summary>
        public const string HighestBidAmountField = "highest_bid_amount", IsSoldField = "is_sold", EndField = "end", ItemBytesLengthField = "item_bytes_length", StartingBidField = "starting_bid";

        /// <summary>Row <paramref name="index"/> of an archived block (item bytes length -1 when null).</summary>
        public static Fingerprint Of(ArchivedAuctionBlock block, int index) => new(
            block.HighestBidAmount[index],
            block.IsSold[index],
            block.FirstEndTicks + block.EndDeltasMs[index] * TimeSpan.TicksPerMillisecond,
            block.ItemBytes[index]?.Length ?? -1,
            block.StartingBid[index]);

        /// <summary>A Scylla row as stored (raw <c>issold</c> flag, End in UTC whole milliseconds).</summary>
        public static Fingerprint Of(CassandraAuction row) => new(
            row.HighestBidAmount,
            row.IsSold,
            ScyllaArchiveRows.EndOf(row).Ticks,
            row.ItemBytes?.Length ?? -1,
            row.StartingBid);

        /// <summary>Label names of the fields that differ (empty when equal).</summary>
        public static IReadOnlyList<string> Compare(Fingerprint archived, Fingerprint scylla)
        {
            var result = new List<string>(0);
            if (archived.HighestBidAmount != scylla.HighestBidAmount) result.Add(HighestBidAmountField);
            if (archived.IsSold != scylla.IsSold) result.Add(IsSoldField);
            if (archived.EndTicks != scylla.EndTicks) result.Add(EndField);
            if (archived.ItemBytesLength != scylla.ItemBytesLength) result.Add(ItemBytesLengthField);
            if (archived.StartingBid != scylla.StartingBid) result.Add(StartingBidField);
            return result;
        }
    }

    /// <summary>Pure helpers of the Scylla archive sweep: partition/month arithmetic, dedupe, row mapping and bookkeeping values.</summary>
    public static class ScyllaArchiveRows
    {
        /// <summary>Rows ending before this are the random-timekey garbage of tags with 12-hour buckets (SkyBlock launched 2019-06).</summary>
        public static readonly DateTime BogusEndBefore = new(2019, 6, 1, 0, 0, 0, DateTimeKind.Utc);
        /// <summary>Epoch of <see cref="ScyllaService.GetWeekOrDaysSinceStart"/>.</summary>
        public static readonly DateTime TimeKeyEpoch = new(2019, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        /// <summary>Bid profile id ToCassandra writes for the profile "unknown".</summary>
        public static readonly Guid UnknownBidProfile = Guid.Parse("00000000-0000-0000-0000-000000000001");

        private static readonly byte[] EmptyBids = MessagePack.MessagePackSerializer.Serialize(new List<CassandraBid>());

        /// <summary>UTC; Unspecified (as the driver may return it) is taken as UTC.</summary>
        public static DateTime ToUtc(DateTime value) => value.Kind switch
        {
            DateTimeKind.Utc => value,
            DateTimeKind.Local => value.ToUniversalTime(),
            _ => DateTime.SpecifyKind(value, DateTimeKind.Utc),
        };

        /// <summary>Cut sub-millisecond ticks (archive blocks store milliseconds).</summary>
        public static DateTime TruncateToMs(DateTime value) => new(value.Ticks - value.Ticks % TimeSpan.TicksPerMillisecond, value.Kind);

        /// <summary>End of a Scylla row as archived: UTC, whole milliseconds.</summary>
        public static DateTime EndOf(CassandraAuction row) => TruncateToMs(ToUtc(row.End));

        /// <summary>Auction uid of a Scylla row (<c>auctionuid</c>, or derived from the uuid like <see cref="ScyllaService.CassandraToOld"/> when 0).</summary>
        public static long UidOf(ScyllaAuction row) => row.AuctionUid != 0 ? row.AuctionUid : AuctionService.Instance.GetId(row.Uuid.ToString("N"));

        /// <summary>Length of one timekey bucket of a tag, taken from <see cref="ScyllaService.GetWeekOrDaysSinceStart"/> itself (7 days, or 12 hours for ENCHANTED_BOOK/unknown).</summary>
        public static TimeSpan BucketLength(string tag)
            => ScyllaService.GetWeekOrDaysSinceStart(tag, TimeKeyEpoch.AddDays(7)) == 1 ? TimeSpan.FromDays(7) : TimeSpan.FromHours(12);

        /// <summary>Inclusive range of timekeys whose bucket overlaps <paramref name="month"/> (7-day tags: at most 6 keys, 12-hour tags: 56-62).</summary>
        public static (short First, short Last) TimeKeysForMonth(string tag, int month)
        {
            var (start, end) = ArchiveMonths.MonthRange(month);
            return (ScyllaService.GetWeekOrDaysSinceStart(tag, start), ScyllaService.GetWeekOrDaysSinceStart(tag, end.AddTicks(-1)));
        }

        /// <summary>UTC range [start, end) of the rows a well-formed partition holds.</summary>
        public static (DateTime Start, DateTime End) PartitionRange(string tag, short timeKey)
        {
            var length = BucketLength(tag);
            var start = TimeKeyEpoch + length * timeKey;
            return (start, start + length);
        }

        /// <summary>True if the partition can hold rows that end in a month of [<paramref name="oldestMonth"/>, <paramref name="newestMonth"/>].</summary>
        public static bool OverlapsMonths(string tag, short timeKey, int oldestMonth, int newestMonth)
        {
            var (start, end) = PartitionRange(tag, timeKey);
            return start < ArchiveMonths.MonthRange(newestMonth).end && end > ArchiveMonths.MonthRange(oldestMonth).start;
        }

        /// <summary>Dedupe rule for two Scylla rows of the same auction uid: sold wins, then the higher bid, then the later End.</summary>
        public static bool IsBetter(CassandraAuction candidate, CassandraAuction current)
            => current == null || IsBetter(Fingerprint.Of(candidate), Fingerprint.Of(current));

        /// <summary><see cref="IsBetter(CassandraAuction, CassandraAuction)"/> on the fingerprints (raw <c>issold</c>, highest bid, End); equal rows keep the first.</summary>
        public static bool IsBetter(Fingerprint candidate, Fingerprint current)
        {
            if (candidate.IsSold != current.IsSold)
                return candidate.IsSold;
            if (candidate.HighestBidAmount != current.HighestBidAmount)
                return candidate.HighestBidAmount > current.HighestBidAmount;
            return candidate.EndTicks > current.EndTicks;
        }

        /// <summary>One row per auction uid (<see cref="IsBetter(CassandraAuction, CassandraAuction)"/>), in order of the first appearance of each uid.</summary>
        public static List<ScyllaAuction> DedupeScyllaRows(IEnumerable<ScyllaAuction> rows)
        {
            var order = new List<long>();
            var best = new Dictionary<long, ScyllaAuction>();
            foreach (var row in rows)
            {
                var uid = UidOf(row);
                if (!best.TryGetValue(uid, out var current))
                    order.Add(uid);
                if (IsBetter(row, current))
                    best[uid] = row;
            }
            return order.Select(uid => best[uid]).ToList();
        }

        /// <summary>
        /// Map a raw Scylla row (never anonymised through <see cref="ScyllaService.ReadRow"/>) to the archive row and the columns
        /// <see cref="SaveAuction"/> can not carry. Builds on <see cref="ScyllaService.CassandraToOld"/> and replaces what it loses:
        /// tag (partition tag), uid, End/Start as UTC milliseconds, raw item bytes (null stays null), profile id (Guid.Empty is null),
        /// bid profile "unknown", enchantments that are no <see cref="Enchantment.EnchantmentType"/> (kept in the extras instead of
        /// collapsing to <c>unknown</c>), rows without serialised bids. The string-keyed nbt lookup goes to the extras; <see cref="SaveAuction.NBTLookup"/> stays empty.
        /// </summary>
        /// <param name="row">Scylla row; a null <c>serialisedbids</c> is set to an empty list.</param>
        /// <param name="tag">Partition tag the row is archived under.</param>
        /// <param name="unknownValue">Called with (column, value) for a tier/category that does not parse (stored as UNKNOWN).</param>
        public static (SaveAuction Row, ScyllaAuctionExtras Extras) Map(ScyllaAuction row, string tag, Action<string, string> unknownValue = null)
        {
            // CassandraToOld deserialises the bids through the Bids getter, which throws on a null blob
            row.SerialisedBids ??= EmptyBids;
            var auction = ScyllaService.CassandraToOld(row);
            auction.Tag = tag;
            auction.UId = UidOf(row);
            auction.Uuid = row.Uuid.ToString("N");
            auction.End = EndOf(row);
            auction.Start = TruncateToMs(ToUtc(row.Start));
            auction.ItemCreatedAt = ToUtc(row.ItemCreatedAt);
            auction.ProfileId = row.ProfileId == Guid.Empty ? null : row.ProfileId.ToString("N");
            auction.NbtData = row.ItemBytes == null ? null : new NbtData { data = row.ItemBytes };
            auction.NBTLookup = new List<NBTLookup>();
            foreach (var bid in auction.Bids)
            {
                bid.AuctionId = auction.Uuid;
                bid.Timestamp = ToUtc(bid.Timestamp);
                if (bid.ProfileId == UnknownBidProfile.ToString("N"))
                    bid.ProfileId = "unknown";
            }
            if (auction.Tier == Tier.UNKNOWN && !string.IsNullOrEmpty(row.Tier) && row.Tier != nameof(Tier.UNKNOWN))
                unknownValue?.Invoke("tier", row.Tier);
            if (auction.Category == Category.UNKNOWN && !string.IsNullOrEmpty(row.Category) && row.Category != nameof(Category.UNKNOWN))
                unknownValue?.Invoke("category", row.Category);

            var enchantments = new List<Enchantment>();
            var unknownEnchantments = new Dictionary<string, int>();
            foreach (var (name, level) in row.Enchantments ?? new Dictionary<string, int>())
            {
                if (TryParseEnchantment(name, out var type) && level >= byte.MinValue && level <= byte.MaxValue)
                    enchantments.Add(new Enchantment(type, (byte)level));
                else
                    unknownEnchantments[name] = level;
            }
            auction.Enchantments = enchantments;

            var extras = new ScyllaAuctionExtras
            {
                HighestBidderName = row.HighestBidderName,
                ProfileName = row.ProfileName,
                CoopName = row.CoopName,
                Color = row.Color,
                ItemUid = row.ItemUid,
                ItemId = ScyllaAuctionExtras.EncodeItemId(row.ItemId),
                NbtLookupStrings = row.NbtLookup,
                UnknownEnchantments = unknownEnchantments,
            };
            return (auction, extras);
        }

        /// <summary>A defined enchantment name (numeric strings and undefined values are not; ToCassandra's <c>unknown{n}</c> placeholders are not either).</summary>
        private static bool TryParseEnchantment(string name, out Enchantment.EnchantmentType type)
        {
            type = default;
            if (string.IsNullOrEmpty(name) || char.IsDigit(name[0]) || name[0] == '-')
                return false;
            return Enum.TryParse(name, out type) && Enum.IsDefined(type);
        }

        /// <summary>
        /// The tag-index entry after phase 2: phase-1 parts (if any) followed by the new Scylla parts.
        /// Source is <see cref="ArchivedAuctionBlock.SourceMerged"/> when phase-1 parts exist, <see cref="ArchivedAuctionBlock.SourceScylla"/> otherwise.
        /// </summary>
        public static ArchivedMonthEntry MergeMonthEntry(ArchivedMonthEntry phaseOne, int month, IReadOnlyList<string> newPartHashes, int newRows, int newSold, DateTime verifiedAt)
        {
            var phaseOneParts = phaseOne?.Parts ?? 0;
            var hashes = new List<string>(phaseOne?.PartSha256 ?? new List<string>());
            if (hashes.Count != phaseOneParts)
                throw new ArgumentException($"Phase-1 entry of {month} has {phaseOneParts} parts but {hashes.Count} hashes", nameof(phaseOne));
            hashes.AddRange(newPartHashes);
            return new ArchivedMonthEntry
            {
                Month = month,
                Parts = hashes.Count,
                Rows = (phaseOne?.Rows ?? 0) + newRows,
                SoldRows = (phaseOne?.SoldRows ?? 0) + newSold,
                PartSha256 = hashes,
                Source = phaseOneParts > 0 ? ArchivedAuctionBlock.SourceMerged : ArchivedAuctionBlock.SourceScylla,
                VerifiedAt = verifiedAt,
            };
        }
    }
}
