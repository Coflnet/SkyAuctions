using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.Core;
using Coflnet.Sky.Core.Archive;
using Microsoft.EntityFrameworkCore;
using Newtonsoft.Json;

namespace Coflnet.Sky.Auctions.Archive
{
    /// <summary>One archived month of a tag as listed by <c>GET /api/archive/v2/{tag}/months</c>.</summary>
    public class ArchivedMonthSummary
    {
        /// <summary>Month, <c>yyyy-MM</c> (usable as the <c>{month}</c> route value).</summary>
        [JsonProperty("month")] public string Month { get; set; }
        /// <summary>Number of parts (blobs).</summary>
        [JsonProperty("parts")] public int Parts { get; set; }
        /// <summary>Rows over every part.</summary>
        [JsonProperty("rows")] public int Rows { get; set; }
        /// <summary>Sold rows over every part.</summary>
        [JsonProperty("soldRows")] public int SoldRows { get; set; }
        /// <summary>mariadb|scylla|merged.</summary>
        [JsonProperty("source")] public string Source { get; set; }
        /// <summary>UTC time the read-back verification passed.</summary>
        [JsonProperty("verifiedAt")] public DateTime VerifiedAt { get; set; }
    }

    /// <summary>
    /// Read side of the auction archive v2 for <see cref="Controllers.ArchiveV2Controller"/>: hosts the core
    /// <see cref="ArchivedAuctionReader"/> (R2 via <see cref="S3AuctionBlobStore"/>, read-only MariaDB via <see cref="ArchiveContext"/>)
    /// and applies the endpoint limits. Registered only when <c>S3:ENABLED</c>.
    /// </summary>
    public class ArchiveReadService
    {
        /// <summary>Default rows of a month read.</summary>
        public const int DefaultMonthLimit = 200;
        /// <summary>Maximum rows of a month read.</summary>
        public const int MaxMonthLimit = 5000;
        /// <summary>Default and maximum rows of an export.</summary>
        public const int MaxExportLimit = 100_000;
        /// <summary>Default auctions of a bidder lookup.</summary>
        public const int DefaultBidLimit = 100;
        /// <summary>Maximum auctions of a bidder lookup.</summary>
        public const int MaxBidLimit = 1000;

        /// <summary>The hosted core reader.</summary>
        public ArchivedAuctionReader Reader { get; }

        private readonly Func<uint, CancellationToken, Task<string>> playerUuid;

        /// <summary>DI constructor. <paramref name="playerUuid"/> maps <c>Players.Id</c> to the uuid (default: the Players table), used for the opt-out check of the player endpoints.</summary>
        public ArchiveReadService(ArchivedAuctionReader reader, Func<uint, CancellationToken, Task<string>> playerUuid = null)
        {
            Reader = reader ?? throw new ArgumentNullException(nameof(reader));
            this.playerUuid = playerUuid ?? LookupPlayerUuidAsync;
        }

        private static async Task<string> LookupPlayerUuidAsync(uint playerId, CancellationToken cancellationToken)
        {
            using var db = new HypixelContext();
            var id = (int)playerId;
            return await db.Players.AsNoTracking().Where(p => p.Id == id).Select(p => p.UuId).FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
        }

        /// <summary>True when the player (<c>Players.Id</c>) opted out of processing; unknown ids are not opted out.</summary>
        public async Task<bool> IsOptedOutAsync(uint playerId, CancellationToken cancellationToken = default)
        {
            var uuid = await playerUuid(playerId, cancellationToken).ConfigureAwait(false);
            return uuid != null && PlayerOptOut.IsOptedOut(uuid);
        }

        /// <summary>A limit within <c>1..max</c>; <paramref name="fallback"/> when not given.</summary>
        public static int ClampLimit(int? limit, int fallback, int max) => Math.Clamp(limit ?? fallback, 1, max);

        /// <summary>Archived months of a tag, null when the tag has no archive.</summary>
        public async Task<List<ArchivedMonthSummary>> GetMonthsAsync(string tag, CancellationToken cancellationToken = default)
        {
            var index = await Reader.GetTagIndexAsync(tag, cancellationToken).ConfigureAwait(false);
            return index?.Months?.Select(m => new ArchivedMonthSummary
            {
                Month = ArchiveMonths.Format(m.Month),
                Parts = m.Parts,
                Rows = m.Rows,
                SoldRows = m.SoldRows,
                Source = m.Source,
                VerifiedAt = DateTime.SpecifyKind(m.VerifiedAt, DateTimeKind.Utc),
            }).ToList();
        }

        /// <summary>True if the month is in the tag's index.</summary>
        public async Task<bool> HasMonthAsync(string tag, int month, CancellationToken cancellationToken = default)
        {
            var index = await Reader.GetTagIndexAsync(tag, cancellationToken).ConfigureAwait(false);
            return index?.Find(month) != null;
        }

        /// <summary>Matching rows of a month as DTOs (the query's limit is clamped to <see cref="MaxMonthLimit"/>), null when the month is not archived.</summary>
        public async Task<List<ArchivedAuctionDto>> ReadMonthAsync(string tag, int month, ArchivedAuctionQuery query, bool includeItemBytes, CancellationToken cancellationToken = default)
        {
            if (!await HasMonthAsync(tag, month, cancellationToken).ConfigureAwait(false))
                return null;
            query.Limit = ClampLimit(query.Limit, DefaultMonthLimit, MaxMonthLimit);
            var result = new List<ArchivedAuctionDto>();
            await foreach (var auction in Reader.ReadMonthAsync(tag, month, query, cancellationToken).ConfigureAwait(false))
                result.Add(ArchivedAuctionDto.From(auction, includeItemBytes));
            return result;
        }

        /// <summary>Matching rows of a month for an export (limit clamped to <see cref="MaxExportLimit"/>, which is also the default).</summary>
        public IAsyncEnumerable<ArchivedAuction> ExportMonth(string tag, int month, ArchivedAuctionQuery query, CancellationToken cancellationToken = default)
        {
            query.Limit = ClampLimit(query.Limit, MaxExportLimit, MaxExportLimit);
            return Reader.ReadMonthAsync(tag, month, query, cancellationToken);
        }

        /// <summary>One archived auction with its bids, null when it is not archived.</summary>
        public async Task<ArchivedAuctionDto> GetAuctionAsync(string uuid, bool includeItemBytes, CancellationToken cancellationToken = default)
        {
            var auction = await Reader.FindByUuidAsync(uuid, cancellationToken).ConfigureAwait(false);
            return ArchivedAuctionDto.From(auction, includeItemBytes, includeBids: true);
        }

        /// <summary>
        /// Archived auctions a player bid on (newest first, with bids), after the paging cursor <paramref name="before"/>. Empty for an opted-out
        /// player (like the player index endpoints); the reader already masks opted-out participants of every returned auction (<see cref="ArchivePrivacy.Mask"/>).
        /// </summary>
        public async Task<List<ArchivedAuctionDto>> GetBidsAsync(uint bidderId, int? fromMonth, int? toMonth, int? limit, AuctionIndexCursor before = null, CancellationToken cancellationToken = default)
        {
            if (await IsOptedOutAsync(bidderId, cancellationToken).ConfigureAwait(false))
                return new List<ArchivedAuctionDto>();
            var found = await Reader.FindByBidderAsync(bidderId, fromMonth, toMonth, ClampLimit(limit, DefaultBidLimit, MaxBidLimit), before, cancellationToken).ConfigureAwait(false);
            return found.Select(a => ArchivedAuctionDto.From(a, includeBids: true)).ToList();
        }

        /// <summary>Archived auctions a player sold or listed; same limits, cursor and privacy rules as <see cref="GetBidsAsync"/>.</summary>
        public async Task<List<ArchivedAuctionDto>> GetSellsAsync(uint sellerId, int? fromMonth, int? toMonth, int? limit, AuctionIndexCursor before = null, CancellationToken cancellationToken = default)
        {
            if (await IsOptedOutAsync(sellerId, cancellationToken).ConfigureAwait(false))
                return new List<ArchivedAuctionDto>();
            var found = await Reader.FindBySellerAsync(sellerId, fromMonth, toMonth, ClampLimit(limit, DefaultBidLimit, MaxBidLimit), before, cancellationToken).ConfigureAwait(false);
            return found.Select(a => ArchivedAuctionDto.From(a, includeBids: true)).ToList();
        }

        /// <summary>Uncached hash verification of a tag-month.</summary>
        public Task<ArchivedMonthVerification> VerifyAsync(string tag, int month, CancellationToken cancellationToken = default)
            => Reader.VerifyMonthAsync(tag, month, cancellationToken);
    }
}
