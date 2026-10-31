using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.Auctions.Models;
using Coflnet.Sky.Auctions.Services;
using Coflnet.Sky.Core;
using Coflnet.Sky.Core.Archive;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
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

    /// <summary>Result of an archive read that may have been replaced by its fallback: <paramref name="Warning"/> is null when the archive answered.</summary>
    public sealed record ArchiveRead<T>(T Value, string Warning);

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

        /// <summary>Default of <c>ARCHIVE:READ_TIMEOUT_MS</c>.</summary>
        public const int DefaultReadTimeoutMs = 3000;

        /// <summary>Time the archive part of an always-on endpoint (<see cref="ReadOrFallbackAsync{T}"/>) may take; zero or less: no limit.</summary>
        public TimeSpan ReadTimeout { get; set; }

        /// <summary>The hosted core reader.</summary>
        public ArchivedAuctionReader Reader { get; }

        private readonly Func<uint, CancellationToken, Task<string>> playerUuid;
        private readonly Func<Guid, CancellationToken, Task<uint?>> playerId;
        private readonly ArchivePlayerPager pager;
        private readonly ILogger<ArchiveReadService> logger;

        /// <summary>
        /// DI constructor. <paramref name="playerUuid"/> maps <c>Players.Id</c> to the uuid (default: the Players table), used for the opt-out check of the player endpoints;
        /// <paramref name="playerId"/> is the inverse (uuid to <c>Players.Id</c>, null when the player is unknown).
        /// <paramref name="lookup"/> is the index access of the player pages (their cursor comes from the index rows; null: pages are not available);
        /// <paramref name="deletedMonths"/> answers the <c>deletedOnly</c> filter (default: the <c>archived_months</c> sentinel rows), reused for <paramref name="deletedMonthsTtl"/> (default 60 s).
        /// </summary>
        public ArchiveReadService(ArchivedAuctionReader reader, Func<uint, CancellationToken, Task<string>> playerUuid = null, Func<Guid, CancellationToken, Task<uint?>> playerId = null,
            IArchiveLocationLookup lookup = null, DeletedMonthsLookup deletedMonths = null, TimeSpan? deletedMonthsTtl = null,
            IConfiguration configuration = null, ILogger<ArchiveReadService> logger = null)
        {
            this.logger = logger;
            ReadTimeout = TimeSpan.FromMilliseconds(configuration?.GetValue<int?>("ARCHIVE:READ_TIMEOUT_MS") ?? DefaultReadTimeoutMs);
            Reader = reader ?? throw new ArgumentNullException(nameof(reader));
            this.playerUuid = playerUuid ?? LookupPlayerUuidAsync;
            this.playerId = playerId ?? LookupPlayerIdAsync;
            pager = lookup == null ? null : new ArchivePlayerPager(reader, lookup, deletedMonths, deletedMonthsTtl);
        }

        private static async Task<uint?> LookupPlayerIdAsync(Guid player, CancellationToken cancellationToken)
        {
            using var db = new HypixelContext();
            var uuid = player.ToString("N");
            var id = await db.Players.AsNoTracking().Where(p => p.UuId == uuid).Select(p => (int?)p.Id).FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
            return id == null || id <= 0 ? null : (uint)id.Value;
        }

        /// <summary><c>Players.Id</c> of a player uuid (the id space of <c>auction_sellers</c>/<c>auction_bidders</c>), null when the player is unknown.</summary>
        public Task<uint?> ResolvePlayerIdAsync(Guid player, CancellationToken cancellationToken = default) => playerId(player, cancellationToken);

        /// <summary>
        /// Archived auctions a player (uuid) sold or listed that ended before <paramref name="before"/>, newest End first, at most <paramref name="amount"/>,
        /// in the <see cref="SaveAuction"/> shape of <c>/api/auction/{uuid}</c>. Empty for an unknown or opted-out player (like <see cref="GetSellsAsync"/>).
        /// </summary>
        public async Task<List<SaveAuction>> GetPlayerSellsAsync(Guid player, DateTime before, int amount, CancellationToken cancellationToken = default)
        {
            if (amount <= 0)
                return new List<SaveAuction>();
            var id = await ResolvePlayerIdAsync(player, cancellationToken).ConfigureAwait(false);
            if (id == null)
                return new List<SaveAuction>();
            if (await IsOptedOutAsync(id.Value, cancellationToken).ConfigureAwait(false))
                return new List<SaveAuction>();
            var cursor = AuctionIndexCursor.After(before, long.MinValue);
            var found = await Reader.FindBySellerAsync(id.Value, null, null, Math.Min(amount, MaxBidLimit), cursor, cancellationToken).ConfigureAwait(false);
            return found.Select(a => a.Row).ToList();
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

        /// <summary>
        /// Continues the result of <c>GET /api/player/{uuid}/auctions</c> from the archive: when Scylla returned fewer than <paramref name="amount"/>
        /// auctions, the archived sells that ended before <paramref name="end"/> (auctions Scylla already returned left out) are added
        /// up to <paramref name="amount"/>. The combined list is sorted newest End first (stable, so equal Ends keep the Scylla rows first).
        /// </summary>
        public async Task<List<SaveAuction>> ContinueFromArchiveAsync(Guid player, DateTime end, int amount, IReadOnlyCollection<SaveAuction> scyllaResult, CancellationToken cancellationToken = default)
        {
            var result = scyllaResult.ToList();
            var missing = amount - result.Count;
            if (missing <= 0)
                return result;
            var seen = new HashSet<string>(result.Select(a => ArchivedAuctionQuery.NormalizeUuid(a.Uuid)).Where(u => u != null), StringComparer.Ordinal);
            // ask for more than missing: rows Scylla returned as well are dropped below
            var archived = await GetPlayerSellsAsync(player, end, Math.Min(MaxBidLimit, missing + result.Count), cancellationToken).ConfigureAwait(false);
            foreach (var auction in archived)
            {
                if (result.Count >= amount)
                    break;
                if (seen.Add(ArchivedAuctionQuery.NormalizeUuid(auction.Uuid)))
                    result.Add(auction);
            }
            return result.OrderByDescending(a => a.End).ToList();
        }

        /// <summary><see cref="ContinueFromArchiveAsync"/> limited to <see cref="ReadTimeout"/>; on timeout or failure the Scylla result is returned unchanged with a warning.</summary>
        public Task<ArchiveRead<List<SaveAuction>>> ContinueFromArchiveOrFallbackAsync(Guid player, DateTime end, int amount, IReadOnlyCollection<SaveAuction> scyllaResult, CancellationToken cancellationToken = default)
            => ReadOrFallbackAsync(token => ContinueFromArchiveAsync(player, end, amount, scyllaResult, token), scyllaResult.ToList(), "player auctions", cancellationToken);

        /// <summary>
        /// Runs an archive read for an always-on endpoint with a linked token that cancels after <see cref="ReadTimeout"/>. When it times out or fails
        /// (other than the caller cancelling, which is rethrown) the <paramref name="fallback"/> is returned with a warning (<c>timeout</c>/<c>failed</c>).
        /// </summary>
        public async Task<ArchiveRead<T>> ReadOrFallbackAsync<T>(Func<CancellationToken, Task<T>> read, T fallback, string what, CancellationToken cancellationToken = default)
        {
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            linked.CancelAfter(ReadTimeout > TimeSpan.Zero ? ReadTimeout : Timeout.InfiniteTimeSpan);
            try
            {
                return new ArchiveRead<T>(await read(linked.Token).ConfigureAwait(false), null);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (OperationCanceledException)
            {
                logger?.LogWarning("Archive read of {What} timed out after {Timeout} ms, answering without the archive", what, ReadTimeout.TotalMilliseconds);
                return new ArchiveRead<T>(fallback, "timeout");
            }
            catch (Exception e)
            {
                logger?.LogWarning(e, "Archive read of {What} failed, answering without the archive", what);
                return new ArchiveRead<T>(fallback, "failed");
            }
        }

        /// <summary>
        /// Participation entries (the shape of the S3 v1 player index) of the archived auctions a player sold and/or bid on that ended in
        /// <c>[from, to)</c>, newest first, at most <paramref name="limit"/>. Empty for an unknown or opted-out player.
        /// </summary>
        public async Task<List<PlayerParticipationEntry>> GetPlayerParticipationsAsync(Guid player, ParticipationType? type, DateTime from, DateTime to, int limit, CancellationToken cancellationToken = default)
        {
            var result = new List<PlayerParticipationEntry>();
            if (limit <= 0)
                return result;
            var id = await ResolvePlayerIdAsync(player, cancellationToken).ConfigureAwait(false);
            if (id == null || await IsOptedOutAsync(id.Value, cancellationToken).ConfigureAwait(false))
                return result;
            var (fromMonth, toMonth) = (ArchiveMonths.MonthOf(from), ArchiveMonths.MonthOf(to));
            var cursor = AuctionIndexCursor.After(to, long.MinValue);
            var take = Math.Min(limit, MaxBidLimit);
            if (type != ParticipationType.Bidder)
                foreach (var a in await Reader.FindBySellerAsync(id.Value, fromMonth, toMonth, take, cursor, cancellationToken).ConfigureAwait(false))
                    AddEntry(result, a, ParticipationType.Seller, from, to);
            if (type != ParticipationType.Seller)
                foreach (var a in await Reader.FindByBidderAsync(id.Value, fromMonth, toMonth, take, cursor, cancellationToken).ConfigureAwait(false))
                    AddEntry(result, a, ParticipationType.Bidder, from, to);
            return result.OrderByDescending(e => e.End).Take(limit).ToList();
        }

        private static void AddEntry(List<PlayerParticipationEntry> into, ArchivedAuction auction, ParticipationType type, DateTime from, DateTime to)
        {
            var row = auction.Row;
            if (row.End < from || row.End >= to || !AuctionIdentity.TryParseGuid(row.Uuid, out var uuid))
                return;
            into.Add(new PlayerParticipationEntry { AuctionUid = uuid, End = row.End, Tag = row.Tag, Type = type });
        }

        /// <summary>One archived auction as a <see cref="SaveAuction"/> (bids included), null when it is not archived.</summary>
        public async Task<SaveAuction> FindAuctionAsync(Guid uuid, CancellationToken cancellationToken = default)
            => (await Reader.FindByUuidAsync(uuid.ToString("N"), cancellationToken).ConfigureAwait(false))?.Row;

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

        /// <summary>
        /// A page of the archived auctions a player bid on (see <see cref="ArchivePlayerPager"/>): the cursor of the next page comes from the index rows.
        /// <paramref name="deletedOnly"/> limits the page to auctions of months whose MariaDB rows were deleted, decided before any block is read.
        /// </summary>
        public Task<ArchivedPlayerPage> GetBidsPageAsync(uint bidderId, int? fromMonth, int? toMonth, int? limit, AuctionIndexCursor before = null, bool deletedOnly = false, CancellationToken cancellationToken = default)
            => GetPageAsync(bidderId, false, fromMonth, toMonth, limit, before, deletedOnly, cancellationToken);

        /// <summary>A page of the archived auctions a player sold or listed; same rules as <see cref="GetBidsPageAsync"/>.</summary>
        public Task<ArchivedPlayerPage> GetSellsPageAsync(uint sellerId, int? fromMonth, int? toMonth, int? limit, AuctionIndexCursor before = null, bool deletedOnly = false, CancellationToken cancellationToken = default)
            => GetPageAsync(sellerId, true, fromMonth, toMonth, limit, before, deletedOnly, cancellationToken);

        private async Task<ArchivedPlayerPage> GetPageAsync(uint playerId, bool sells, int? fromMonth, int? toMonth, int? limit, AuctionIndexCursor before, bool deletedOnly, CancellationToken cancellationToken)
        {
            if (pager == null)
                throw new InvalidOperationException("The player pages need the archive location lookup");
            if (await IsOptedOutAsync(playerId, cancellationToken).ConfigureAwait(false))
                return new ArchivedPlayerPage(new List<ArchivedAuctionDto>(), null);
            var take = ClampLimit(limit, DefaultBidLimit, MaxBidLimit);
            return await pager.GetPageAsync(playerId, sells, fromMonth, toMonth, take, before, deletedOnly, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>Uncached hash verification of a tag-month.</summary>
        public Task<ArchivedMonthVerification> VerifyAsync(string tag, int month, CancellationToken cancellationToken = default)
            => Reader.VerifyMonthAsync(tag, month, cancellationToken);
    }
}
