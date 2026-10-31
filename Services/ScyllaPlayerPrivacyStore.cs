using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Data.Linq;
using Coflnet.Sky.Auctions.Archive;
using Coflnet.Sky.Auctions.Models;
using Coflnet.Sky.Core;
using Coflnet.Sky.Core.Archive;
using Microsoft.Extensions.Logging;

namespace Coflnet.Sky.Auctions.Services;

/// <summary>Scylla and (optional) S3 backed <see cref="IPlayerPrivacyStore"/>. Kept thin on purpose, the logic lives in <see cref="PlayerPrivacyService"/>.</summary>
public class ScyllaPlayerPrivacyStore : IPlayerPrivacyStore
{
    private readonly ScyllaService scylla;
    private readonly S3StorageService s3;
    private readonly S3PlayerIndexService playerIndex;
    private readonly ArchiveReadService archiveV2;

    private readonly ILogger logger;

    // the service's ILogger output only goes to the OTLP exporter, mirror it to the console like the rest of the service
    private void Log(string template, params object[] args)
    {
        logger?.LogInformation(template, args);
        Console.WriteLine("privacy: " + template + " | " + string.Join(", ", args));
    }

    public ScyllaPlayerPrivacyStore(ScyllaService scylla, S3StorageService s3 = null, S3PlayerIndexService playerIndex = null, ILogger logger = null, ArchiveReadService archiveV2 = null)
    {
        this.archiveV2 = archiveV2;
        this.logger = logger;
        this.scylla = scylla;
        this.s3 = s3;
        this.playerIndex = playerIndex;
    }

    /// <summary>Rows per page of the secondary index scans</summary>
    public const int IndexPageSize = 100;

    public async Task<List<CassandraBid>> GetBids(Guid player, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        return (await scylla.GetBidsTableInstance().Where(b => b.BidderUuid == player).ExecuteAsync()).ToList();
    }

    // same shape as ScyllaService.GetRecentFromPlayer (index + ALLOW FILTERING), but read page by page
    public Task<List<ScyllaAuction>> GetAuctionsBySeller(Guid player, CancellationToken ct) =>
        ReadPaged(scylla.AuctionsTable.Where(a => a.Auctioneer == player).AllowFiltering(), ct);

    private async Task<List<ScyllaAuction>> ReadPaged(CqlQuery<ScyllaAuction> query, CancellationToken ct)
    {
        var result = new List<ScyllaAuction>();
        var pages = 0;
        var sw = System.Diagnostics.Stopwatch.StartNew();
        query.SetPageSize(IndexPageSize);
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            var page = await query.ExecutePagedAsync();
            pages++;
            result.AddRange(page);
            if (pages % 5 == 0)
                Log("Privacy index query progress {Rows} rows, {Pages} pages, {Elapsed}ms", result.Count, pages, sw.ElapsedMilliseconds);
            if (page.PagingState == null)
                break;
            query.SetPagingState(page.PagingState);
        }
        Log("Privacy index query read {Rows} rows in {Pages} pages, {Elapsed}ms", result.Count, pages, sw.ElapsedMilliseconds);
        return result;
    }

    // same shape as ScyllaService.GetAuction(Guid): plain secondary index lookup without ALLOW FILTERING
    public async Task<List<ScyllaAuction>> GetAuctionsByAuctionUuid(Guid auctionUuid, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        var uid = AuctionService.Instance.GetId(auctionUuid.ToString("N"));
        return (await scylla.AuctionsTable.Where(a => a.AuctionUid == uid).ExecuteAsync()).ToList();
    }


    public Task UpdateAuctionIdentity(ScyllaAuction row, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        return scylla.UpdateAuctionIdentity(row);
    }

    public async Task DeleteBids(Guid player, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        await scylla.GetBidsTableInstance().Where(b => b.BidderUuid == player).Delete().SetConsistencyLevel(ConsistencyLevel.LocalQuorum).ExecuteAsync();
    }

    /// <summary>Entries per role and page when listing the archive v2 index</summary>
    public const int ArchiveV2PageSize = 1000;
    /// <summary>Entries per role after which the archive v2 listing stops</summary>
    public const int ArchiveV2MaxEntriesPerRole = 200_000;

    /// <summary>
    /// The player's entries of the R2 archive v2 index. A failure while listing (MariaDB down, timeout) does not fail the whole export: the entries
    /// read so far are returned, <c>Truncated</c> is set and <c>Reason</c> says why, so the export can state that this part is incomplete.
    /// </summary>
    public async Task<(List<ArchivedPlayerEntry> Entries, bool Truncated, string Reason)> GetArchiveV2Participation(Guid player, CancellationToken ct)
    {
        var entries = new List<ArchivedPlayerEntry>();
        if (archiveV2 == null)
            return (entries, false, null);
        try
        {
            var truncated = await ListArchiveV2Async(player, entries, ct);
            Log("Privacy archive v2 listing of {Player}: {Count} entries, truncated {Truncated}", player, entries.Count, truncated);
            return (entries, truncated, truncated ? $"stopped at the cap of {ArchiveV2Cap} entries per role" : null);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
        {
            var reason = $"listing failed after {entries.Count} entries ({ex.GetType().Name}: {ex.Message})";
            logger?.LogError(ex, "Privacy archive v2 listing of {Player} failed after {Count} entries", player, entries.Count);
            Log("Privacy archive v2 listing of {Player} failed", player, reason);
            return (entries, true, reason);
        }
    }

    private async Task<bool> ListArchiveV2Async(Guid player, List<ArchivedPlayerEntry> entries, CancellationToken ct)
    {
        var id = await archiveV2.ResolvePlayerIdAsync(player, ct);
        if (id == null)
            return false;
        var sellerTruncated = await ListArchiveV2RoleAsync(id.Value, "Seller", entries, ct);
        var bidderTruncated = await ListArchiveV2RoleAsync(id.Value, "Bidder", entries, ct);
        return sellerTruncated || bidderTruncated;
    }

    /// <summary>Entries per role after which the listing stops (the constant; tests lower it).</summary>
    internal int ArchiveV2Cap { get; set; } = ArchiveV2MaxEntriesPerRole;

    /// <summary>Pages through one role of the index into <paramref name="entries"/>; true only when MORE than the cap exists (one extra row is fetched to know).</summary>
    private async Task<bool> ListArchiveV2RoleAsync(uint playerId, string role, List<ArchivedPlayerEntry> entries, CancellationToken ct)
    {
        AuctionIndexCursor cursor = null;
        var count = 0;
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            var limit = Math.Min(ArchiveV2PageSize, ArchiveV2Cap + 1 - count);
            var page = role == "Seller"
                ? await archiveV2.Reader.ListSellerParticipationAsync(playerId, limit, cursor, ct)
                : await archiveV2.Reader.ListBidderParticipationAsync(playerId, limit, cursor, ct);
            var room = ArchiveV2Cap - count;
            entries.AddRange(page.Take(room).Select(p => new ArchivedPlayerEntry { Role = role, AuctionUid = p.Uid, End = p.End, Tag = p.Tag, Month = p.Month, Part = p.Part }));
            count += Math.Min(page.Count, room);
            if (page.Count > room)
                return true;
            if (page.Count < limit)
                return false;
            cursor = AuctionIndexCursor.After(page[^1].End, page[^1].Uid);
        }
    }

    public async Task<Dictionary<int, List<PlayerParticipationEntry>>> GetS3Participation(Guid player, CancellationToken ct)
    {
        var result = new Dictionary<int, List<PlayerParticipationEntry>>();
        if (s3 == null || playerIndex == null)
            return result;
        foreach (var key in await s3.ListBlobs(IndexPrefix(player), ct))
        {
            var year = YearOf(key);
            if (year != null)
                result[year.Value] = await playerIndex.ReadParticipationUnfiltered(player, year.Value, ct);
        }
        return result;
    }

    public async Task<int> DeleteS3PlayerIndex(Guid player, CancellationToken ct)
    {
        if (s3 == null)
            return 0;
        var keys = await s3.ListBlobs(IndexPrefix(player), ct);
        foreach (var key in keys)
            await s3.DeleteBlob(key, ct);
        return keys.Count;
    }

    private static string IndexPrefix(Guid player) => $"players/{player.ToString("N")[..2]}/{player:N}/";

    private static int? YearOf(string key)
    {
        var name = key[(key.LastIndexOf('/') + 1)..];
        return int.TryParse(name.Split('.')[0], out var year) ? year : null;
    }
}
