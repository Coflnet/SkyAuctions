using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Data.Linq;
using Coflnet.Sky.Auctions.Models;
using Coflnet.Sky.Core;
using Microsoft.Extensions.Logging;

namespace Coflnet.Sky.Auctions.Services;

/// <summary>Scylla and (optional) S3 backed <see cref="IPlayerPrivacyStore"/>. Kept thin on purpose, the logic lives in <see cref="PlayerPrivacyService"/>.</summary>
public class ScyllaPlayerPrivacyStore : IPlayerPrivacyStore
{
    private readonly ScyllaService scylla;
    private readonly S3StorageService s3;
    private readonly S3PlayerIndexService playerIndex;

    private readonly ILogger logger;

    // the service's ILogger output only goes to the OTLP exporter, mirror it to the console like the rest of the service
    private void Log(string template, params object[] args)
    {
        logger?.LogInformation(template, args);
        Console.WriteLine("privacy: " + template + " | " + string.Join(", ", args));
    }

    public ScyllaPlayerPrivacyStore(ScyllaService scylla, S3StorageService s3 = null, S3PlayerIndexService playerIndex = null, ILogger logger = null)
    {
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
