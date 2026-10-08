using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Data.Linq;
using Coflnet.Sky.Auctions.Models;
using Coflnet.Sky.Core;

namespace Coflnet.Sky.Auctions.Services;

/// <summary>Scylla and (optional) S3 backed <see cref="IPlayerPrivacyStore"/>. Kept thin on purpose, the logic lives in <see cref="PlayerPrivacyService"/>.</summary>
public class ScyllaPlayerPrivacyStore : IPlayerPrivacyStore
{
    private readonly ScyllaService scylla;
    private readonly S3StorageService s3;
    private readonly S3PlayerIndexService playerIndex;

    public ScyllaPlayerPrivacyStore(ScyllaService scylla, S3StorageService s3 = null, S3PlayerIndexService playerIndex = null)
    {
        this.scylla = scylla;
        this.s3 = s3;
        this.playerIndex = playerIndex;
    }

    public async Task<List<CassandraBid>> GetBids(Guid player) =>
        (await scylla.GetBidsTableInstance().Where(b => b.BidderUuid == player).ExecuteAsync()).ToList();

    public async Task<List<ScyllaAuction>> GetAuctionsBySeller(Guid player) =>
        (await scylla.AuctionsTable.Where(a => a.Auctioneer == player).AllowFiltering().ExecuteAsync()).ToList();

    public async Task<List<ScyllaAuction>> GetAuctionsByHighestBidder(Guid player) =>
        (await scylla.AuctionsTable.Where(a => a.HighestBidder == player).AllowFiltering().ExecuteAsync()).ToList();

    public async Task<List<ScyllaAuction>> GetAuctionsByAuctionUuid(Guid auctionUuid)
    {
        var uid = AuctionService.Instance.GetId(auctionUuid.ToString("N"));
        return (await scylla.AuctionsTable.Where(a => a.AuctionUid == uid).AllowFiltering().ExecuteAsync()).ToList();
    }

    public async Task<ScyllaAuction> GetAuction(AuctionRowKey key) =>
        (await scylla.AuctionsTable.Where(a => a.Tag == key.Tag && a.TimeKey == key.TimeKey && a.IsSold == key.IsSold && a.End == key.End && a.AuctionUid == key.AuctionUid)
            .ExecuteAsync()).FirstOrDefault();

    public Task UpdateAuctionIdentity(ScyllaAuction row) => scylla.UpdateAuctionIdentity(row);

    public async Task DeleteBids(Guid player) =>
        await scylla.GetBidsTableInstance().Where(b => b.BidderUuid == player).Delete().SetConsistencyLevel(ConsistencyLevel.LocalQuorum).ExecuteAsync();

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
