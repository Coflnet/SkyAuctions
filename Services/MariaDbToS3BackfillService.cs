using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.Auctions.Models;
using Coflnet.Sky.Core;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace Coflnet.Sky.Auctions.Services;

/// <summary>
/// Backfills recent auctions (still in MariaDB) to S3.
/// Queries HypixelContext ascending by Id with full includes.
/// </summary>
public class MariaDbToS3BackfillService : BackgroundService
{
    private readonly IServiceScopeFactory scopeFactory;
    private readonly S3AuctionBlobService s3Blobs;
    private readonly S3PlayerIndexService s3PlayerIndex;
    private readonly BloomFilterService bloomFilter;
    private readonly IConnectionMultiplexer redis;
    private readonly ILogger<MariaDbToS3BackfillService> logger;
    private readonly bool enabled;

    private const string ProgressKey = "s3_mariadb_backfill_last_id";
    private const int BatchSize = 2000;
    private const int PlayerIndexFlushEveryBatches = 20;

    public MariaDbToS3BackfillService(
        IServiceScopeFactory scopeFactory,
        S3AuctionBlobService s3Blobs,
        S3PlayerIndexService s3PlayerIndex,
        BloomFilterService bloomFilter,
        IConnectionMultiplexer redis,
        ILogger<MariaDbToS3BackfillService> logger,
        IConfiguration config)
    {
        this.scopeFactory = scopeFactory;
        this.s3Blobs = s3Blobs;
        this.s3PlayerIndex = s3PlayerIndex;
        this.bloomFilter = bloomFilter;
        this.redis = redis;
        this.logger = logger;
        this.enabled = config["S3:BackfillMariaDb"]?.Equals("true", StringComparison.OrdinalIgnoreCase) ?? false;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!enabled)
        {
            logger.LogInformation("MariaDB→S3 backfill disabled (S3:BackfillMariaDb != true)");
            return;
        }

        await Task.Delay(10_000, stoppingToken); // let other services start
        logger.LogInformation("Starting MariaDB→S3 backfill");

        var db = redis.GetDatabase();
        var lastId = (int)(await db.StringGetAsync(ProgressKey));
        var processedBatches = 0;

        while (!stoppingToken.IsCancellationRequested)
        {
            using var context = new HypixelContext();

            var batch = await context.Auctions
                .Where(a => a.Id > lastId)
                .OrderBy(a => a.Id)
                .Take(BatchSize)
                .Include(a => a.Bids)
                .Include(a => a.Enchantments)
                .Include(a => a.NbtData)
                .Include(a => a.NBTLookup)
                .Include(a => a.CoopMembers)
                .AsNoTracking()
                .ToListAsync(stoppingToken);

            if (batch.Count == 0)
            {
                logger.LogInformation("MariaDB→S3 backfill completed (reached end of MariaDB data at id {LastId})", lastId);
                break;
            }

            // Group by (tag, month) → write to S3
            var groups = batch.GroupBy(a => (Tag: a.Tag ?? "unknown", Month: new DateTime(a.End.Year, a.End.Month, 1)));
            foreach (var group in groups)
            {
                await s3Blobs.WriteAuctions(group.Key.Tag, group.Key.Month, group, stoppingToken);
            }

            // Bloom + player index
            foreach (var a in batch)
            {
                if (!AuctionIdentity.TryParseGuid(a.Uuid, out var auctionUuid))
                {
                    logger.LogWarning("Skipping MariaDB backfill record with invalid uuid {Uuid}", a.Uuid);
                    continue;
                }

                bloomFilter.Add(auctionUuid);
                AddPlayerParticipation(a, auctionUuid);
            }

            processedBatches++;
            if (processedBatches % PlayerIndexFlushEveryBatches == 0)
            {
                await s3PlayerIndex.FlushAll(stoppingToken);
            }

            lastId = batch.Max(a => a.Id);
            await db.StringSetAsync(ProgressKey, lastId);
            logger.LogInformation("MariaDB→S3 backfill: processed up to id {LastId} ({Count} auctions)", lastId, batch.Count);

            await Task.Delay(100, stoppingToken); // small delay to not overload
        }

        await s3PlayerIndex.FlushAll(stoppingToken);
        await bloomFilter.FlushIfDirty(stoppingToken);
    }

    private void AddPlayerParticipation(SaveAuction a, Guid auctionUuid)
    {
        var sellerGuid = AuctionIdentity.ParseGuidOrEmpty(a.AuctioneerId);
        if (sellerGuid != Guid.Empty)
        {
            s3PlayerIndex.AddParticipations(sellerGuid, new[]
            {
                new PlayerParticipationEntry
                {
                    AuctionUid = auctionUuid, End = a.End, Tag = a.Tag, Type = ParticipationType.Seller
                }
            });
        }
        foreach (var bid in a.Bids ?? new())
        {
            var bidderGuid = AuctionIdentity.ParseGuidOrEmpty(bid.Bidder);
            if (bidderGuid != Guid.Empty)
            {
                s3PlayerIndex.AddParticipations(bidderGuid, new[]
                {
                    new PlayerParticipationEntry
                    {
                        AuctionUid = auctionUuid, End = a.End, Tag = a.Tag, Type = ParticipationType.Bidder
                    }
                });
            }
        }
    }
}
