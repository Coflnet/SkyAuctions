using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Data.Linq;
using Coflnet.Sky.Auctions.Models;
using Coflnet.Sky.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace Coflnet.Sky.Auctions.Services;

/// <summary>
/// Migrates all auctions from ScyllaDB to S3, month by month per tag.
/// Progress tracked in Redis so it can resume on crash.
/// </summary>
public class ScyllaToS3BackfillService : BackgroundService
{
    private readonly ScyllaService scyllaService;
    private readonly S3AuctionBlobService s3Blobs;
    private readonly S3PlayerIndexService s3PlayerIndex;
    private readonly BloomFilterService bloomFilter;
    private readonly IConnectionMultiplexer redis;
    private readonly ILogger<ScyllaToS3BackfillService> logger;
    private readonly bool enabled;

    private const string ProgressKeyPrefix = "s3_backfill_scylla:";

    public ScyllaToS3BackfillService(
        ScyllaService scyllaService,
        S3AuctionBlobService s3Blobs,
        S3PlayerIndexService s3PlayerIndex,
        BloomFilterService bloomFilter,
        IConnectionMultiplexer redis,
        ILogger<ScyllaToS3BackfillService> logger,
        IConfiguration config)
    {
        this.scyllaService = scyllaService;
        this.s3Blobs = s3Blobs;
        this.s3PlayerIndex = s3PlayerIndex;
        this.bloomFilter = bloomFilter;
        this.redis = redis;
        this.logger = logger;
        this.enabled = config["S3:BackfillScylla"]?.Equals("true", StringComparison.OrdinalIgnoreCase) ?? false;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!enabled)
        {
            logger.LogInformation("Scylla→S3 backfill disabled (S3:BackfillScylla != true)");
            return;
        }

        await Task.Delay(5000, stoppingToken); // let other services initialize
        logger.LogInformation("Starting Scylla→S3 backfill");

        var db = redis.GetDatabase();

        var partitions = await LoadPartitions(stoppingToken);
        logger.LogInformation("Loaded {Count} Scylla partitions for S3 backfill", partitions.Count);

        foreach (var partition in partitions.OrderBy(p => p.TimeKey).ThenBy(p => p.Tag))
        {
            if (stoppingToken.IsCancellationRequested)
            {
                break;
            }

            var progressKey = $"{ProgressKeyPrefix}{partition.Tag}:{partition.TimeKey}";
            var alreadyDone = await db.StringGetAsync(progressKey);
            if (alreadyDone.HasValue)
                continue;

            try
            {
                await BackfillPartition(partition.Tag, partition.TimeKey, stoppingToken);
                await db.StringSetAsync(progressKey, DateTime.UtcNow.ToString("O"));
                logger.LogInformation("Completed backfill for {Tag} timeKey {TimeKey}", partition.Tag, partition.TimeKey);
            }
            catch (OperationCanceledException) { break; }
            catch (Exception e)
            {
                logger.LogError(e, "Error during backfill for {Tag} timeKey {TimeKey}", partition.Tag, partition.TimeKey);
                await Task.Delay(10_000, stoppingToken);
            }
        }

        // Final Bloom flush
        await bloomFilter.FlushIfDirty(stoppingToken);
        logger.LogInformation("Scylla→S3 backfill completed");
    }

    private async Task<List<(string Tag, short TimeKey)>> LoadPartitions(CancellationToken ct)
    {
        var statement = new SimpleStatement("SELECT DISTINCT tag, timekey FROM weekly_auctions_2").SetPageSize(1000);
        var rows = await scyllaService.Session.ExecuteAsync(statement);
        return rows
            .Select(row => (Tag: row.GetValue<string>("tag"), TimeKey: row.GetValue<short>("timekey")))
            .ToList();
    }

    private async Task BackfillPartition(string tag, short timeKey, CancellationToken ct)
    {
        var table = scyllaService.AuctionsTable;
        var auctions = (await table.Where(a => a.Tag == tag && a.TimeKey == timeKey && a.IsSold).ExecuteAsync()).ToList();
        var unsold = (await table.Where(a => a.Tag == tag && a.TimeKey == timeKey && !a.IsSold).ExecuteAsync()).ToList();
        auctions.AddRange(unsold);

        if (auctions.Count == 0)
        {
            return;
        }

        var converted = auctions.Select(ScyllaService.CassandraToOld).ToList();
        var byMonth = converted.GroupBy(a => new DateTime(a.End.Year, a.End.Month, 1));
        foreach (var monthGroup in byMonth)
        {
            await s3Blobs.WriteAuctions(tag, monthGroup.Key, monthGroup, ct);
        }

        foreach (var auction in converted)
        {
            if (!AuctionIdentity.TryParseGuid(auction.Uuid, out var auctionUuid))
            {
                logger.LogWarning("Skipping Scylla backfill record with invalid uuid {Uuid}", auction.Uuid);
                continue;
            }

            bloomFilter.Add(auctionUuid);
            AddPlayerParticipation(auction, auctionUuid);
        }

        await s3PlayerIndex.FlushAll(ct);
    }

    /// <summary>
    /// Backfill a specific tag and timeKey range. Can be called from an API endpoint.
    /// </summary>
    public async Task BackfillTag(string tag, short fromTimeKey, short toTimeKey, CancellationToken ct = default)
    {
        var db = redis.GetDatabase();

        for (short tk = fromTimeKey; tk <= toTimeKey && !ct.IsCancellationRequested; tk++)
        {
            var progressKey = $"{ProgressKeyPrefix}{tag}:{tk}";
            if ((await db.StringGetAsync(progressKey)).HasValue)
                continue;

            await BackfillPartition(tag, tk, ct);
            await db.StringSetAsync(progressKey, DateTime.UtcNow.ToString("O"));
            logger.LogInformation("Backfilled {Tag} timeKey={TimeKey}", tag, tk);
        }

        await bloomFilter.FlushIfDirty(ct);
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
