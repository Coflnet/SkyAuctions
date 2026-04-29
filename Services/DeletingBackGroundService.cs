using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System.Linq;
using Microsoft.Extensions.Logging;
using Coflnet.Sky.Core;
using System;
using MoreLinq;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace Coflnet.Sky.Auctions.Services;

public class DeletingBackGroundService : BackgroundService
{

    private IServiceScopeFactory scopeFactory;
    private ILogger<DeletingBackGroundService> logger;
    private S3AuctionBlobService s3Blobs;
    private readonly IConfiguration config;
    private readonly bool deletionEnabled;
    private readonly bool requireFullContentMatch;
    private readonly int batchSize;
    private Prometheus.Counter deleteCount = Prometheus.Metrics.CreateCounter("sky_auctions_delete_count", "How many auctions were deleted");
    private Prometheus.Counter verifyMissCount = Prometheus.Metrics.CreateCounter("sky_auctions_delete_verify_miss_total", "Auctions skipped from deletion because S3 archive did not match");

    public DeletingBackGroundService(IServiceScopeFactory scopeFactory, ILogger<DeletingBackGroundService> logger, IConfiguration config, S3AuctionBlobService s3Blobs = null)
    {
        this.scopeFactory = scopeFactory;
        this.logger = logger;
        this.s3Blobs = s3Blobs;
        this.config = config;
        // Two independent gates so it cannot run by accident:
        //   S3:EnableDeletion (registered) AND S3:DeletionConfirmed (operator confirmed they verified the archive).
        this.deletionEnabled = config.GetValue<bool>("S3:DeletionConfirmed");
        this.requireFullContentMatch = config.GetValue<bool?>("S3:DeletionRequireFullContentMatch") ?? true;
        this.batchSize = config.GetValue<int?>("S3:DeletionBatchSize") ?? 32;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!deletionEnabled)
        {
            logger.LogWarning("DeletingBackGroundService running in DRY-RUN mode (S3:DeletionConfirmed=false). No rows will be removed.");
        }
        while (!stoppingToken.IsCancellationRequested)
        {
            await Delete();
            await Task.Delay(5000, stoppingToken);
        }
    }


    private async Task Delete()
    {
        var threeYearsAgo = DateTime.UtcNow.AddYears(-3);
        var biggestDate = new DateTime();
        var highest = 0;
        while (biggestDate < threeYearsAgo)
        {
            List<SaveAuction> batch = new();
            using (var context = new HypixelContext())
                batch = await context.Auctions.Where(a => a.Id > highest).OrderBy(a => a.Id).Take(128).ToListAsync();
            if (batch.Count == 0)
            {
                break;
            }
            biggestDate = batch.LastOrDefault()?.End ?? DateTime.UtcNow;
            logger.LogInformation("Deleting batch");
            var w1 = DeleteBatch(threeYearsAgo, batch.Take(64).ToList());
            var w2 = DeleteBatch(threeYearsAgo, batch.Skip(64).ToList());
            await Task.WhenAll(w1, w2);
            logger.LogInformation("sheduled batch for delete {id}", highest);
            highest = batch.Max(b => b.Id);
            if (DateTime.UtcNow.DayOfWeek == DayOfWeek.Saturday)
            {
                // higher load, wait longer
                await Task.Delay(20_000);
            }
            await Task.Delay(15_000);
        }
    }

    private async Task DeleteBatch(DateTime threeYearsAgo, List<SaveAuction> batch)
    {
        if (batch.Count == 0)
        {
            return;
        }

        var verified = batch;
        if (s3Blobs == null)
        {
            // Refuse to delete if the archive isn't even configured.
            logger.LogWarning("S3 archive not configured; deletion skipped for {Count} rows", batch.Count);
            return;
        }

        verified = new List<SaveAuction>();
        foreach (var group in batch.GroupBy(a => (Tag: a.Tag ?? "unknown", Month: new DateTime(a.End.Year, a.End.Month, 1))))
        {
            try
            {
                var archived = await s3Blobs.ReadAuctions(group.Key.Tag, group.Key.Month);
                var archivedById = new Dictionary<string, SaveAuction>(StringComparer.OrdinalIgnoreCase);
                foreach (var a in archived)
                {
                    var n = AuctionIdentity.NormalizeUuid(a.Uuid);
                    if (!string.IsNullOrEmpty(n)) archivedById[n] = a;
                }

                foreach (var auction in group)
                {
                    var normalizedUuid = AuctionIdentity.NormalizeUuid(auction.Uuid);
                    if (string.IsNullOrEmpty(normalizedUuid) || !archivedById.TryGetValue(normalizedUuid, out var archivedAuction))
                    {
                        verifyMissCount.Inc();
                        logger.LogWarning("Skipping deletion for {Uuid}: not present in {Tag}/{Month:yyyy-MM}", auction.Uuid, group.Key.Tag, group.Key.Month);
                        continue;
                    }

                    if (requireFullContentMatch && !ContentMatches(auction, archivedAuction, group.Key.Tag, group.Key.Month))
                    {
                        verifyMissCount.Inc();
                        continue;
                    }

                    verified.Add(auction);
                }
            }
            catch (Exception e)
            {
                logger.LogWarning(e, "Skipping deletion for {Tag}/{Month:yyyy-MM} because the archive blob could not be verified", group.Key.Tag, group.Key.Month);
            }
        }

        if (verified.Count == 0)
        {
            return;
        }

        using var scope = scopeFactory.CreateScope();
        var service = scope.ServiceProvider.GetRequiredService<RestoreService>();
        foreach (var item in verified.OrderBy(i => i.End).Batch(batchSize))
        {
            if (item.First().End > threeYearsAgo)
            {
                logger.LogInformation($"Reached end date {item.First().End}");
                return;
            }
            var guids = item
                .Select(i => AuctionIdentity.ParseGuidOrEmpty(i.Uuid))
                .Where(guid => guid != Guid.Empty)
                .ToArray();
            if (guids.Length == 0)
            {
                continue;
            }
            if (!deletionEnabled)
            {
                // DRY-RUN: log what WOULD be deleted, but do not touch the database.
                logger.LogInformation("[dry-run] would delete {Count} verified auctions: {Guids}", guids.Length, string.Join(", ", guids));
                continue;
            }
            // ---------------------------------------------------------------
            // ACTIVE DELETION PATH - intentionally commented out.
            // Uncomment AFTER:
            //   1. 2019-2023 Scylla data is fully archived in S3 (zero missing, zero extras)
            //   2. Operator has manually inspected a representative sample
            //   3. S3:DeletionConfirmed=true in the running deployment
            // ---------------------------------------------------------------
            // try
            // {
            //     await service.RemoveAuction(guids);
            //     deleteCount.Inc(guids.Length);
            //     logger.LogInformation("Deleted {Count} auctions: {Guids}", guids.Length, string.Join(", ", guids));
            // }
            // catch (Exception e)
            // {
            //     logger.LogError(e, "Error while deleting {Guids}", string.Join(", ", guids));
            //     return;
            // }
            logger.LogWarning("Active delete path is commented out in DeletingBackGroundService.cs - {Count} verified rows left untouched", guids.Length);
        }
    }

    /// <summary>
    /// Strict content comparison between the MariaDB row and the S3-archived auction.
    /// Only fields that are guaranteed-stable post-archive are compared.
    /// </summary>
    private bool ContentMatches(SaveAuction toDelete, SaveAuction archived, string tag, DateTime month)
    {
        bool ok = true;
        if (toDelete.End != archived.End) ok = false;
        if (toDelete.HighestBidAmount != archived.HighestBidAmount) ok = false;
        if (!string.Equals(toDelete.Tag ?? "unknown", archived.Tag ?? "unknown", StringComparison.OrdinalIgnoreCase)) ok = false;
        var dbSeller = AuctionIdentity.NormalizeUuid(toDelete.AuctioneerId);
        var arSeller = AuctionIdentity.NormalizeUuid(archived.AuctioneerId);
        if (!string.IsNullOrEmpty(dbSeller) && !string.IsNullOrEmpty(arSeller) && !string.Equals(dbSeller, arSeller, StringComparison.OrdinalIgnoreCase)) ok = false;
        var dbBidCount = toDelete.Bids?.Count ?? 0;
        var arBidCount = archived.Bids?.Count ?? 0;
        if (dbBidCount != arBidCount) ok = false;

        if (!ok)
        {
            logger.LogWarning("Content mismatch for {Uuid} in {Tag}/{Month:yyyy-MM}: db end={DbEnd} bid={DbBid} bids={DbBids} | s3 end={S3End} bid={S3Bid} bids={S3Bids}",
                toDelete.Uuid, tag, month, toDelete.End, toDelete.HighestBidAmount, dbBidCount, archived.End, archived.HighestBidAmount, arBidCount);
        }
        return ok;
    }
}