using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Data.Linq;
using Coflnet.Sky.Core;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Prometheus;

namespace Coflnet.Sky.Auctions.Services;

/// <summary>
/// Background service that migrates auctions from ScyllaDB to S3 storage
/// Verifies data integrity before deletion and supports dry-run mode
/// </summary>
public class S3MigrationService : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<S3MigrationService> _logger;
    private readonly S3AuctionStorage _s3Storage;
    private readonly ScyllaService _scyllaService;

    // Configuration
    private readonly bool _dryRun = true; // Set to false when ready for actual deletion
    private readonly int _monthsToKeepInScylla = 3; // Keep last 3 months in ScyllaDB for fast access
    private readonly int _batchSize = 1000;

    // Metrics
    private static readonly Counter MigratedAuctions = Metrics.CreateCounter(
        "sky_auctions_s3_migrated_total",
        "Total number of auctions migrated to S3",
        new CounterConfiguration { LabelNames = new[] { "tag", "status" } });

    private static readonly Counter VerificationFailures = Metrics.CreateCounter(
        "sky_auctions_s3_verification_failures_total",
        "Number of verification failures during migration");

    private static readonly Gauge MigrationProgress = Metrics.CreateGauge(
        "sky_auctions_s3_migration_progress",
        "Current migration progress",
        new GaugeConfiguration { LabelNames = new[] { "tag", "year_month" } });

    /// <summary>
    /// Creates a new S3MigrationService
    /// </summary>
    public S3MigrationService(
        IServiceScopeFactory scopeFactory,
        ILogger<S3MigrationService> logger,
        S3AuctionStorage s3Storage,
        ScyllaService scyllaService)
    {
        _scopeFactory = scopeFactory;
        _logger = logger;
        _s3Storage = s3Storage;
        _scyllaService = scyllaService;
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Wait for services to initialize
        await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);

        _logger.LogInformation("S3 Migration Service started (DryRun: {DryRun})", _dryRun);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RunMigrationCycle(stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during S3 migration cycle");
            }

            // Run once per day
            await Task.Delay(TimeSpan.FromHours(24), stoppingToken);
        }
    }

    private async Task RunMigrationCycle(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Starting S3 migration cycle");

        // Get all unique tags from ScyllaDB
        var tags = await GetAllTags();
        _logger.LogInformation("Found {Count} unique tags to process", tags.Count);

        var cutoffDate = DateTime.UtcNow.AddMonths(-_monthsToKeepInScylla);

        foreach (var tag in tags)
        {
            if (stoppingToken.IsCancellationRequested)
                break;

            try
            {
                await MigrateTagData(tag, cutoffDate, stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error migrating data for tag {Tag}", tag);
            }

            // Small delay between tags to avoid overwhelming the system
            await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
        }

        _logger.LogInformation("Completed S3 migration cycle");
    }

    private async Task<List<string>> GetAllTags()
    {
        // Query distinct tags from ScyllaDB
        // This is a simplified approach - in production you might want to maintain a tag list
        using var scope = _scopeFactory.CreateScope();
        var context = scope.ServiceProvider.GetRequiredService<HypixelContext>();

        var tags = await context.Items
            .Select(i => i.Tag)
            .Distinct()
            .ToListAsync();

        return tags.Where(t => !string.IsNullOrEmpty(t)).ToList();
    }

    private async Task MigrateTagData(string tag, DateTime cutoffDate, CancellationToken stoppingToken)
    {
        _logger.LogInformation("Processing tag: {Tag}", tag);

        // Get the time key range for data older than cutoff
        var startTimeKey = ScyllaService.GetWeekOrDaysSinceStart(tag, new DateTime(2019, 1, 1));
        var endTimeKey = ScyllaService.GetWeekOrDaysSinceStart(tag, cutoffDate);

        // Process month by month
        var currentDate = new DateTime(2019, 1, 1);
        while (currentDate < cutoffDate)
        {
            if (stoppingToken.IsCancellationRequested)
                break;

            var year = currentDate.Year;
            var month = currentDate.Month;

            // Check if this month was already migrated
            if (await _s3Storage.MonthDataExists(tag, year, month))
            {
                _logger.LogDebug("Month {Year}/{Month} for {Tag} already migrated", year, month, tag);
                currentDate = currentDate.AddMonths(1);
                continue;
            }

            // Get auctions for this month
            var monthStart = new DateTime(year, month, 1);
            var monthEnd = monthStart.AddMonths(1);
            var auctions = await GetAuctionsForMonth(tag, monthStart, monthEnd);

            if (auctions.Count > 0)
            {
                await MigrateMonth(tag, year, month, auctions, stoppingToken);
            }

            currentDate = currentDate.AddMonths(1);
        }
    }

    private async Task<List<ScyllaAuction>> GetAuctionsForMonth(string tag, DateTime monthStart, DateTime monthEnd)
    {
        var auctions = new List<ScyllaAuction>();

        var startKey = ScyllaService.GetWeekOrDaysSinceStart(tag, monthStart);
        var endKey = ScyllaService.GetWeekOrDaysSinceStart(tag, monthEnd);

        for (var timeKey = startKey; timeKey <= endKey; timeKey++)
        {
            try
            {
                var batch = await _scyllaService.AuctionsTable
                    .Where(a => a.Tag == tag && a.TimeKey == timeKey && a.End >= monthStart && a.End < monthEnd)
                    .ExecuteAsync();

                auctions.AddRange(batch);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error fetching auctions for tag {Tag} timeKey {TimeKey}", tag, timeKey);
            }
        }

        return auctions;
    }

    private async Task MigrateMonth(string tag, int year, int month, List<ScyllaAuction> auctions, CancellationToken stoppingToken)
    {
        var yearMonth = $"{year}/{month:D2}";
        _logger.LogInformation("Migrating {Count} auctions for {Tag} {YearMonth}", auctions.Count, tag, yearMonth);
        MigrationProgress.WithLabels(tag, yearMonth).Set(0);

        try
        {
            // Step 1: Store in S3
            await _s3Storage.StoreAuctions(tag, year, month, auctions);
            MigrationProgress.WithLabels(tag, yearMonth).Set(0.5);

            // Step 2: Verify the data was stored correctly
            var verified = await VerifyMigration(tag, year, month, auctions);

            if (!verified)
            {
                VerificationFailures.Inc();
                _logger.LogError("Verification failed for {Tag} {YearMonth} - NOT deleting from ScyllaDB", tag, yearMonth);
                MigratedAuctions.WithLabels(tag, "verification_failed").Inc(auctions.Count);
                return;
            }

            MigrationProgress.WithLabels(tag, yearMonth).Set(0.75);

            // Step 3: Delete from ScyllaDB (or log in dry-run mode)
            if (_dryRun)
            {
                _logger.LogInformation("[DRY RUN] Would delete {Count} auctions for {Tag} {YearMonth} from ScyllaDB",
                    auctions.Count, tag, yearMonth);
                MigratedAuctions.WithLabels(tag, "dry_run").Inc(auctions.Count);
            }
            else
            {
                await DeleteFromScylla(tag, auctions);
                MigratedAuctions.WithLabels(tag, "success").Inc(auctions.Count);
            }

            MigrationProgress.WithLabels(tag, yearMonth).Set(1.0);
            _logger.LogInformation("Successfully migrated {Count} auctions for {Tag} {YearMonth}", auctions.Count, tag, yearMonth);
        }
        catch (Exception ex)
        {
            MigratedAuctions.WithLabels(tag, "error").Inc(auctions.Count);
            _logger.LogError(ex, "Error during migration of {Tag} {YearMonth}", tag, yearMonth);
            throw;
        }
    }

    private async Task<bool> VerifyMigration(string tag, int year, int month, List<ScyllaAuction> originalAuctions)
    {
        try
        {
            // Load data back from S3
            var s3Auctions = await _s3Storage.GetAuctions(tag, year, month);

            // Verify count matches
            if (s3Auctions.Count != originalAuctions.Count)
            {
                _logger.LogError("Auction count mismatch: original={Original}, s3={S3}",
                    originalAuctions.Count, s3Auctions.Count);
                return false;
            }

            // Verify all auction UUIDs are present
            var originalUuids = originalAuctions.Select(a => a.Uuid).ToHashSet();
            var s3Uuids = s3Auctions.Select(a => a.Uuid).ToHashSet();

            if (!originalUuids.SetEquals(s3Uuids))
            {
                var missing = originalUuids.Except(s3Uuids).Take(10).ToList();
                var extra = s3Uuids.Except(originalUuids).Take(10).ToList();
                _logger.LogError("UUID mismatch - Missing: {Missing}, Extra: {Extra}",
                    string.Join(", ", missing), string.Join(", ", extra));
                return false;
            }

            // Spot check some auctions for data integrity
            var random = new Random();
            var samplesToCheck = Math.Min(10, originalAuctions.Count);
            var samples = originalAuctions.OrderBy(_ => random.Next()).Take(samplesToCheck).ToList();

            foreach (var original in samples)
            {
                var s3Version = s3Auctions.FirstOrDefault(a => a.Uuid == original.Uuid);
                if (s3Version == null)
                {
                    _logger.LogError("Sample auction {Uuid} not found in S3", original.Uuid);
                    return false;
                }

                // Verify critical fields
                if (original.HighestBidAmount != s3Version.HighestBidAmount ||
                    original.Auctioneer != s3Version.Auctioneer ||
                    original.End != s3Version.End ||
                    original.Tag != s3Version.Tag)
                {
                    _logger.LogError("Data mismatch for auction {Uuid}: Original={Original}, S3={S3}",
                        original.Uuid,
                        JsonConvert.SerializeObject(new { original.HighestBidAmount, original.Auctioneer, original.End, original.Tag }),
                        JsonConvert.SerializeObject(new { s3Version.HighestBidAmount, s3Version.Auctioneer, s3Version.End, s3Version.Tag }));
                    return false;
                }
            }

            _logger.LogInformation("Verification passed for {Tag} {Year}/{Month}: {Count} auctions verified",
                tag, year, month, originalAuctions.Count);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Verification failed with exception for {Tag} {Year}/{Month}", tag, year, month);
            return false;
        }
    }

    private async Task DeleteFromScylla(string tag, List<ScyllaAuction> auctions)
    {
        var session = _scyllaService.Session;
        var table = _scyllaService.AuctionsTable;

        // Delete in batches
        var batches = auctions.Chunk(_batchSize);

        foreach (var batch in batches)
        {
            var batchStatement = new BatchStatement();

            foreach (var auction in batch)
            {
                var deleteStatement = table
                    .Where(a => a.Tag == auction.Tag &&
                               a.TimeKey == auction.TimeKey &&
                               a.IsSold == auction.IsSold &&
                               a.End == auction.End &&
                               a.AuctionUid == auction.AuctionUid)
                    .Delete();
                batchStatement.Add(deleteStatement);
            }

            batchStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            await session.ExecuteAsync(batchStatement);

            _logger.LogDebug("Deleted batch of {Count} auctions from ScyllaDB", batch.Length);
        }
    }

    /// <summary>
    /// Manually trigger migration for a specific tag and date range
    /// </summary>
    public async Task MigrateManual(string tag, DateTime startDate, DateTime endDate, bool dryRun = true)
    {
        _logger.LogInformation("Manual migration requested for {Tag} from {Start} to {End} (DryRun: {DryRun})",
            tag, startDate, endDate, dryRun);

        var currentDate = new DateTime(startDate.Year, startDate.Month, 1);
        while (currentDate < endDate)
        {
            var year = currentDate.Year;
            var month = currentDate.Month;
            var monthEnd = currentDate.AddMonths(1);

            var auctions = await GetAuctionsForMonth(tag, currentDate, monthEnd);
            if (auctions.Count > 0)
            {
                await _s3Storage.StoreAuctions(tag, year, month, auctions);

                if (await VerifyMigration(tag, year, month, auctions))
                {
                    if (!dryRun)
                    {
                        await DeleteFromScylla(tag, auctions);
                        _logger.LogInformation("Deleted {Count} auctions for {Tag} {Year}/{Month}",
                            auctions.Count, tag, year, month);
                    }
                    else
                    {
                        _logger.LogInformation("[DRY RUN] Would delete {Count} auctions for {Tag} {Year}/{Month}",
                            auctions.Count, tag, year, month);
                    }
                }
            }

            currentDate = currentDate.AddMonths(1);
        }
    }
}
