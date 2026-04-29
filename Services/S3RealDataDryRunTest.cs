using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Data.Linq;
using Cassandra.Mapping;
using Coflnet.Sky.Auctions.Models;
using Coflnet.Sky.Auctions.Services;
using Coflnet.Sky.Core;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using NUnit.Framework;

namespace Coflnet.Sky.Auctions.Tests;

/// <summary>
/// Performs a full dry-run archive from ScyllaDB and MariaDB into a local S3-compatible store.
/// </summary>
[TestFixture]
public class S3RealDataDryRunTest
{
    private S3StorageService s3;
    private AuctionBlobSerializer serializer;
    private S3AuctionBlobService blobService;
    private S3PlayerIndexService playerIndex;
    private BloomFilterService bloom;
    private ILoggerFactory loggerFactory;

    private const int BufferFlushThreshold = 50_000;
    private const int MariaDbBatchSize = 2000;

    private string bucketName;
    private readonly List<(string uuid, string tag, DateTime month)> sampleAuctions = new();
    private readonly List<(Guid playerUuid, int year)> samplePlayers = new();
    private readonly HashSet<long> expectedAuctionIds = new();

    /// <summary>
    /// Creates the MinIO-backed services used by the real-data dry-run.
    /// </summary>
    [SetUp]
    public void Setup()
    {
        bucketName = $"sky-auctions-dryrun-{Guid.NewGuid().ToString("N")[..12]}";
        var endpoint = GetEnvironmentValue("http://localhost:9000", "S3__Endpoint", "MINIO_ENDPOINT");
        var accessKey = GetEnvironmentValue("minioadmin", "S3__AccessKey", "MINIO_ACCESS_KEY", "MINIO_ROOT_USER");
        var secretKey = GetEnvironmentValue("minioadmin", "S3__SecretKey", "MINIO_SECRET_KEY", "MINIO_ROOT_PASSWORD");
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string>
            {
                ["S3:Endpoint"] = endpoint,
                ["S3:AccessKey"] = accessKey,
                ["S3:SecretKey"] = secretKey,
                ["S3:BucketName"] = bucketName
            })
            .Build();

        loggerFactory = LoggerFactory.Create(b => b.AddConsole().SetMinimumLevel(LogLevel.Warning));
        s3 = new S3StorageService(config, loggerFactory.CreateLogger<S3StorageService>());
        serializer = new AuctionBlobSerializer();
        blobService = new S3AuctionBlobService(s3, serializer, loggerFactory.CreateLogger<S3AuctionBlobService>());
        playerIndex = new S3PlayerIndexService(s3, loggerFactory.CreateLogger<S3PlayerIndexService>());
        bloom = new BloomFilterService(s3, loggerFactory.CreateLogger<BloomFilterService>());
    }

    /// <summary>
    /// Backfills the last two years of ScyllaDB and MariaDB data to the configured S3-compatible endpoint and verifies coverage.
    /// </summary>
    [Test]
    public async Task BackfillLastTwoYearsAndVerify()
    {
        var sw = Stopwatch.StartNew();
        var ct = CancellationToken.None;
        var twoYearsAgo = DateTime.UtcNow.AddYears(-2);

        await s3.EnsureBucket(ct);
        await bloom.Initialize(expectedItems: 3_000_000, fpr: 0.01, ct);
        Console.WriteLine($"[{sw.Elapsed:mm\\:ss}] Bucket '{bucketName}' and Bloom filter ready");

        var totalAuctions = 0;
        var totalBlobsWritten = 0;
        var buffer = new Dictionary<(string tag, DateTime month), List<SaveAuction>>();

        // ============================================================
        // PHASE 1: ScyllaDB
        // ============================================================
        Console.WriteLine($"\n[{sw.Elapsed:mm\\:ss}] === PHASE 1: ScyllaDB ===");

        var cluster = Cluster.Builder()
            .AddContactPoint(GetEnvironmentValue("localhost", "SKYAUCTIONS_TEST_SCYLLA_HOST"))
            .WithCredentials(
                GetEnvironmentValue("cassandra", "SKYAUCTIONS_TEST_SCYLLA_USER"),
                GetEnvironmentValue("cassandra", "SKYAUCTIONS_TEST_SCYLLA_PASSWORD"))
            .WithCompression(CompressionType.LZ4)
            .WithQueryTimeout(30_000)
            .Build();

        using var session = await cluster.ConnectAsync(GetEnvironmentValue("sky_auctions", "SKYAUCTIONS_TEST_SCYLLA_KEYSPACE"));
        Console.WriteLine($"[{sw.Elapsed:mm\\:ss}] Connected to ScyllaDB");

        // Get all distinct partitions
        var distinctStmt = new SimpleStatement("SELECT DISTINCT tag, timekey FROM weekly_auctions_2")
            .SetPageSize(1000);
        var partitions = new List<(string tag, short timeKey)>();
        var rows = await session.ExecuteAsync(distinctStmt);
        foreach (var row in rows)
            partitions.Add((row.GetValue<string>("tag"), row.GetValue<short>("timekey")));

        Console.WriteLine($"[{sw.Elapsed:mm\\:ss}] Found {partitions.Count} partitions");

        var mapping = new MappingConfiguration()
            .Define(new Map<ScyllaAuction>()
            .PartitionKey(t => t.Tag, t => t.TimeKey)
            .ClusteringKey(
                new Tuple<string, SortOrder>("issold", SortOrder.Ascending),
                new("end", SortOrder.Descending),
                new("auctionuid", SortOrder.Descending))
            .Column(t => t.AuctionUid, cm => cm.WithSecondaryIndex())
            .Column(t => t.ItemUid, cm => cm.WithSecondaryIndex())
            .Column(t => t.Auctioneer, cm => cm.WithSecondaryIndex())
            .Column(t => t.HighestBidder, cm => cm.WithSecondaryIndex())
            .Column(t => t.Bids, cm => cm.Ignore())
        );
        var auctionsTable = new Table<ScyllaAuction>(session, mapping, "weekly_auctions_2");

        var scyllaCount = 0;
        var scyllaErrors = 0;

        foreach (var (tag, timeKey) in partitions)
        {
            try
            {
                var sold = (await auctionsTable.Where(a => a.Tag == tag && a.TimeKey == timeKey && a.IsSold).ExecuteAsync()).ToList();
                var unsold = (await auctionsTable.Where(a => a.Tag == tag && a.TimeKey == timeKey && !a.IsSold).ExecuteAsync()).ToList();
                var all = sold.Concat(unsold).ToList();
                if (all.Count == 0) continue;

                foreach (var scyllaRow in all)
                {
                    SaveAuction converted;
                    try
                    {
                        converted = ScyllaService.CassandraToOld(scyllaRow);
                    }
                    catch
                    {
                        scyllaErrors++;
                        continue;
                    }

                    if (converted.End < twoYearsAgo) continue;

                    var auctionId = AuctionIdentity.GetAuctionUid(converted);
                    if (auctionId == 0)
                    {
                        scyllaErrors++;
                        continue;
                    }

                    var month = new DateTime(converted.End.Year, converted.End.Month, 1);
                    var key = (converted.Tag ?? "unknown", month);
                    if (!buffer.TryGetValue(key, out var list))
                        buffer[key] = list = new List<SaveAuction>();
                    list.Add(converted);
                    expectedAuctionIds.Add(auctionId);
                    scyllaCount++;

                    if (sampleAuctions.Count < 3)
                        sampleAuctions.Add((converted.Uuid, converted.Tag ?? "unknown", month));
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  Warning: partition ({tag}, {timeKey}): {ex.Message}");
            }
        }

        Console.WriteLine($"[{sw.Elapsed:mm\\:ss}] ScyllaDB: {scyllaCount} auctions, {scyllaErrors} conversion errors");
        totalBlobsWritten += await FlushBuffer(buffer);
        totalAuctions += scyllaCount;

        // ============================================================
        // PHASE 2: MariaDB
        // ============================================================
        Console.WriteLine($"\n[{sw.Elapsed:mm\\:ss}] === PHASE 2: MariaDB ===");

        // Override HypixelContext connection string directly
        HypixelContext.DbContextId = GetEnvironmentValue(
            "server=localhost;port=3306;user=root;password=takenfrombitnami;database=test;default command timeout=300",
            "SKYAUCTIONS_TEST_MARIADB_CONNECTION");

        // Binary search for starting ID to avoid expensive full-scan query
        int startId;
        using (var ctx = new HypixelContext())
        {
            // Get approximate bounds: MAX(Id) and a rough estimate
            var maxId = await ctx.Auctions.MaxAsync(a => a.Id, ct);
            var lo = 0;
            var hi = maxId;
            while (lo < hi)
            {
                var mid = lo + (hi - lo) / 2;
                var sampleEnd = await ctx.Auctions
                    .Where(a => a.Id >= mid)
                    .OrderBy(a => a.Id)
                    .Select(a => a.End)
                    .FirstOrDefaultAsync(ct);
                if (sampleEnd >= twoYearsAgo)
                    hi = mid;
                else
                    lo = mid + 1;
            }
            startId = lo;
        }
        Console.WriteLine($"[{sw.Elapsed:mm\\:ss}] MariaDB start ID: {startId}");

        var mariaCount = 0;
        var lastId = startId - 1;
        var batchNum = 0;

        while (true)
        {
            List<SaveAuction> batch;
            using (var ctx = new HypixelContext())
            {
                batch = await ctx.Auctions
                    .Where(a => a.Id > lastId)
                    .OrderBy(a => a.Id)
                    .Take(MariaDbBatchSize)
                    .Include(a => a.Bids)
                    .Include(a => a.Enchantments)
                    .Include(a => a.NbtData)
                    .Include(a => a.NBTLookup)
                    .Include(a => a.CoopMembers)
                    .AsNoTracking()
                    .ToListAsync(ct);
            }

            if (batch.Count == 0) break;

            foreach (var auction in batch)
            {
                if (auction.End < twoYearsAgo) continue;

                var auctionId = AuctionIdentity.GetAuctionUid(auction);
                if (auctionId == 0)
                {
                    continue;
                }

                var month = new DateTime(auction.End.Year, auction.End.Month, 1);
                var key = (auction.Tag ?? "unknown", month);
                if (!buffer.TryGetValue(key, out var list))
                    buffer[key] = list = new List<SaveAuction>();
                list.Add(auction);
                expectedAuctionIds.Add(auctionId);
                mariaCount++;

                // Track samples
                if (sampleAuctions.Count < 10 && mariaCount % 50_000 == 1)
                    sampleAuctions.Add((auction.Uuid, auction.Tag ?? "unknown", month));
                if (samplePlayers.Count < 5 && mariaCount % 100_000 == 1)
                {
                    if (AuctionIdentity.TryParseGuid(auction.AuctioneerId, out var seller) && seller != Guid.Empty)
                        samplePlayers.Add((seller, auction.End.Year));
                }
            }

            lastId = batch.Max(a => a.Id);
            batchNum++;

            // Flush when buffer is large
            var bufferSize = buffer.Values.Sum(l => l.Count);
            if (bufferSize >= BufferFlushThreshold)
            {
                totalBlobsWritten += await FlushBuffer(buffer);
                totalAuctions += bufferSize;
                Console.WriteLine($"[{sw.Elapsed:mm\\:ss}] MariaDB: {mariaCount:N0} rows, lastId={lastId}, total blobs={totalBlobsWritten}");
            }

            if (batchNum % 250 == 0)
                Console.WriteLine($"[{sw.Elapsed:mm\\:ss}]   batch {batchNum}, {mariaCount:N0} rows, lastId={lastId}");
        }

        // Final flush
        var remaining = buffer.Values.Sum(l => l.Count);
        if (remaining > 0)
        {
            totalBlobsWritten += await FlushBuffer(buffer);
            totalAuctions += remaining;
        }

        Console.WriteLine($"[{sw.Elapsed:mm\\:ss}] MariaDB: {mariaCount:N0} total rows");

        // ============================================================
        // PHASE 3: Finalize
        // ============================================================
        Console.WriteLine($"\n[{sw.Elapsed:mm\\:ss}] === PHASE 3: Finalize ===");
        await bloom.FlushIfDirty(ct);
        await playerIndex.FlushAll(ct);
        Console.WriteLine($"[{sw.Elapsed:mm\\:ss}] Bloom filter and player index flushed");

        // ============================================================
        // PHASE 4: Verification
        // ============================================================
        Console.WriteLine($"\n[{sw.Elapsed:mm\\:ss}] === PHASE 4: Verification ===");

        var coverage = await VerifyArchiveCoverage(ct);

        var verifiedBlobs = 0;
        foreach (var (uuid, tag, month) in sampleAuctions.Take(5))
        {
            try
            {
                var auctions = await blobService.ReadAuctions(tag, month, ct);
                var found = auctions.FirstOrDefault(a => a.Uuid == uuid);
                if (found != null)
                {
                    verifiedBlobs++;
                    Console.WriteLine($"  [read] {tag}/{month:yyyy-MM}: found {uuid[..8]}... bid={found.HighestBidAmount}");
                }
                else
                {
                    Console.WriteLine($"  [miss] {uuid[..8]}... not in blob {tag}/{month:yyyy-MM} ({auctions.Count} auctions)");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  [err]  {tag}/{month:yyyy-MM}: {ex.Message}");
            }
        }

        // Bloom filter verification
        var bloomHits = sampleAuctions.Count(s => AuctionIdentity.TryParseGuid(s.uuid, out var guid) && bloom.MightExist(guid));
        var randomFalse = !bloom.MightExist(Guid.NewGuid());
        Console.WriteLine($"  Bloom: {bloomHits}/{sampleAuctions.Count} hits, random UUID absent={randomFalse}");

        // Player index verification
        var playerVerified = 0;
        foreach (var (playerUuid, year) in samplePlayers.Take(3))
        {
            try
            {
                var entries = await playerIndex.GetParticipation(playerUuid, year, ct);
                if (entries.Count > 0)
                {
                    playerVerified++;
                    Console.WriteLine($"  Player {playerUuid.ToString("N")[..8]}...: {entries.Count} entries in {year}");
                }
            }
            catch { /* player might not exist in this year */ }
        }

        // S3 object counts
        var auctionBlobs = await s3.ListBlobs("auctions/", ct);
        var playerBlobs = await s3.ListBlobs("players/", ct);

        Console.WriteLine($"\n[{sw.Elapsed:mm\\:ss}] === SUMMARY ===");
        Console.WriteLine($"  Auctions: {totalAuctions:N0} (ScyllaDB: {scyllaCount:N0}, MariaDB: {mariaCount:N0})");
        Console.WriteLine($"  S3 auction blobs: {auctionBlobs.Count}");
        Console.WriteLine($"  S3 player blobs:  {playerBlobs.Count}");
        Console.WriteLine($"  Archive coverage: missing={coverage.MissingCount}, duplicates/extras={coverage.ExtraOrDuplicateCount}, records={coverage.RecordCount}");
        Console.WriteLine($"  Verified reads:   {verifiedBlobs}/{Math.Min(5, sampleAuctions.Count)}");
        Console.WriteLine($"  Bloom hits:       {bloomHits}/{sampleAuctions.Count}");
        Console.WriteLine($"  Player verified:  {playerVerified}/{Math.Min(3, samplePlayers.Count)}");
        Console.WriteLine($"  Time:             {sw.Elapsed:hh\\:mm\\:ss}");
        Console.WriteLine($"\n=== REAL DATA DRY-RUN COMPLETE ===");

        Assert.That(totalAuctions, Is.GreaterThan(0));
        Assert.That(auctionBlobs.Count, Is.GreaterThan(0));
        Assert.That(coverage.MissingCount, Is.EqualTo(0));
        Assert.That(coverage.ExtraOrDuplicateCount, Is.EqualTo(0));
        Assert.That(verifiedBlobs, Is.GreaterThan(0));
        Assert.That(bloomHits, Is.EqualTo(sampleAuctions.Count));
    }

    private async Task<int> FlushBuffer(Dictionary<(string tag, DateTime month), List<SaveAuction>> buffer)
    {
        var blobCount = 0;
        foreach (var ((tag, month), auctions) in buffer)
        {
            await blobService.WriteAuctions(tag, month, auctions);

            foreach (var a in auctions)
            {
                if (Guid.TryParse(a.Uuid, out var uuid))
                {
                    bloom.Add(uuid);
                    AddPlayerParticipation(a, uuid);
                }
            }
            blobCount++;
        }

        await playerIndex.FlushAll();
        buffer.Clear();
        return blobCount;
    }

    private async Task<(int RecordCount, int ExtraOrDuplicateCount, int MissingCount)> VerifyArchiveCoverage(CancellationToken ct)
    {
        var remaining = new HashSet<long>(expectedAuctionIds);
        var extraOrDuplicateCount = 0;
        var recordCount = 0;
        var auctionBlobs = await s3.ListBlobs("auctions/", ct);

        foreach (var blobKey in auctionBlobs)
        {
            if (!TryParseAuctionBlobKey(blobKey, out var tag, out var month))
            {
                continue;
            }

            var auctions = await blobService.ReadAuctions(tag, month, ct);
            foreach (var auction in auctions)
            {
                var auctionId = AuctionIdentity.GetAuctionUid(auction);
                if (auctionId == 0)
                {
                    extraOrDuplicateCount++;
                    continue;
                }

                recordCount++;
                if (!remaining.Remove(auctionId))
                {
                    extraOrDuplicateCount++;
                }
            }
        }

        return (recordCount, extraOrDuplicateCount, remaining.Count);
    }

    private static bool TryParseAuctionBlobKey(string blobKey, out string tag, out DateTime month)
    {
        tag = null;
        month = default;

        var parts = blobKey.Split('/', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length != 3 || !string.Equals(parts[0], "auctions", StringComparison.Ordinal))
        {
            return false;
        }

        var monthText = parts[2].Replace(".json.gz", string.Empty, StringComparison.OrdinalIgnoreCase);
        if (!DateTime.TryParseExact(monthText, "yyyy-MM", null, System.Globalization.DateTimeStyles.AssumeUniversal, out month))
        {
            return false;
        }

        month = new DateTime(month.Year, month.Month, 1, 0, 0, 0, DateTimeKind.Utc);
        tag = parts[1];
        return true;
    }

    private void AddPlayerParticipation(SaveAuction a, Guid auctionUuid)
    {
        if (Guid.TryParse(a.AuctioneerId, out var seller) && seller != Guid.Empty)
        {
            playerIndex.AddParticipations(seller, new[]
            {
                new PlayerParticipationEntry
                {
                    AuctionUid = auctionUuid, End = a.End, Tag = a.Tag, Type = ParticipationType.Seller
                }
            });
        }
        foreach (var bid in a.Bids ?? new())
        {
            if (Guid.TryParse(bid.Bidder, out var bidder) && bidder != Guid.Empty)
            {
                playerIndex.AddParticipations(bidder, new[]
                {
                    new PlayerParticipationEntry
                    {
                        AuctionUid = auctionUuid, End = a.End, Tag = a.Tag, Type = ParticipationType.Bidder
                    }
                });
            }
        }
    }

    private static string GetEnvironmentValue(string defaultValue, params string[] keys)
    {
        foreach (var key in keys)
        {
            var value = Environment.GetEnvironmentVariable(key);
            if (!string.IsNullOrWhiteSpace(value))
            {
                return value;
            }
        }

        return defaultValue;
    }
}
