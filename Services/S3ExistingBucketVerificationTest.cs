using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using Coflnet.Sky.Auctions.Services;
using Coflnet.Sky.Core;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using NUnit.Framework;

namespace Coflnet.Sky.Auctions.Tests;

/// <summary>
/// Verifies an existing MinIO archive bucket against ScyllaDB and MariaDB without re-running the full backfill.
/// Set SKYAUCTIONS_TEST_BUCKET to the bucket name to verify.
/// </summary>
[TestFixture]
public class S3ExistingBucketVerificationTest
{
    private S3StorageService s3;
    private S3AuctionBlobService blobService;

    /// <summary>
    /// Creates the archive services used to verify an existing bucket.
    /// </summary>
    [SetUp]
    public void Setup()
    {
        var bucketName = GetRequiredEnvironmentValue("SKYAUCTIONS_TEST_BUCKET");
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

        var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Warning));
        s3 = new S3StorageService(config, loggerFactory.CreateLogger<S3StorageService>());
        blobService = new S3AuctionBlobService(s3, new AuctionBlobSerializer(), loggerFactory.CreateLogger<S3AuctionBlobService>());
    }

    /// <summary>
    /// Verifies that every auction from the last two years in ScyllaDB and MariaDB exists exactly once in the configured archive bucket.
    /// </summary>
    [Test]
    public async Task VerifyExistingBucketCoverage()
    {
        var sw = Stopwatch.StartNew();
        var ct = CancellationToken.None;
        var twoYearsAgo = DateTime.UtcNow.AddYears(-2);
        var expectedAuctionIds = new HashSet<long>();
        var sampleAuctions = new List<(string Uuid, string Tag, DateTime Month)>();

        Console.WriteLine($"[{sw.Elapsed:mm\\:ss}] Verifying existing bucket");

        await LoadScyllaExpectations(expectedAuctionIds, sampleAuctions, twoYearsAgo, ct, sw);
        await LoadMariaDbExpectations(expectedAuctionIds, sampleAuctions, twoYearsAgo, ct, sw);

        var archiveCoverage = await VerifyArchiveCoverage(expectedAuctionIds, ct, sw);
        var sampleHits = await VerifySampleReads(sampleAuctions, ct, sw);

        Console.WriteLine($"[{sw.Elapsed:mm\\:ss}] Existing bucket summary");
        Console.WriteLine($"  Expected auctions: {expectedAuctionIds.Count:N0}");
        Console.WriteLine($"  Archived records:  {archiveCoverage.RecordCount:N0}");
        Console.WriteLine($"  Missing:           {archiveCoverage.MissingCount:N0}");
        Console.WriteLine($"  Duplicates/extras: {archiveCoverage.ExtraOrDuplicateCount:N0}");
        Console.WriteLine($"  Sample hits:       {sampleHits}/{Math.Min(5, sampleAuctions.Count)}");
        Console.WriteLine($"  Time:              {sw.Elapsed:hh\\:mm\\:ss}");

        Assert.That(archiveCoverage.MissingCount, Is.EqualTo(0));
        Assert.That(archiveCoverage.ExtraOrDuplicateCount, Is.EqualTo(0));
        Assert.That(sampleHits, Is.EqualTo(Math.Min(5, sampleAuctions.Count)));
    }

    private async Task LoadScyllaExpectations(HashSet<long> expectedAuctionIds, List<(string Uuid, string Tag, DateTime Month)> sampleAuctions, DateTime twoYearsAgo, CancellationToken ct, Stopwatch sw)
    {
        var cluster = Cluster.Builder()
            .AddContactPoint(GetEnvironmentValue("localhost", "SKYAUCTIONS_TEST_SCYLLA_HOST"))
            .WithCredentials(
                GetEnvironmentValue("cassandra", "SKYAUCTIONS_TEST_SCYLLA_USER"),
                GetEnvironmentValue("cassandra", "SKYAUCTIONS_TEST_SCYLLA_PASSWORD"))
            .WithCompression(CompressionType.LZ4)
            .WithQueryTimeout(30_000)
            .Build();

        using var session = await cluster.ConnectAsync(GetEnvironmentValue("sky_auctions", "SKYAUCTIONS_TEST_SCYLLA_KEYSPACE"));
        var partitions = new List<(string Tag, short TimeKey)>();
        var partitionStatement = new SimpleStatement("SELECT DISTINCT tag, timekey FROM weekly_auctions_2").SetPageSize(1000);
        var partitionRows = await session.ExecuteAsync(partitionStatement);
        foreach (var row in partitionRows)
        {
            partitions.Add((row.GetValue<string>("tag"), row.GetValue<short>("timekey")));
        }

        var scyllaCount = 0;
        foreach (var (tag, timeKey) in partitions)
        {
            var statement = new SimpleStatement(
                "SELECT auctionuid, uuid, end FROM weekly_auctions_2 WHERE tag = ? AND timekey = ?",
                tag,
                timeKey)
                .SetPageSize(5000);

            var rows = await session.ExecuteAsync(statement);
            foreach (var row in rows)
            {
                var end = row.GetValue<DateTime>("end");
                if (end < twoYearsAgo)
                {
                    continue;
                }

                var auctionUid = row.GetValue<long>("auctionuid");
                if (auctionUid == 0)
                {
                    var uuid = ReadRowUuid(row);
                    if (!AuctionIdentity.TryParseGuid(uuid, out var guid))
                    {
                        continue;
                    }

                    auctionUid = Coflnet.Sky.Core.AuctionService.Instance.GetId(guid.ToString("N"));
                }

                if (auctionUid == 0)
                {
                    continue;
                }

                expectedAuctionIds.Add(auctionUid);
                scyllaCount++;
                if (sampleAuctions.Count < 5)
                {
                    var uuid = ReadRowUuid(row);
                    if (!string.IsNullOrWhiteSpace(uuid))
                    {
                        sampleAuctions.Add((uuid, tag ?? "unknown", new DateTime(end.Year, end.Month, 1, 0, 0, 0, DateTimeKind.Utc)));
                    }
                }
            }
        }

        Console.WriteLine($"[{sw.Elapsed:mm\\:ss}] Loaded Scylla expectations: {scyllaCount:N0} auction versions across {partitions.Count:N0} partitions");
    }

    private async Task LoadMariaDbExpectations(HashSet<long> expectedAuctionIds, List<(string Uuid, string Tag, DateTime Month)> sampleAuctions, DateTime twoYearsAgo, CancellationToken ct, Stopwatch sw)
    {
        HypixelContext.DbContextId = GetEnvironmentValue(
            "server=localhost;port=3306;user=root;password=takenfrombitnami;database=test;default command timeout=300",
            "SKYAUCTIONS_TEST_MARIADB_CONNECTION");

        var startId = await FindMariaDbStartId(twoYearsAgo, ct);
        var lastId = startId - 1;
        var mariaCount = 0;
        const int batchSize = 20_000;

        while (!ct.IsCancellationRequested)
        {
            List<MariaAuctionProjection> batch;
            using (var context = new HypixelContext())
            {
                batch = await context.Auctions
                    .Where(a => a.Id > lastId)
                    .OrderBy(a => a.Id)
                    .Take(batchSize)
                    .Select(a => new MariaAuctionProjection
                    {
                        Id = a.Id,
                        UId = a.UId,
                        Uuid = a.Uuid,
                        Tag = a.Tag,
                        End = a.End
                    })
                    .AsNoTracking()
                    .ToListAsync(ct);
            }

            if (batch.Count == 0)
            {
                break;
            }

            foreach (var auction in batch)
            {
                if (auction.End < twoYearsAgo)
                {
                    continue;
                }

                var auctionUid = auction.UId != 0
                    ? auction.UId
                    : AuctionIdentity.TryParseGuid(auction.Uuid, out var guid)
                        ? Coflnet.Sky.Core.AuctionService.Instance.GetId(guid.ToString("N"))
                        : 0;

                if (auctionUid == 0)
                {
                    continue;
                }

                expectedAuctionIds.Add(auctionUid);
                mariaCount++;
                if (sampleAuctions.Count < 10 && !string.IsNullOrWhiteSpace(auction.Uuid))
                {
                    sampleAuctions.Add((auction.Uuid, auction.Tag ?? "unknown", new DateTime(auction.End.Year, auction.End.Month, 1, 0, 0, 0, DateTimeKind.Utc)));
                }
            }

            lastId = batch[^1].Id;
        }

        Console.WriteLine($"[{sw.Elapsed:mm\\:ss}] Loaded MariaDB expectations: {mariaCount:N0} auctions from id {startId:N0} onward");
    }

    private async Task<int> FindMariaDbStartId(DateTime twoYearsAgo, CancellationToken ct)
    {
        using var context = new HypixelContext();
        var maxId = await context.Auctions.MaxAsync(a => a.Id, ct);
        var low = 0;
        var high = maxId;

        while (low < high)
        {
            var mid = low + (high - low) / 2;
            var sampleEnd = await context.Auctions
                .Where(a => a.Id >= mid)
                .OrderBy(a => a.Id)
                .Select(a => a.End)
                .FirstOrDefaultAsync(ct);

            if (sampleEnd >= twoYearsAgo)
            {
                high = mid;
            }
            else
            {
                low = mid + 1;
            }
        }

        return low;
    }

    private async Task<(int RecordCount, int ExtraOrDuplicateCount, int MissingCount)> VerifyArchiveCoverage(HashSet<long> expectedAuctionIds, CancellationToken ct, Stopwatch sw)
    {
        var remaining = new ConcurrentDictionary<long, byte>(expectedAuctionIds.Select(id => new KeyValuePair<long, byte>(id, 0)));
        var recordCount = 0;
        var extraOrDuplicateCount = 0;
        var blobKeys = await s3.ListBlobs("auctions/", ct);

        var options = new ParallelOptions
        {
            CancellationToken = ct,
            MaxDegreeOfParallelism = 8
        };

        await Parallel.ForEachAsync(blobKeys, options, async (blobKey, token) =>
        {
            if (!TryParseAuctionBlobKey(blobKey, out var tag, out var month))
            {
                return;
            }

            var auctions = await blobService.ReadAuctions(tag, month, token);
            foreach (var auction in auctions)
            {
                var auctionUid = AuctionIdentity.GetAuctionUid(auction);
                Interlocked.Increment(ref recordCount);
                if (auctionUid == 0 || !remaining.TryRemove(auctionUid, out _))
                {
                    Interlocked.Increment(ref extraOrDuplicateCount);
                }
            }
        });

        Console.WriteLine($"[{sw.Elapsed:mm\\:ss}] Verified {blobKeys.Count:N0} auction blobs");
        return (recordCount, extraOrDuplicateCount, remaining.Count);
    }

    private async Task<int> VerifySampleReads(List<(string Uuid, string Tag, DateTime Month)> sampleAuctions, CancellationToken ct, Stopwatch sw)
    {
        var hits = 0;
        foreach (var sample in sampleAuctions.Take(5))
        {
            var auctions = await blobService.ReadAuctions(sample.Tag, sample.Month, ct);
            if (auctions.Any(a => string.Equals(AuctionIdentity.NormalizeUuid(a.Uuid), AuctionIdentity.NormalizeUuid(sample.Uuid), StringComparison.OrdinalIgnoreCase)))
            {
                hits++;
            }
        }

        Console.WriteLine($"[{sw.Elapsed:mm\\:ss}] Verified {hits}/{Math.Min(5, sampleAuctions.Count)} sample reads");
        return hits;
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
        if (!DateTime.TryParseExact(monthText, "yyyy-MM", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out month))
        {
            return false;
        }

        tag = parts[1];
        month = new DateTime(month.Year, month.Month, 1, 0, 0, 0, DateTimeKind.Utc);
        return true;
    }

    private static string ReadRowUuid(Row row)
    {
        try
        {
            return row.GetValue<Guid>("uuid").ToString("N");
        }
        catch (InvalidCastException)
        {
            return row.GetValue<string>("uuid");
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

    private static string GetRequiredEnvironmentValue(string key)
    {
        var value = Environment.GetEnvironmentVariable(key);
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new InvalidOperationException($"Environment variable '{key}' must be set");
        }

        return value;
    }

    private sealed class MariaAuctionProjection
    {
        public int Id { get; init; }
        public long UId { get; init; }
        public string Uuid { get; init; }
        public string Tag { get; init; }
        public DateTime End { get; init; }
    }
}