using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
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
/// Repairs an existing MinIO archive bucket by backfilling only records that are missing from the bucket.
/// Set SKYAUCTIONS_TEST_BUCKET to the bucket name to repair.
/// </summary>
[TestFixture]
public class S3ExistingBucketRepairTest
{
    private S3StorageService s3;
    private S3AuctionBlobService blobService;
    private S3PlayerIndexService playerIndex;
    private BloomFilterService bloom;

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
        playerIndex = new S3PlayerIndexService(s3, loggerFactory.CreateLogger<S3PlayerIndexService>());
        bloom = new BloomFilterService(s3, loggerFactory.CreateLogger<BloomFilterService>());
    }

    [Test]
    public async Task RepairExistingBucketCoverage()
    {
        var sw = Stopwatch.StartNew();
        var ct = CancellationToken.None;
        var twoYearsAgo = DateTime.UtcNow.AddYears(-2);

        Console.WriteLine($"[{sw.Elapsed:mm\\:ss}] Loading existing bucket for repair");
        await bloom.Initialize(expectedItems: 3_000_000, fpr: 0.01, ct);

        var archiveIndex = await LoadArchiveIndex(ct, sw);
        var missingScylla = await FindMissingScyllaPartitions(archiveIndex.AuctionIds, twoYearsAgo, ct, sw);
        var missingMaria = await FindMissingMariaIds(archiveIndex.AuctionIds, twoYearsAgo, ct, sw);

        Console.WriteLine($"[{sw.Elapsed:mm\\:ss}] Repair plan");
        Console.WriteLine($"  Scylla partitions to repair: {missingScylla.Partitions.Count:N0}");
        Console.WriteLine($"  MariaDB auctions to repair:  {missingMaria.Ids.Count:N0}");
        if (missingMaria.Ids.Count > 0)
        {
            Console.WriteLine($"  MariaDB missing id range:    {missingMaria.FirstId:N0}..{missingMaria.LastId:N0}");
        }

        await RepairScyllaPartitions(missingScylla.Partitions, twoYearsAgo, ct, sw);
        await RepairMariaAuctions(missingMaria.Ids, ct, sw);

        await playerIndex.FlushAll(ct);
        await bloom.FlushIfDirty(ct);

        Console.WriteLine($"[{sw.Elapsed:mm\\:ss}] Repair complete");
        Console.WriteLine($"  Archive records before repair: {archiveIndex.RecordCount:N0}");
        Console.WriteLine($"  Duplicate/extra records seen:  {archiveIndex.DuplicateOrExtraCount:N0}");
        Console.WriteLine($"  Invalid archive ids seen:      {archiveIndex.InvalidCount:N0}");
        Console.WriteLine($"  Time:                          {sw.Elapsed:hh\\:mm\\:ss}");
    }

    private async Task<ArchiveIndex> LoadArchiveIndex(CancellationToken ct, Stopwatch sw)
    {
        var auctionIds = new ConcurrentDictionary<long, byte>();
        var recordCount = 0;
        var duplicateOrExtraCount = 0;
        var invalidCount = 0;
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

                if (auctionUid == 0)
                {
                    Interlocked.Increment(ref invalidCount);
                    Interlocked.Increment(ref duplicateOrExtraCount);
                    continue;
                }

                if (!auctionIds.TryAdd(auctionUid, 0))
                {
                    Interlocked.Increment(ref duplicateOrExtraCount);
                }
            }
        });

        Console.WriteLine($"[{sw.Elapsed:mm\\:ss}] Indexed {recordCount:N0} archive records across {blobKeys.Count:N0} blobs ({auctionIds.Count:N0} unique ids)");
        return new ArchiveIndex(auctionIds, recordCount, duplicateOrExtraCount, invalidCount);
    }

    private async Task<MissingScyllaReport> FindMissingScyllaPartitions(ConcurrentDictionary<long, byte> archiveIds, DateTime twoYearsAgo, CancellationToken ct, Stopwatch sw)
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

        var missingPartitions = new Dictionary<(string Tag, short TimeKey), int>();
        var checkedRows = 0;
        var missingRows = 0;

        foreach (var (tag, timeKey) in partitions)
        {
            var statement = new SimpleStatement(
                "SELECT auctionuid, uuid, end FROM weekly_auctions_2 WHERE tag = ? AND timekey = ?",
                tag,
                timeKey)
                .SetPageSize(5000);

            var rows = await session.ExecuteAsync(statement);
            var partitionMissing = 0;
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

                    auctionUid = AuctionService.Instance.GetId(guid.ToString("N"));
                }

                if (auctionUid == 0)
                {
                    continue;
                }

                checkedRows++;
                if (!archiveIds.ContainsKey(auctionUid))
                {
                    partitionMissing++;
                    missingRows++;
                }
            }

            if (partitionMissing > 0)
            {
                missingPartitions[(tag, timeKey)] = partitionMissing;
            }
        }

        Console.WriteLine($"[{sw.Elapsed:mm\\:ss}] Scylla gap scan: {missingRows:N0} missing rows across {missingPartitions.Count:N0} partitions from {checkedRows:N0} checked rows");
        return new MissingScyllaReport(missingPartitions, missingRows);
    }

    private async Task<MissingMariaReport> FindMissingMariaIds(ConcurrentDictionary<long, byte> archiveIds, DateTime twoYearsAgo, CancellationToken ct, Stopwatch sw)
    {
        HypixelContext.DbContextId = GetEnvironmentValue(
            "server=localhost;port=3306;user=root;password=takenfrombitnami;database=test;default command timeout=300",
            "SKYAUCTIONS_TEST_MARIADB_CONNECTION");

        var startId = await FindMariaDbStartId(twoYearsAgo, ct);
        var lastId = startId - 1;
        const int batchSize = 20_000;
        var missingIds = new List<int>();
        var missingByMonth = new Dictionary<DateTime, int>();
        var checkedRows = 0;
        var firstMissingId = 0;
        var lastMissingId = 0;

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
                        ? AuctionService.Instance.GetId(guid.ToString("N"))
                        : 0;

                if (auctionUid == 0)
                {
                    continue;
                }

                checkedRows++;
                if (!archiveIds.ContainsKey(auctionUid))
                {
                    missingIds.Add(auction.Id);
                    firstMissingId = firstMissingId == 0 ? auction.Id : Math.Min(firstMissingId, auction.Id);
                    lastMissingId = Math.Max(lastMissingId, auction.Id);

                    var month = new DateTime(auction.End.Year, auction.End.Month, 1, 0, 0, 0, DateTimeKind.Utc);
                    missingByMonth[month] = missingByMonth.TryGetValue(month, out var count) ? count + 1 : 1;
                }
            }

            lastId = batch[^1].Id;
        }

        Console.WriteLine($"[{sw.Elapsed:mm\\:ss}] MariaDB gap scan: {missingIds.Count:N0} missing auctions from {checkedRows:N0} checked rows (id {startId:N0}+)");
        foreach (var month in missingByMonth.OrderByDescending(entry => entry.Value).Take(6))
        {
            Console.WriteLine($"  Missing month {month.Key:yyyy-MM}: {month.Value:N0}");
        }

        return new MissingMariaReport(missingIds, firstMissingId, lastMissingId, missingByMonth);
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

    private async Task RepairScyllaPartitions(Dictionary<(string Tag, short TimeKey), int> partitions, DateTime twoYearsAgo, CancellationToken ct, Stopwatch sw)
    {
        if (partitions.Count == 0)
        {
            return;
        }

        var cluster = Cluster.Builder()
            .AddContactPoint(GetEnvironmentValue("localhost", "SKYAUCTIONS_TEST_SCYLLA_HOST"))
            .WithCredentials(
                GetEnvironmentValue("cassandra", "SKYAUCTIONS_TEST_SCYLLA_USER"),
                GetEnvironmentValue("cassandra", "SKYAUCTIONS_TEST_SCYLLA_PASSWORD"))
            .WithCompression(CompressionType.LZ4)
            .WithQueryTimeout(30_000)
            .Build();

        using var session = await cluster.ConnectAsync(GetEnvironmentValue("sky_auctions", "SKYAUCTIONS_TEST_SCYLLA_KEYSPACE"));
        var mapping = new MappingConfiguration()
            .Define(new Map<ScyllaAuction>()
            .PartitionKey(t => t.Tag, t => t.TimeKey)
            .ClusteringKey(
                new Tuple<string, SortOrder>("issold", SortOrder.Ascending),
                new Tuple<string, SortOrder>("end", SortOrder.Descending),
                new Tuple<string, SortOrder>("auctionuid", SortOrder.Descending))
            .Column(t => t.AuctionUid, cm => cm.WithSecondaryIndex())
            .Column(t => t.ItemUid, cm => cm.WithSecondaryIndex())
            .Column(t => t.Auctioneer, cm => cm.WithSecondaryIndex())
            .Column(t => t.HighestBidder, cm => cm.WithSecondaryIndex())
            .Column(t => t.Bids, cm => cm.Ignore())
        );
        var table = new Table<ScyllaAuction>(session, mapping, "weekly_auctions_2");

        var repairedAuctions = 0;
        var repairedPartitions = 0;
        foreach (var partition in partitions.Keys.OrderBy(p => p.TimeKey).ThenBy(p => p.Tag))
        {
            var sold = (await table.Where(a => a.Tag == partition.Tag && a.TimeKey == partition.TimeKey && a.IsSold).ExecuteAsync()).ToList();
            var unsold = (await table.Where(a => a.Tag == partition.Tag && a.TimeKey == partition.TimeKey && !a.IsSold).ExecuteAsync()).ToList();
            sold.AddRange(unsold);

            var converted = sold
                .Select(ScyllaService.CassandraToOld)
                .Where(a => a.End >= twoYearsAgo)
                .ToList();

            foreach (var monthGroup in converted.GroupBy(a => new DateTime(a.End.Year, a.End.Month, 1, 0, 0, 0, DateTimeKind.Utc)))
            {
                await blobService.WriteAuctions(partition.Tag, monthGroup.Key, monthGroup, ct);
            }

            foreach (var auction in converted)
            {
                if (!AuctionIdentity.TryParseGuid(auction.Uuid, out var auctionUuid))
                {
                    continue;
                }

                bloom.Add(auctionUuid);
                AddPlayerParticipation(auction, auctionUuid);
                repairedAuctions++;
            }

            repairedPartitions++;
            if (repairedPartitions % 50 == 0)
            {
                await playerIndex.FlushAll(ct);
                Console.WriteLine($"[{sw.Elapsed:mm\\:ss}] Repaired {repairedPartitions:N0}/{partitions.Count:N0} Scylla partitions");
            }
        }

        if (repairedPartitions > 0)
        {
            Console.WriteLine($"[{sw.Elapsed:mm\\:ss}] Repaired {repairedPartitions:N0} Scylla partitions ({repairedAuctions:N0} auctions written)");
        }
    }

    private async Task RepairMariaAuctions(List<int> missingIds, CancellationToken ct, Stopwatch sw)
    {
        if (missingIds.Count == 0)
        {
            return;
        }

        HypixelContext.DbContextId = GetEnvironmentValue(
            "server=localhost;port=3306;user=root;password=takenfrombitnami;database=test;default command timeout=300",
            "SKYAUCTIONS_TEST_MARIADB_CONNECTION");

        const int fetchBatchSize = 1000;
        var repairedAuctions = 0;
        var processedBatches = 0;

        for (var index = 0; index < missingIds.Count; index += fetchBatchSize)
        {
            var batchIds = missingIds.Skip(index).Take(fetchBatchSize).ToArray();
            List<SaveAuction> auctions;
            using (var context = new HypixelContext())
            {
                auctions = await context.Auctions
                    .Where(a => batchIds.Contains(a.Id))
                    .Include(a => a.Bids)
                    .Include(a => a.Enchantments)
                    .Include(a => a.NbtData)
                    .Include(a => a.NBTLookup)
                    .Include(a => a.CoopMembers)
                    .AsNoTracking()
                    .ToListAsync(ct);
            }

            foreach (var group in auctions.GroupBy(a => (Tag: a.Tag ?? "unknown", Month: new DateTime(a.End.Year, a.End.Month, 1, 0, 0, 0, DateTimeKind.Utc))))
            {
                await blobService.WriteAuctions(group.Key.Tag, group.Key.Month, group, ct);
            }

            foreach (var auction in auctions)
            {
                if (!AuctionIdentity.TryParseGuid(auction.Uuid, out var auctionUuid))
                {
                    continue;
                }

                bloom.Add(auctionUuid);
                AddPlayerParticipation(auction, auctionUuid);
                repairedAuctions++;
            }

            processedBatches++;
            if (processedBatches % 10 == 0)
            {
                await playerIndex.FlushAll(ct);
                Console.WriteLine($"[{sw.Elapsed:mm\\:ss}] Repaired {Math.Min(index + fetchBatchSize, missingIds.Count):N0}/{missingIds.Count:N0} missing MariaDB auctions");
            }
        }

        Console.WriteLine($"[{sw.Elapsed:mm\\:ss}] Repaired {repairedAuctions:N0} MariaDB auctions");
    }

    private void AddPlayerParticipation(SaveAuction auction, Guid auctionUuid)
    {
        var sellerGuid = AuctionIdentity.ParseGuidOrEmpty(auction.AuctioneerId);
        if (sellerGuid != Guid.Empty)
        {
            playerIndex.AddParticipations(sellerGuid, new[]
            {
                new PlayerParticipationEntry
                {
                    AuctionUid = auctionUuid,
                    End = auction.End,
                    Tag = auction.Tag,
                    Type = ParticipationType.Seller
                }
            });
        }

        foreach (var bid in auction.Bids ?? new List<SaveBids>())
        {
            var bidderGuid = AuctionIdentity.ParseGuidOrEmpty(bid.Bidder);
            if (bidderGuid == Guid.Empty)
            {
                continue;
            }

            playerIndex.AddParticipations(bidderGuid, new[]
            {
                new PlayerParticipationEntry
                {
                    AuctionUid = auctionUuid,
                    End = auction.End,
                    Tag = auction.Tag,
                    Type = ParticipationType.Bidder
                }
            });
        }
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

    private sealed record ArchiveIndex(ConcurrentDictionary<long, byte> AuctionIds, int RecordCount, int DuplicateOrExtraCount, int InvalidCount);

    private sealed record MissingScyllaReport(Dictionary<(string Tag, short TimeKey), int> Partitions, int MissingRows);

    private sealed record MissingMariaReport(List<int> Ids, int FirstId, int LastId, Dictionary<DateTime, int> MissingByMonth);

    private sealed class MariaAuctionProjection
    {
        public int Id { get; init; }
        public long UId { get; init; }
        public string Uuid { get; init; }
        public string Tag { get; init; }
        public DateTime End { get; init; }
    }
}