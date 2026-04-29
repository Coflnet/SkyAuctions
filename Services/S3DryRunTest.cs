using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Coflnet.Sky.Auctions.Models;
using Coflnet.Sky.Auctions.Services;
using Coflnet.Sky.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using NUnit.Framework;

namespace Coflnet.Sky.Auctions.Tests;

/// <summary>
/// Dry-run integration test against a local MinIO instance.
/// Uses environment overrides when present and otherwise falls back to localhost defaults.
/// Start with: docker run -d --name minio -p 9000:9000 -p 9001:9001 minio/minio server /data --console-address ":9001"
/// </summary>
[TestFixture]
public class S3DryRunTest
{
    private S3StorageService s3;
    private AuctionBlobSerializer serializer;
    private S3AuctionBlobService blobService;
    private S3PlayerIndexService playerIndex;
    private BloomFilterService bloom;
    private string bucketName;

    /// <summary>
    /// Creates the MinIO-backed services used by the synthetic dry-run.
    /// </summary>
    [SetUp]
    public void Setup()
    {
        bucketName = $"sky-auctions-test-{Guid.NewGuid().ToString("N")[..12]}";
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

        var loggerFactory = LoggerFactory.Create(b => b.AddConsole());
        s3 = new S3StorageService(config, loggerFactory.CreateLogger<S3StorageService>());
        serializer = new AuctionBlobSerializer();
        blobService = new S3AuctionBlobService(s3, serializer, loggerFactory.CreateLogger<S3AuctionBlobService>());
        playerIndex = new S3PlayerIndexService(s3, loggerFactory.CreateLogger<S3PlayerIndexService>());
        bloom = new BloomFilterService(s3, loggerFactory.CreateLogger<BloomFilterService>());
    }

    /// <summary>
    /// Verifies the archive round-trip against the configured local S3-compatible endpoint.
    /// </summary>
    [Test]
    public async Task FullRoundTrip()
    {
        // 1. Ensure bucket
        await s3.EnsureBucket();
        Console.WriteLine("✓ Bucket created/exists");

        // 2. Initialize Bloom filter (small for test)
        await bloom.Initialize(expectedItems: 1000, fpr: 0.01);
        Console.WriteLine("✓ Bloom filter initialized");

        // 3. Create test auctions
        var sellerUuid = Guid.NewGuid();
        var bidderUuid = Guid.NewGuid();
        var auctionUuid = Guid.NewGuid();
        // Use unique tag per run to avoid stale data from previous runs
        var testTag = $"TEST_SWORD_{DateTime.UtcNow.Ticks}";
        var testMonth = new DateTime(2024, 6, 1);

        var testAuction = new SaveAuction
        {
            Uuid = auctionUuid.ToString("N"),
            AuctioneerId = sellerUuid.ToString("N"),
            ProfileId = sellerUuid.ToString("N"),
            Tag = testTag,
            ItemName = "Legendary Diamond Sword",
            Category = Category.WEAPON,
            Tier = Tier.LEGENDARY,
            End = new DateTime(2024, 6, 15, 12, 0, 0, DateTimeKind.Utc),
            Start = new DateTime(2024, 6, 15, 0, 0, 0, DateTimeKind.Utc),
            StartingBid = 1000,
            HighestBidAmount = 50000,
            Bin = false,
            Count = 1,
            Enchantments = new List<Enchantment>
            {
                new() { Type = Enchantment.EnchantmentType.sharpness, Level = 7 }
            },
            Bids = new List<SaveBids>
            {
                new()
                {
                    Amount = 50000,
                    Bidder = bidderUuid.ToString("N"),
                    AuctionId = auctionUuid.ToString("N"),
                    Timestamp = new DateTime(2024, 6, 15, 6, 0, 0, DateTimeKind.Utc),
                    ProfileId = bidderUuid.ToString("N")
                }
            },
            FlatenedNBT = new Dictionary<string, string> { { "uid", "abc123" } },
            NbtData = new NbtData()
        };
        Console.WriteLine("✓ Test auction created");

        // 4. Write auction blob to S3
        await blobService.WriteAuctions(testTag, testMonth, new[] { testAuction });
        Console.WriteLine("✓ Auction blob written to S3");

        // 5. Verify blob exists
        var exists = await blobService.BlobExists(testTag, testMonth);
        Assert.That(exists, Is.True, "Blob should exist after write");
        Console.WriteLine("✓ Blob exists check passed");

        // 6. Read back and verify
        var readBack = await blobService.ReadAuctions(testTag, testMonth);
        Assert.That(readBack.Count, Is.EqualTo(1));
        Assert.That(readBack[0].Uuid, Is.EqualTo(auctionUuid.ToString("N")));
        Assert.That(readBack[0].Tag, Is.EqualTo(testTag));
        Assert.That(readBack[0].HighestBidAmount, Is.EqualTo(50000));
        Assert.That(readBack[0].Bids.Count, Is.EqualTo(1));
        Assert.That(readBack[0].Bids[0].Amount, Is.EqualTo(50000));
        Assert.That(readBack[0].Enchantments.Count, Is.EqualTo(1));
        Console.WriteLine("✓ Read-back verified: auction data matches");

        // 7. Write a second auction to same month (merge test)
        var auctionUuid2 = Guid.NewGuid();
        var testAuction2 = new SaveAuction
        {
            Uuid = auctionUuid2.ToString("N"),
            AuctioneerId = sellerUuid.ToString("N"),
            ProfileId = sellerUuid.ToString("N"),
            Tag = testTag,
            ItemName = "Another Diamond Sword",
            Category = Category.WEAPON,
            Tier = Tier.EPIC,
            End = new DateTime(2024, 6, 20, 12, 0, 0, DateTimeKind.Utc),
            Start = new DateTime(2024, 6, 20, 0, 0, 0, DateTimeKind.Utc),
            StartingBid = 500,
            HighestBidAmount = 25000,
            Bin = true,
            Count = 1,
            Enchantments = new List<Enchantment>(),
            Bids = new List<SaveBids>(),
            FlatenedNBT = new Dictionary<string, string>(),
            NbtData = new NbtData()
        };
        await blobService.WriteAuctions(testTag, testMonth, new[] { testAuction2 });
        var merged = await blobService.ReadAuctions(testTag, testMonth);
        Assert.That(merged.Count, Is.EqualTo(2));
        Console.WriteLine("✓ Merge test passed: 2 auctions in blob");

        // 8. Add to Bloom filter and check
        bloom.Add(auctionUuid);
        bloom.Add(auctionUuid2);
        Assert.That(bloom.MightExist(auctionUuid), Is.True);
        Assert.That(bloom.MightExist(auctionUuid2), Is.True);
        Assert.That(bloom.MightExist(Guid.NewGuid()), Is.False); // very likely false for a non-added UUID
        Console.WriteLine("✓ Bloom filter check passed");

        // 9. Flush Bloom to S3 and re-load
        await bloom.FlushIfDirty();
        var bloom2 = new BloomFilterService(s3, LoggerFactory.Create(b => b.AddConsole()).CreateLogger<BloomFilterService>());
        await bloom2.Initialize(expectedItems: 1000, fpr: 0.01);
        Assert.That(bloom2.MightExist(auctionUuid), Is.True, "Bloom filter should persist across reload");
        Console.WriteLine("✓ Bloom filter persistence test passed");

        // 10. Player index
        playerIndex.AddParticipations(sellerUuid, new[]
        {
            new PlayerParticipationEntry
            {
                AuctionUid = auctionUuid,
                End = testAuction.End,
                Tag = testTag,
                Type = ParticipationType.Seller
            }
        });
        playerIndex.AddParticipations(bidderUuid, new[]
        {
            new PlayerParticipationEntry
            {
                AuctionUid = auctionUuid,
                End = testAuction.End,
                Tag = testTag,
                Type = ParticipationType.Bidder
            }
        });
        await playerIndex.FlushAll();
        Console.WriteLine("✓ Player index flushed");

        var sellerEntries = await playerIndex.GetParticipation(sellerUuid, 2024);
        Assert.That(sellerEntries.Count, Is.EqualTo(1));
        Assert.That(sellerEntries[0].Type, Is.EqualTo(ParticipationType.Seller));
        Console.WriteLine("✓ Player seller index verified");

        var bidderEntries = await playerIndex.GetParticipation(bidderUuid, 2024);
        Assert.That(bidderEntries.Count, Is.EqualTo(1));
        Assert.That(bidderEntries[0].Type, Is.EqualTo(ParticipationType.Bidder));
        Console.WriteLine("✓ Player bidder index verified");

        // 11. Serializer round-trip test
        var bytes = serializer.Serialize(merged);
        var deserialized = serializer.Deserialize(bytes);
        Assert.That(deserialized.Count, Is.EqualTo(2));
        Console.WriteLine($"✓ Serializer round-trip: {bytes.Length} bytes compressed for {merged.Count} auctions");

        Console.WriteLine("\n=== ALL DRY-RUN TESTS PASSED ===");
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
