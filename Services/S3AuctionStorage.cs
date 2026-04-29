using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3;
using Coflnet.Sky.Core;
using Microsoft.Extensions.Logging;

namespace Coflnet.Sky.Auctions.Services;

/// <summary>
/// High-level service for accessing archived auctions from S3.
/// Provides UUID lookups via bloom filter and retrieval by tag/month.
/// </summary>
public class S3AuctionStorage
{
    private readonly S3StorageService s3;
    private readonly S3AuctionBlobService blobService;
    private readonly BloomFilterService bloomFilter;
    private readonly ILogger<S3AuctionStorage> logger;

    /// <summary>
    /// Creates a new S3AuctionStorage instance
    /// </summary>
    public S3AuctionStorage(
        S3StorageService s3,
        S3AuctionBlobService blobService,
        BloomFilterService bloomFilter,
        ILogger<S3AuctionStorage> logger)
    {
        this.s3 = s3;
        this.blobService = blobService;
        this.bloomFilter = bloomFilter;
        this.logger = logger;
    }

    /// <summary>
    /// Checks if an auction UUID might exist in the archive (bloom filter check)
    /// </summary>
    /// <param name="uuid">The auction UUID to check</param>
    /// <returns>False = definitely not present, True = possibly present</returns>
    public bool MayContain(Guid uuid) => bloomFilter.MightExist(uuid);

    /// <summary>
    /// Gets auctions for a specific item tag and month
    /// </summary>
    /// <param name="tag">The item tag</param>
    /// <param name="year">Year</param>
    /// <param name="month">Month (1-12)</param>
    /// <param name="ct">Cancellation token</param>
    /// <returns>List of auctions as CassandraAuction for compatibility with ScyllaService</returns>
    public async Task<IEnumerable<CassandraAuction>> GetAuctions(string tag, int year, int month, CancellationToken ct = default)
    {
        try
        {
            var date = new DateTime(year, month, 1, 0, 0, 0, DateTimeKind.Utc);
            var auctions = await blobService.ReadAuctions(tag, date, ct);
            // Convert to CassandraAuction format for compatibility
            return auctions.Select(ToCassandraAuction);
        }
        catch (Amazon.S3.AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            logger.LogDebug("No S3 blob found for {Tag} {Year}-{Month:D2}", tag, year, month);
            return Enumerable.Empty<CassandraAuction>();
        }
    }

    /// <summary>
    /// Attempts to find a specific auction by UUID.
    /// This is an expensive operation that may need to scan multiple blobs.
    /// Use MayContain first to filter obvious misses.
    /// </summary>
    /// <param name="uuid">The auction UUID</param>
    /// <param name="uid">The auction UId (for time estimation)</param>
    /// <param name="ct">Cancellation token</param>
    /// <returns>The auction if found, null otherwise</returns>
    public async Task<SaveAuction> GetAuctionByUuid(Guid uuid, long uid, CancellationToken ct = default)
    {
        if (!bloomFilter.MightExist(uuid))
        {
            logger.LogDebug("Bloom filter says UUID {Uuid} not in archive", uuid);
            return null;
        }

        var estimatedDate = EstimateDateFromUid(uid);

        var blobKeys = await s3.ListBlobs("auctions/", ct);
        var locations = blobKeys
            .Select(TryParseBlobLocation)
            .Where(location => location.HasValue)
            .Select(location => location.Value)
            .OrderBy(location => Math.Abs(((location.Month.Year - estimatedDate.Year) * 12) + location.Month.Month - estimatedDate.Month))
            .ToList();

        var match = await ScanLocations(uuid, locations, ct);
        if (match != null)
        {
            return match;
        }

        logger.LogDebug("Auction {Uuid} not found in S3 after scanning {Count} blobs", uuid, locations.Count);
        return null;
    }

    /// <summary>
    /// Gets an auction by UUID when the tag is known (much more efficient)
    /// </summary>
    /// <param name="uuid">The auction UUID</param>
    /// <param name="tag">The item tag</param>
    /// <param name="estimatedDate">Approximate date of the auction</param>
    /// <param name="ct">Cancellation token</param>
    public async Task<SaveAuction> GetAuctionByUuid(Guid uuid, string tag, DateTime estimatedDate, CancellationToken ct = default)
    {
        if (!bloomFilter.MightExist(uuid))
        {
            return null;
        }

        var searchMonths = GetSearchMonthRange(estimatedDate);
        var uuidString = uuid.ToString("N");

        foreach (var month in searchMonths)
        {
            if (ct.IsCancellationRequested) break;

            try
            {
                var auctions = await blobService.ReadAuctions(tag, month, ct);
                var match = auctions.FirstOrDefault(a => 
                    string.Equals(a.Uuid, uuidString, StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(a.Uuid, uuid.ToString(), StringComparison.OrdinalIgnoreCase));
                
                if (match != null)
                {
                    logger.LogDebug("Found auction {Uuid} in S3 blob {Tag}/{Month:yyyy-MM}", uuid, tag, month);
                    return match;
                }
            }
            catch (Amazon.S3.AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
            }
        }

        var allMonths = await GetAvailableMonths(tag, ct);
        var remainingMonths = allMonths.Where(month => !searchMonths.Contains(month));
        foreach (var month in remainingMonths)
        {
            if (ct.IsCancellationRequested)
            {
                break;
            }

            try
            {
                var auctions = await blobService.ReadAuctions(tag, month, ct);
                var match = auctions.FirstOrDefault(a =>
                    string.Equals(AuctionIdentity.NormalizeUuid(a.Uuid), uuidString, StringComparison.OrdinalIgnoreCase));
                if (match != null)
                {
                    logger.LogDebug("Found auction {Uuid} in S3 blob {Tag}/{Month:yyyy-MM}", uuid, tag, month);
                    return match;
                }
            }
            catch (Amazon.S3.AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
            }
        }

        logger.LogDebug("Auction {Uuid} not found in S3 for tag {Tag} around {Date}", uuid, tag, estimatedDate);
        return null;
    }

    /// <summary>
    /// Lists available months for a tag
    /// </summary>
    public async Task<List<DateTime>> GetAvailableMonths(string tag, CancellationToken ct = default)
    {
        var prefix = $"auctions/{tag}/";
        var keys = await s3.ListBlobs(prefix, ct);
        return keys
            .Select(TryParseBlobLocation)
            .Where(location => location.HasValue)
            .Select(location => location.Value.Month)
            .Distinct()
            .OrderBy(month => month)
            .ToList();
    }

    private static DateTime EstimateDateFromUid(long uid)
    {
        // UIds are sequential, rough estimate:
        // - Started around 2019-06-01
        // - ~30M auctions per year average
        var baseDate = new DateTime(2019, 6, 1, 0, 0, 0, DateTimeKind.Utc);
        var auctionsPerYear = 30_000_000L;
        var yearsFromUid = uid / auctionsPerYear;
        return baseDate.AddYears((int)yearsFromUid);
    }

    private static IEnumerable<DateTime> GetSearchMonthRange(DateTime center)
    {
        yield return new DateTime(center.Year, center.Month, 1);
        for (int i = 1; i <= 2; i++)
        {
            yield return new DateTime(center.Year, center.Month, 1).AddMonths(-i);
            yield return new DateTime(center.Year, center.Month, 1).AddMonths(i);
        }
    }

    private async Task<SaveAuction> ScanLocations(Guid uuid, IEnumerable<(string Tag, DateTime Month)> locations, CancellationToken ct)
    {
        var uuidString = uuid.ToString("N");
        foreach (var location in locations)
        {
            if (ct.IsCancellationRequested)
            {
                break;
            }

            try
            {
                var auctions = await blobService.ReadAuctions(location.Tag, location.Month, ct);
                var match = auctions.FirstOrDefault(a =>
                    string.Equals(AuctionIdentity.NormalizeUuid(a.Uuid), uuidString, StringComparison.OrdinalIgnoreCase));
                if (match != null)
                {
                    logger.LogDebug("Found auction {Uuid} in S3 blob {Tag}/{Month:yyyy-MM}", uuid, location.Tag, location.Month);
                    return match;
                }
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
            }
        }

        return null;
    }

    private static (string Tag, DateTime Month)? TryParseBlobLocation(string key)
    {
        if (string.IsNullOrWhiteSpace(key))
        {
            return null;
        }

        var parts = key.Split('/', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length != 3 || !string.Equals(parts[0], "auctions", StringComparison.Ordinal))
        {
            return null;
        }

        var monthPart = Path.GetFileNameWithoutExtension(Path.GetFileNameWithoutExtension(parts[2]));
        if (!DateTime.TryParseExact(monthPart, "yyyy-MM", null, System.Globalization.DateTimeStyles.AssumeUniversal, out var month))
        {
            return null;
        }

        return (parts[1], new DateTime(month.Year, month.Month, 1, 0, 0, 0, DateTimeKind.Utc));
    }

    private static CassandraAuction ToCassandraAuction(SaveAuction a)
    {
        var highestBid = a.Bids?.OrderByDescending(b => b.Amount).FirstOrDefault();
        return new CassandraAuction
        {
            Uuid = AuctionIdentity.ParseGuidOrEmpty(a.Uuid),
            Tag = a.Tag ?? "unknown",
            End = a.End,
            Start = a.Start,
            HighestBidAmount = a.HighestBidAmount,
            Bin = a.Bin,
            IsSold = a.HighestBidAmount > 0 && a.End <= DateTime.UtcNow,
            Auctioneer = AuctionIdentity.ParseGuidOrEmpty(a.AuctioneerId),
            ProfileId = AuctionIdentity.ParseGuidOrEmpty(a.ProfileId),
            ItemName = a.ItemName ?? string.Empty,
            Tier = a.Tier.ToString(),
            Category = a.Category.ToString(),
            NbtLookup = a.FlatenedNBT ?? new Dictionary<string, string>(),
            ItemBytes = a.NbtData?.data?.ToArray(),
            Count = a.Count,
            StartingBid = a.StartingBid,
            ItemCreatedAt = a.ItemCreatedAt,
            ItemUid = AuctionIdentity.ParseItemUid(a.FlatenedNBT),
            ItemId = AuctionIdentity.ParseItemGuid(a.FlatenedNBT),
            Coop = a.CoopMembers?.Select(c => c.ToString()).ToList() ?? a.Coop,
            HighestBidder = AuctionIdentity.ParseGuidOrEmpty(highestBid?.Bidder),
            Bids = a.Bids?.Select(b => new CassandraBid
            {
                BidderUuid = AuctionIdentity.ParseGuidOrEmpty(b.Bidder),
                ProfileId = AuctionIdentity.ParseGuidOrEmpty(b.ProfileId),
                Amount = b.Amount,
                Timestamp = b.Timestamp,
                AuctionUuid = AuctionIdentity.ParseGuidOrEmpty(b.AuctionId),
                BidderName = string.Empty
            }).ToList(),
            Enchantments = a.Enchantments?.ToDictionary(e => e.Type.ToString(), e => (int)e.Level) ?? new Dictionary<string, int>()
        };
    }
}
