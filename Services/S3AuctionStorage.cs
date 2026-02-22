using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using Coflnet.Sky.Core;
using MessagePack;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Coflnet.Sky.Auctions.Services;

#nullable enable

/// <summary>
/// S3-compatible storage service for archived auctions
/// Stores auctions by month and item tag with bloom filter indexing
/// </summary>
public class S3AuctionStorage : IDisposable
{
    private readonly IAmazonS3 _s3Client;
    private readonly ILogger<S3AuctionStorage> _logger;
    private readonly string _bucketName;
    private readonly ConcurrentDictionary<string, AuctionBloomFilter> _bloomFilterCache = new();
    private readonly SemaphoreSlim _bloomFilterLock = new(1, 1);

    /// <summary>
    /// Creates a new S3AuctionStorage instance
    /// </summary>
    public S3AuctionStorage(IAmazonS3 s3Client, ILogger<S3AuctionStorage> logger, IConfiguration configuration)
    {
        _s3Client = s3Client;
        _logger = logger;
        _bucketName = configuration["S3:BUCKET_NAME"] ?? "sky-auctions-archive";
    }

    /// <summary>
    /// Gets the S3 key for storing auctions of a specific item and month
    /// Format: auctions/{tag}/{year}/{month}.msgpack.gz
    /// </summary>
    public static string GetAuctionKey(string tag, int year, int month)
    {
        return $"auctions/{SanitizeTag(tag)}/{year}/{month:D2}.msgpack.gz";
    }

    /// <summary>
    /// Gets the S3 key for storing the bloom filter index for a specific item
    /// Format: index/{tag}/bloom.bin
    /// </summary>
    public static string GetBloomFilterKey(string tag)
    {
        return $"index/{SanitizeTag(tag)}/bloom.bin";
    }

    /// <summary>
    /// Gets the S3 key for the master bloom filter that indexes all auctions
    /// Format: index/master_bloom_{level}.bin
    /// </summary>
    public static string GetMasterBloomFilterKey(int level = 0)
    {
        return $"index/master_bloom_{level}.bin";
    }

    /// <summary>
    /// Sanitizes a tag for use in S3 keys
    /// </summary>
    private static string SanitizeTag(string tag)
    {
        return tag?.Replace("/", "_").Replace("\\", "_") ?? "unknown";
    }

    /// <summary>
    /// Stores a batch of auctions for a specific tag and month
    /// </summary>
    public async Task StoreAuctions(string tag, int year, int month, IEnumerable<ScyllaAuction> auctions)
    {
        var auctionList = auctions.ToList();
        if (!auctionList.Any())
        {
            _logger.LogInformation("No auctions to store for {Tag} {Year}/{Month}", tag, year, month);
            return;
        }

        var key = GetAuctionKey(tag, year, month);

        // Serialize auctions using MessagePack for efficient storage
        var serialized = MessagePackSerializer.Serialize(auctionList, MessagePackSerializerOptions.Standard.WithCompression(MessagePackCompression.Lz4BlockArray));

        // Compress with gzip for additional compression
        using var compressedStream = new MemoryStream();
        await using (var gzipStream = new GZipStream(compressedStream, CompressionLevel.Optimal, leaveOpen: true))
        {
            await gzipStream.WriteAsync(serialized);
        }
        compressedStream.Position = 0;

        var putRequest = new PutObjectRequest
        {
            BucketName = _bucketName,
            Key = key,
            InputStream = compressedStream,
            ContentType = "application/x-msgpack",
            Metadata =
            {
                ["x-amz-meta-count"] = auctionList.Count.ToString(),
                ["x-amz-meta-tag"] = tag,
                ["x-amz-meta-year"] = year.ToString(),
                ["x-amz-meta-month"] = month.ToString()
            }
        };

        await _s3Client.PutObjectAsync(putRequest);

        _logger.LogInformation("Stored {Count} auctions for {Tag} {Year}/{Month} ({Size} bytes compressed)",
            auctionList.Count, tag, year, month, compressedStream.Length);

        // Update bloom filter index
        await UpdateBloomFilterIndex(tag, auctionList);
    }

    /// <summary>
    /// Retrieves auctions for a specific tag and month
    /// </summary>
    public async Task<List<ScyllaAuction>> GetAuctions(string tag, int year, int month)
    {
        var key = GetAuctionKey(tag, year, month);

        try
        {
            var response = await _s3Client.GetObjectAsync(_bucketName, key);

            using var responseStream = response.ResponseStream;
            using var gzipStream = new GZipStream(responseStream, CompressionMode.Decompress);
            using var memoryStream = new MemoryStream();
            await gzipStream.CopyToAsync(memoryStream);
            memoryStream.Position = 0;

            var auctions = MessagePackSerializer.Deserialize<List<ScyllaAuction>>(memoryStream,
                MessagePackSerializerOptions.Standard.WithCompression(MessagePackCompression.Lz4BlockArray));

            _logger.LogInformation("Retrieved {Count} auctions for {Tag} {Year}/{Month}", auctions.Count, tag, year, month);
            return auctions;
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            _logger.LogDebug("No auctions found for {Tag} {Year}/{Month}", tag, year, month);
            return new List<ScyllaAuction>();
        }
    }

    /// <summary>
    /// Checks if an auction exists in the bloom filter (may have false positives)
    /// </summary>
    public async Task<(bool mayExist, string? tag, int? year, int? month)> MayContainAuction(Guid auctionUuid)
    {
        // Check master bloom filter first
        var masterFilter = await GetOrLoadMasterBloomFilter();
        if (masterFilter == null || !masterFilter.MayContain(auctionUuid))
        {
            return (false, null, null, null);
        }

        // If master filter says it may exist, we need to search tag-specific filters
        // This is a simplified approach - in production you might want to index tag/year/month too
        return (true, null, null, null);
    }

    /// <summary>
    /// Looks up an auction by UUID using bloom filter hierarchy
    /// </summary>
    public async Task<ScyllaAuction?> GetAuctionByUuid(Guid auctionUuid, long? auctionUid = null)
    {
        // First check if it might exist in any archive
        var (mayExist, _, _, _) = await MayContainAuction(auctionUuid);
        if (!mayExist)
        {
            _logger.LogDebug("Auction {Uuid} not in bloom filter, skipping S3 search", auctionUuid);
            return null;
        }

        // Get the tag-specific bloom filters to narrow down the search
        // This requires iterating through stored bloom filters
        var tagFilters = await ListTagBloomFilters();

        foreach (var tagFilter in tagFilters)
        {
            var filter = await GetOrLoadBloomFilter(tagFilter.Tag);
            if (filter != null && filter.MayContain(auctionUuid))
            {
                // Search through months for this tag
                foreach (var (year, month) in tagFilter.MonthsWithData)
                {
                    var auctions = await GetAuctions(tagFilter.Tag, year, month);
                    var match = auctions.FirstOrDefault(a => a.Uuid == auctionUuid);
                    if (match != null)
                    {
                        return match;
                    }
                }
            }
        }

        return null;
    }

    /// <summary>
    /// Updates the bloom filter index with new auctions
    /// </summary>
    private async Task UpdateBloomFilterIndex(string tag, List<ScyllaAuction> auctions)
    {
        await _bloomFilterLock.WaitAsync();
        try
        {
            // Get or create tag-specific bloom filter
            var filter = await GetOrLoadBloomFilter(tag) ?? new AuctionBloomFilter(tag, expectedItems: 1_000_000, falsePositiveRate: 0.01);

            foreach (var auction in auctions)
            {
                filter.Add(auction.Uuid);
            }

            // Save tag-specific filter
            await SaveBloomFilter(tag, filter);

            // Update master bloom filter
            var masterFilter = await GetOrLoadMasterBloomFilter() ?? new AuctionBloomFilter("master", expectedItems: 100_000_000, falsePositiveRate: 0.001);
            foreach (var auction in auctions)
            {
                masterFilter.Add(auction.Uuid);
            }
            await SaveMasterBloomFilter(masterFilter);
        }
        finally
        {
            _bloomFilterLock.Release();
        }
    }

    private async Task<AuctionBloomFilter?> GetOrLoadBloomFilter(string tag)
    {
        if (_bloomFilterCache.TryGetValue(tag, out var cached))
        {
            return cached;
        }

        try
        {
            var key = GetBloomFilterKey(tag);
            var response = await _s3Client.GetObjectAsync(_bucketName, key);

            using var stream = response.ResponseStream;
            using var memoryStream = new MemoryStream();
            await stream.CopyToAsync(memoryStream);
            memoryStream.Position = 0;

            var filter = AuctionBloomFilter.Deserialize(memoryStream.ToArray());
            _bloomFilterCache[tag] = filter;
            return filter;
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    private async Task<AuctionBloomFilter?> GetOrLoadMasterBloomFilter()
    {
        return await GetOrLoadBloomFilter("master");
    }

    private async Task SaveBloomFilter(string tag, AuctionBloomFilter filter)
    {
        var key = GetBloomFilterKey(tag);
        var data = filter.Serialize();

        using var stream = new MemoryStream(data);
        var putRequest = new PutObjectRequest
        {
            BucketName = _bucketName,
            Key = key,
            InputStream = stream,
            ContentType = "application/octet-stream"
        };

        await _s3Client.PutObjectAsync(putRequest);
        _bloomFilterCache[tag] = filter;

        _logger.LogDebug("Saved bloom filter for {Tag} ({Size} bytes)", tag, data.Length);
    }

    private async Task SaveMasterBloomFilter(AuctionBloomFilter filter)
    {
        var key = GetMasterBloomFilterKey();
        var data = filter.Serialize();

        using var stream = new MemoryStream(data);
        var putRequest = new PutObjectRequest
        {
            BucketName = _bucketName,
            Key = key,
            InputStream = stream,
            ContentType = "application/octet-stream"
        };

        await _s3Client.PutObjectAsync(putRequest);
        _bloomFilterCache["master"] = filter;

        _logger.LogDebug("Saved master bloom filter ({Size} bytes)", data.Length);
    }

    /// <summary>
    /// Lists all tags that have bloom filters stored
    /// </summary>
    private async Task<List<TagFilterInfo>> ListTagBloomFilters()
    {
        var result = new List<TagFilterInfo>();
        var request = new ListObjectsV2Request
        {
            BucketName = _bucketName,
            Prefix = "index/",
            Delimiter = "/"
        };

        ListObjectsV2Response response;
        do
        {
            response = await _s3Client.ListObjectsV2Async(request);

            foreach (var prefix in response.CommonPrefixes)
            {
                var tag = prefix.TrimEnd('/').Split('/').Last();
                if (tag != "master_bloom_0.bin")
                {
                    // Get months with data for this tag
                    var months = await GetMonthsWithData(tag);
                    result.Add(new TagFilterInfo { Tag = tag, MonthsWithData = months });
                }
            }

            request.ContinuationToken = response.NextContinuationToken;
        } while (response.IsTruncated);

        return result;
    }

    /// <summary>
    /// Gets all year/month combinations that have data for a specific tag
    /// </summary>
    public async Task<List<(int year, int month)>> GetMonthsWithData(string tag)
    {
        var result = new List<(int year, int month)>();
        var request = new ListObjectsV2Request
        {
            BucketName = _bucketName,
            Prefix = $"auctions/{SanitizeTag(tag)}/"
        };

        ListObjectsV2Response response;
        do
        {
            response = await _s3Client.ListObjectsV2Async(request);

            foreach (var obj in response.S3Objects)
            {
                // Parse key like "auctions/DIAMOND_SWORD/2024/01.msgpack.gz"
                var parts = obj.Key.Split('/');
                if (parts.Length >= 4 &&
                    int.TryParse(parts[2], out var year) &&
                    int.TryParse(Path.GetFileNameWithoutExtension(parts[3]).Split('.')[0], out var month))
                {
                    result.Add((year, month));
                }
            }

            request.ContinuationToken = response.NextContinuationToken;
        } while (response.IsTruncated);

        return result;
    }

    /// <summary>
    /// Checks if a specific month's data exists in S3
    /// </summary>
    public async Task<bool> MonthDataExists(string tag, int year, int month)
    {
        var key = GetAuctionKey(tag, year, month);

        try
        {
            await _s3Client.GetObjectMetadataAsync(_bucketName, key);
            return true;
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return false;
        }
    }

    /// <summary>
    /// Deletes auction data for a specific tag and month
    /// </summary>
    public async Task DeleteAuctions(string tag, int year, int month)
    {
        var key = GetAuctionKey(tag, year, month);
        await _s3Client.DeleteObjectAsync(_bucketName, key);
        _logger.LogInformation("Deleted auctions for {Tag} {Year}/{Month}", tag, year, month);
    }

    /// <summary>
    /// Disposes the S3 client and other resources
    /// </summary>
    public void Dispose()
    {
        _s3Client?.Dispose();
        _bloomFilterLock?.Dispose();
    }

    private class TagFilterInfo
    {
        public string Tag { get; set; } = string.Empty;
        public List<(int year, int month)> MonthsWithData { get; set; } = new();
    }
}
