using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.Core;
using Microsoft.Extensions.Logging;

namespace Coflnet.Sky.Auctions.Services;

public class S3AuctionBlobService
{
    private readonly S3StorageService s3;
    private readonly AuctionBlobSerializer serializer;
    private readonly ILogger<S3AuctionBlobService> logger;
    private readonly ConcurrentDictionary<string, SemaphoreSlim> blobLocks = new(StringComparer.Ordinal);

    public S3AuctionBlobService(S3StorageService s3, AuctionBlobSerializer serializer, ILogger<S3AuctionBlobService> logger)
    {
        this.s3 = s3;
        this.serializer = serializer;
        this.logger = logger;
    }

    public static string BlobKey(string tag, DateTime month) =>
        $"auctions/{tag}/{month:yyyy-MM}.json.gz";

    public async Task WriteAuctions(string tag, DateTime month, IEnumerable<SaveAuction> newAuctions, CancellationToken ct = default)
    {
        var key = BlobKey(tag, month);
        var writeLock = blobLocks.GetOrAdd(key, _ => new SemaphoreSlim(1, 1));
        await writeLock.WaitAsync(ct);
        try
        {
            var existing = new List<SaveAuction>();
            if (await s3.HeadBlob(key, ct))
            {
                try
                {
                    var data = await s3.GetBlob(key, ct);
                    existing = serializer.Deserialize(data);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Failed to read existing archive blob {key}", e);
                }
            }

            var lookup = new Dictionary<string, SaveAuction>(StringComparer.OrdinalIgnoreCase);
            foreach (var auction in existing)
            {
                var normalizedUuid = AuctionIdentity.NormalizeUuid(auction.Uuid);
                if (!string.IsNullOrEmpty(normalizedUuid))
                {
                    lookup[normalizedUuid] = auction;
                }
            }

            foreach (var auction in newAuctions)
            {
                var normalizedUuid = AuctionIdentity.NormalizeUuid(auction.Uuid);
                if (string.IsNullOrEmpty(normalizedUuid))
                {
                    logger.LogWarning("Skipping archive write for auction with invalid uuid in {Key}: {Uuid}", key, auction.Uuid);
                    continue;
                }

                if (!lookup.TryGetValue(normalizedUuid, out var previous) || auction.HighestBidAmount >= previous.HighestBidAmount)
                {
                    lookup[normalizedUuid] = auction;
                }
            }

            var merged = lookup.Values
                .OrderBy(a => a.End)
                .ThenBy(a => AuctionIdentity.GetAuctionUid(a))
                .ToList();
            var bytes = serializer.Serialize(merged);
            await s3.PutBlob(key, bytes, "application/gzip", ct);
            logger.LogInformation("Wrote {Count} auctions to {Key} ({SizeKB} KB)", merged.Count, key, bytes.Length / 1024);
        }
        finally
        {
            writeLock.Release();
        }
    }

    public async Task<List<SaveAuction>> ReadAuctions(string tag, DateTime month, CancellationToken ct = default)
    {
        var key = BlobKey(tag, month);
        var data = await s3.GetBlob(key, ct);
        return serializer.Deserialize(data);
    }

    public async Task<bool> BlobExists(string tag, DateTime month, CancellationToken ct = default)
    {
        return await s3.HeadBlob(BlobKey(tag, month), ct);
    }
}
