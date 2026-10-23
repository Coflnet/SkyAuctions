// Copied from SkyIndexer (Services/Archive/S3AuctionBlobStore.cs) until the services merge (plan 4.6).
// Differences: the S3 client is the DI-registered IAmazonS3 (Startup, S3:SERVICE_URL/S3:REGION/S3:ACCESS_KEY/S3:SECRET_KEY),
// the bucket is S3:BUCKET_NAME (fallback S3:BUCKET), metrics are prefixed sky_auctions_blob_*, the payload buffer is the
// DiskBackedStream SkyAuctions already has (Coflnet.Sky.Core.DiskBackedStream, the class SkyIndexer's copy was made from),
// and it implements the core IArchiveMetaStore (partition list cache, Scylla month manifests).
using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using Coflnet.Sky.Core;
using Coflnet.Sky.Core.Archive;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Coflnet.Sky.Auctions.Archive
{
    /// <summary>
    /// S3-compatible (Cloudflare R2) implementation of <see cref="IAuctionBlobStore"/> for SkyAuctions.
    /// Blobs are Brotli compressed MessagePack (<see cref="ArchiveBlobCodec"/>) under <see cref="AuctionBlobKeys"/>.
    /// Blocks are cached in a dedicated bytes-bounded <see cref="MemoryCache"/> (<c>S3:CACHE_*</c> keys); indexes and meta objects are never cached.
    /// </summary>
    public class S3AuctionBlobStore : IAuctionBlobStore, IArchiveMetaStore, IDisposable
    {
        private readonly ILogger<S3AuctionBlobStore> logger;
        private readonly MemoryCache cache;
        private readonly IAmazonS3 client;
        private readonly string bucket;
        private readonly bool enabled;
        private readonly TimeSpan cacheTtl;
        private readonly long perEntryOverheadBytes;
        /// <summary>Set once the endpoint answered 501 to a conditional PUT; index writes are unconditional from then on.</summary>
        private volatile bool conditionalWritesUnsupported;

        private static readonly Prometheus.Counter blobReads = Prometheus.Metrics.CreateCounter("sky_auctions_blob_reads", "Archive blob reads (cached or not)");
        private static readonly Prometheus.Counter blobReadHits = Prometheus.Metrics.CreateCounter("sky_auctions_blob_read_hits", "Archive blob reads that returned data from storage");
        private static readonly Prometheus.Counter blobReadCacheHits = Prometheus.Metrics.CreateCounter("sky_auctions_blob_read_cache_hits", "Archive blob reads served from the in-process cache");
        private static readonly Prometheus.Counter blobReadMisses = Prometheus.Metrics.CreateCounter("sky_auctions_blob_read_misses", "Archive blob reads that returned 404");
        private static readonly Prometheus.Counter blobReadErrors = Prometheus.Metrics.CreateCounter("sky_auctions_blob_read_errors", "Archive blob reads that failed with an unexpected error");
        private static readonly Prometheus.Counter blobWrites = Prometheus.Metrics.CreateCounter("sky_auctions_blob_writes", "Archive blob writes");
        private static readonly Prometheus.Counter blobWriteErrors = Prometheus.Metrics.CreateCounter("sky_auctions_blob_write_errors", "Archive blob writes that failed");
        private static readonly Prometheus.Counter indexConflicts = Prometheus.Metrics.CreateCounter("sky_auctions_blob_index_conflicts", "Conditional tag index writes rejected because another writer changed the index");
        private static readonly Prometheus.Histogram blobWriteBytes = Prometheus.Metrics.CreateHistogram("sky_auctions_blob_write_bytes", "Compressed archive blob size on write",
            new Prometheus.HistogramConfiguration { Buckets = Prometheus.Histogram.ExponentialBuckets(256, 4, 12) });

        /// <inheritdoc/>
        public bool IsEnabled => enabled;

        /// <summary>DI constructor: the client is the one registered in Startup when <c>S3:ENABLED</c> is true.</summary>
        public S3AuctionBlobStore(IAmazonS3 client, IConfiguration config, ILogger<S3AuctionBlobStore> logger)
        {
            this.logger = logger;
            (cache, cacheTtl, perEntryOverheadBytes) = CreateCache(config);
            this.client = client;
            bucket = BucketName(config);
            var configured = bool.TryParse(config?["S3:ENABLED"], out var isEnabled) && isEnabled;
            if (!configured)
                return;
            if (client == null || string.IsNullOrWhiteSpace(bucket))
            {
                logger.LogWarning("S3:ENABLED=true but the S3 client or S3:BUCKET_NAME is missing; auction archive store disabled.");
                return;
            }
            enabled = true;
            logger.LogInformation("Auction archive store enabled: bucket={Bucket} endpoint={Endpoint}", bucket, config["S3:SERVICE_URL"] ?? config["S3:ENDPOINT"] ?? "(aws default)");
        }

        /// <summary><c>S3:BUCKET_NAME</c>, falling back to <c>S3:BUCKET</c> (the SkyIndexer/SkyBazaar key).</summary>
        internal static string BucketName(IConfiguration config)
        {
            var name = config?["S3:BUCKET_NAME"];
            return string.IsNullOrWhiteSpace(name) ? config?["S3:BUCKET"] : name;
        }

        private static (MemoryCache, TimeSpan, long) CreateCache(IConfiguration config)
        {
            var ttl = TimeSpan.FromMinutes(ConfigLong(config, "S3:CACHE_SLIDING_TTL_MINUTES", 10));
            // Each entry is billed as max(compressed, decoded estimate) + this overhead, see CacheBlock.
            var overhead = ConfigLong(config, "S3:CACHE_PER_ENTRY_OVERHEAD_BYTES", 8 * 1024);
            var size = ConfigLong(config, "S3:CACHE_SIZE_BYTES", 256L * 1024 * 1024);
            // Dedicated cache so archive reads can not evict the rest of the app's caches; the unit is bytes.
            return (new MemoryCache(new MemoryCacheOptions { SizeLimit = size }), ttl, overhead);
        }

        private static string CacheKey(string tag, int month, int part) => "block:" + tag + ":" + month + ":" + part;

        /// <inheritdoc/>
        public async Task<ArchivedAuctionBlock> ReadBlockAsync(string tag, int month, int part, bool uncached = false, CancellationToken cancellationToken = default)
        {
            if (!enabled)
                return null;
            blobReads.Inc();
            var cacheKey = CacheKey(tag, month, part);
            if (!uncached && cache.TryGetValue(cacheKey, out ArchivedAuctionBlock cached))
            {
                blobReadCacheHits.Inc();
                return cached;
            }
            var (block, compressedBytes, _) = await GetAsync<ArchivedAuctionBlock>(AuctionBlobKeys.Block(tag, month, part), cancellationToken).ConfigureAwait(false);
            if (block != null)
                CacheBlock(cacheKey, block, compressedBytes);
            else
                cache.Remove(cacheKey);
            return block;
        }

        /// <inheritdoc/>
        public async Task WriteBlockAsync(ArchivedAuctionBlock block, CancellationToken cancellationToken = default)
        {
            if (block == null)
                throw new ArgumentNullException(nameof(block));
            await PutAsync(AuctionBlobKeys.Block(block.Tag, block.Month, block.Part), block, cancellationToken).ConfigureAwait(false);
            // not cached on write: the archive job verifies with an uncached read, which refreshes the cache
            cache.Remove(CacheKey(block.Tag, block.Month, block.Part));
        }

        /// <inheritdoc/>
        public async Task<bool> BlockExistsAsync(string tag, int month, int part, CancellationToken cancellationToken = default)
        {
            if (!enabled)
                return false;
            try
            {
                await client.GetObjectMetadataAsync(new GetObjectMetadataRequest
                {
                    BucketName = bucket,
                    Key = AuctionBlobKeys.Block(tag, month, part),
                }, cancellationToken).ConfigureAwait(false);
                return true;
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                return false;
            }
        }

        /// <inheritdoc/>
        public async Task<ArchivedTagIndex> ReadIndexAsync(string tag, CancellationToken cancellationToken = default)
        {
            if (!enabled)
                return null;
            blobReads.Inc();
            var (index, _, etag) = await GetAsync<ArchivedTagIndex>(AuctionBlobKeys.Index(tag), cancellationToken).ConfigureAwait(false);
            if (index != null)
                index.ETag = etag;
            return index;
        }

        /// <inheritdoc/>
        public async Task WriteIndexAsync(ArchivedTagIndex index, CancellationToken cancellationToken = default)
        {
            if (index == null)
                throw new ArgumentNullException(nameof(index));
            index.ETag = await PutConditionalAsync(AuctionBlobKeys.Index(index.Tag), index, index.ETag, index.Tag, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// PUT replacing only the version that was read (If-Match <paramref name="etag"/>), or creating only if nobody else created
        /// it meanwhile (If-None-Match: * for a null etag). Returns the new ETag. 412/409 conflicts become
        /// <see cref="ArchiveIndexConflictException"/>; an endpoint without conditional PUT (501) is warned about once and written unconditionally.
        /// </summary>
        private async Task<string> PutConditionalAsync<T>(string key, T value, string etag, string conflictName, CancellationToken cancellationToken)
        {
            if (!conditionalWritesUnsupported)
            {
                try
                {
                    var response = await PutAsync(key, value, cancellationToken, request =>
                    {
                        if (etag != null)
                            request.IfMatch = etag;
                        else
                            request.IfNoneMatch = "*";
                    }).ConfigureAwait(false);
                    return response?.ETag;
                }
                catch (AmazonS3Exception ex) when (IsPreconditionFailure(ex))
                {
                    indexConflicts.Inc();
                    throw new ArchiveIndexConflictException(conflictName, ex);
                }
                catch (AmazonS3Exception ex) when (IsNotImplemented(ex))
                {
                    conditionalWritesUnsupported = true;
                    logger.LogWarning(ex, "The S3 endpoint does not support conditional PUT (If-Match/If-None-Match); tag index and manifest writes fall back to unconditional PUTs, concurrent writers can lose entries");
                }
            }
            var plain = await PutAsync(key, value, cancellationToken).ConfigureAwait(false);
            return plain?.ETag;
        }

        /// <inheritdoc/>
        public async Task<T> ReadMetaAsync<T>(string key, CancellationToken cancellationToken = default) where T : class
            => (await ReadMetaVersionedAsync<T>(key, cancellationToken).ConfigureAwait(false)).Value;

        /// <inheritdoc/>
        public async Task<(T Value, string ETag)> ReadMetaVersionedAsync<T>(string key, CancellationToken cancellationToken = default) where T : class
        {
            if (!enabled)
                return (null, null);
            blobReads.Inc();
            var (value, _, etag) = await GetAsync<T>(AuctionBlobKeys.Meta(key), cancellationToken).ConfigureAwait(false);
            return (value, value == null ? null : etag);
        }

        /// <inheritdoc/>
        public async Task WriteMetaAsync<T>(string key, T value, CancellationToken cancellationToken = default) where T : class
        {
            if (value == null)
                throw new ArgumentNullException(nameof(value));
            await PutAsync(AuctionBlobKeys.Meta(key), value, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public Task<string> WriteMetaConditionalAsync<T>(string key, T value, string etag, CancellationToken cancellationToken = default) where T : class
        {
            if (value == null)
                throw new ArgumentNullException(nameof(value));
            var fullKey = AuctionBlobKeys.Meta(key);
            return PutConditionalAsync(fullKey, value, etag, fullKey, cancellationToken);
        }

        /// <inheritdoc/>
        public async Task<IReadOnlyList<string>> ListMetaKeysAsync(string prefix, CancellationToken cancellationToken = default)
        {
            if (!enabled)
                return Array.Empty<string>();
            var fullPrefix = AuctionBlobKeys.Meta(prefix);
            var keys = new List<string>();
            var request = new ListObjectsV2Request { BucketName = bucket, Prefix = fullPrefix };
            ListObjectsV2Response response;
            do
            {
                response = await client.ListObjectsV2Async(request, cancellationToken).ConfigureAwait(false);
                foreach (var o in response.S3Objects ?? new List<S3Object>())
                    keys.Add(o.Key);
                request.ContinuationToken = response.NextContinuationToken;
            } while (response.IsTruncated == true && !string.IsNullOrEmpty(response.NextContinuationToken));
            keys.Sort(StringComparer.Ordinal);
            return keys;
        }

        /// <summary>412, or the PreconditionFailed / ConditionalRequestConflict error codes (409 when two conditional writes race).</summary>
        internal static bool IsPreconditionFailure(AmazonS3Exception ex)
            => ex.StatusCode == HttpStatusCode.PreconditionFailed
               || string.Equals(ex.ErrorCode, "PreconditionFailed", StringComparison.OrdinalIgnoreCase)
               || ex.StatusCode == HttpStatusCode.Conflict && string.Equals(ex.ErrorCode, "ConditionalRequestConflict", StringComparison.OrdinalIgnoreCase);

        private static bool IsNotImplemented(AmazonS3Exception ex)
            => ex.StatusCode == HttpStatusCode.NotImplemented || string.Equals(ex.ErrorCode, "NotImplemented", StringComparison.OrdinalIgnoreCase);

        private async Task<(T value, long compressedBytes, string etag)> GetAsync<T>(string key, CancellationToken cancellationToken) where T : class
        {
            try
            {
                using var response = await client.GetObjectAsync(new GetObjectRequest { BucketName = bucket, Key = key }, cancellationToken).ConfigureAwait(false);
                using var stream = response.ResponseStream;
                var value = await ArchiveBlobCodec.ReadAsync<T>(stream, cancellationToken).ConfigureAwait(false);
                blobReadHits.Inc();
                return (value, response.ContentLength, response.ETag);
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                blobReadMisses.Inc();
                return (null, 0, null);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                blobReadErrors.Inc();
                logger.LogWarning(ex, "Archive blob read failed for {Key}", key);
                throw;
            }
        }

        private async Task<PutObjectResponse> PutAsync<T>(string key, T value, CancellationToken cancellationToken, Action<PutObjectRequest> conditions = null)
        {
            if (!enabled)
                throw new InvalidOperationException("S3 auction archive store is not enabled");
            try
            {
                // Large months spill to a temp file instead of holding the compressed payload twice in memory.
                using var payload = new DiskBackedStream();
                ArchiveBlobCodec.Write(payload, value, cancellationToken);
                payload.Position = 0;
                var length = payload.Length;
                var request = new PutObjectRequest
                {
                    BucketName = bucket,
                    Key = key,
                    InputStream = payload,
                    AutoCloseStream = false,
                    ContentType = AuctionBlobKeys.ContentType,
                    // R2 and some other S3-compatible providers reject chunked SigV4 payload
                    // signing. We sign headers but not the body; integrity is covered by HTTPS
                    // and by the uncached read-back verification of the archive sweep.
                    DisablePayloadSigning = true,
                };
                conditions?.Invoke(request);
                var response = await client.PutObjectAsync(request, cancellationToken).ConfigureAwait(false);
                blobWrites.Inc();
                blobWriteBytes.Observe(length);
                return response;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (AmazonS3Exception ex) when (conditions != null && (IsPreconditionFailure(ex) || IsNotImplemented(ex)))
            {
                throw; // handled by WriteIndexAsync
            }
            catch (Exception ex)
            {
                blobWriteErrors.Inc();
                logger.LogWarning(ex, "Archive blob write failed for {Key}", key);
                throw;
            }
        }

        private void CacheBlock(string key, ArchivedAuctionBlock block, long compressedBytes)
        {
            // Billed at max(compressed size, decoded size estimate) + per-entry overhead: decoded blocks are
            // several times larger than the wire size and the limit must bound real memory.
            var size = Math.Max(1, Math.Max(compressedBytes, block.EstimatedBytes()) + perEntryOverheadBytes);
            cache.Set(key, block, new MemoryCacheEntryOptions
            {
                Size = size,
                SlidingExpiration = cacheTtl,
                AbsoluteExpirationRelativeToNow = TimeSpan.FromHours(1),
                Priority = CacheItemPriority.Normal,
            });
        }

        /// <summary>Disposes the cache; the client belongs to the DI container.</summary>
        public void Dispose()
        {
            cache?.Dispose();
        }

        private static long ConfigLong(IConfiguration config, string key, long fallback)
        {
            return long.TryParse(config?[key], out var v) ? v : fallback;
        }
    }
}
