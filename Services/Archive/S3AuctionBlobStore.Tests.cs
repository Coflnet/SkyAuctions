using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using Coflnet.Sky.Core;
using Coflnet.Sky.Core.Archive;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using NUnit.Framework;

namespace Coflnet.Sky.Auctions.Archive;

/// <summary>Minimal in-memory S3 built with <see cref="DispatchProxy"/> (copied from SkyIndexer's store tests).</summary>
public class ArchiveFakeS3 : DispatchProxy
{
    public Dictionary<string, byte[]> Objects = new();
    public List<PutObjectRequest> Puts = new();
    public Dictionary<string, int> Versions = new();
    public int Gets;
    public int ListPageSize = 1000;
    /// <summary>Answer conditional PUTs with 501 like an endpoint without conditional write support.</summary>
    public bool RejectConditionalPuts;
    /// <summary>Fail the next PUT with this status and error code.</summary>
    public (HttpStatusCode status, string code)? FailNextPut;

    private string ETag(string key) => Versions.TryGetValue(key, out var v) ? $"\"etag-{v}\"" : null;

    public static (IAmazonS3 client, ArchiveFakeS3 state) Create()
    {
        var client = Create<IAmazonS3, ArchiveFakeS3>();
        return (client, (ArchiveFakeS3)(object)client);
    }

    protected override object Invoke(MethodInfo targetMethod, object[] args)
    {
        switch (targetMethod.Name)
        {
            case nameof(IAmazonS3.PutObjectAsync) when args[0] is PutObjectRequest put:
                Puts.Add(put);
                var conditional = put.IfMatch != null || put.IfNoneMatch != null;
                if (FailNextPut is { } fail)
                {
                    FailNextPut = null;
                    return Task.FromException<PutObjectResponse>(new AmazonS3Exception("injected") { StatusCode = fail.status, ErrorCode = fail.code });
                }
                if (conditional && RejectConditionalPuts)
                    return Task.FromException<PutObjectResponse>(new AmazonS3Exception("not implemented") { StatusCode = HttpStatusCode.NotImplemented, ErrorCode = "NotImplemented" });
                if (put.IfMatch != null && put.IfMatch != ETag(put.Key) || put.IfNoneMatch == "*" && Objects.ContainsKey(put.Key))
                    return Task.FromException<PutObjectResponse>(new AmazonS3Exception("precondition failed") { StatusCode = HttpStatusCode.PreconditionFailed, ErrorCode = "PreconditionFailed" });
                using (var ms = new MemoryStream())
                {
                    put.InputStream.CopyTo(ms);
                    Objects[put.Key] = ms.ToArray();
                }
                Versions[put.Key] = Versions.GetValueOrDefault(put.Key) + 1;
                return Task.FromResult(new PutObjectResponse { ETag = ETag(put.Key) });
            case nameof(IAmazonS3.GetObjectAsync) when args[0] is GetObjectRequest get:
                Gets++;
                if (!Objects.TryGetValue(get.Key, out var bytes))
                    return Task.FromException<GetObjectResponse>(new AmazonS3Exception("not found") { StatusCode = HttpStatusCode.NotFound });
                return Task.FromResult(new GetObjectResponse { ResponseStream = new MemoryStream(bytes), ContentLength = bytes.Length, ETag = ETag(get.Key) });
            case nameof(IAmazonS3.GetObjectMetadataAsync) when args[0] is GetObjectMetadataRequest head:
                if (!Objects.ContainsKey(head.Key))
                    return Task.FromException<GetObjectMetadataResponse>(new AmazonS3Exception("not found") { StatusCode = HttpStatusCode.NotFound });
                return Task.FromResult(new GetObjectMetadataResponse());
            case nameof(IAmazonS3.ListObjectsV2Async) when args[0] is ListObjectsV2Request list:
                var page = Objects.Keys.Where(k => k.StartsWith(list.Prefix ?? "", StringComparison.Ordinal)).OrderBy(k => k, StringComparer.Ordinal)
                    .SkipWhile(k => list.ContinuationToken != null && string.CompareOrdinal(k, list.ContinuationToken) <= 0).ToList();
                var shown = page.Take(ListPageSize).ToList();
                return Task.FromResult(new ListObjectsV2Response
                {
                    S3Objects = shown.Select(k => new S3Object { Key = k }).ToList(),
                    IsTruncated = page.Count > shown.Count,
                    NextContinuationToken = page.Count > shown.Count ? shown[^1] : null,
                });
            case nameof(IDisposable.Dispose):
                return null;
            default:
                throw new NotSupportedException(targetMethod.Name);
        }
    }
}

public class S3AuctionBlobStoreTests
{
    private static IConfiguration Config(params (string key, string value)[] values)
        => new ConfigurationBuilder().AddInMemoryCollection(values.ToDictionary(v => v.key, v => v.value)).Build();

    private static (S3AuctionBlobStore store, ArchiveFakeS3 s3) Store(params (string key, string value)[] values)
    {
        var (client, s3) = ArchiveFakeS3.Create();
        var config = values.Length > 0 ? Config(values) : Config(("S3:ENABLED", "true"), ("S3:BUCKET_NAME", "sky-auctions-archive"));
        return (new S3AuctionBlobStore(client, config, NullLogger<S3AuctionBlobStore>.Instance), s3);
    }

    private static ArchivedAuctionBlock Block()
    {
        var rows = Enumerable.Range(1, 3).Select(i => new SaveAuction
        {
            Uuid = i.ToString("x32"), UId = i, Tag = "HYPERION", End = new DateTime(2020, 9, 1).AddHours(i), Start = new DateTime(2020, 8, 30),
            NbtData = new NbtData { data = Enumerable.Repeat((byte)i, 50).ToArray() },
        });
        return ArchivedAuctionBlock.FromAuctions("HYPERION", 202009, 0, ArchivedAuctionBlock.SourceScylla, rows);
    }

    [Test]
    public async Task WriteThenUncachedReadRoundTripsUnderTheV2KeyInTheConfiguredBucket()
    {
        var (store, s3) = Store();
        Assert.That(store.IsEnabled, Is.True);
        var block = Block();
        await store.WriteBlockAsync(block);
        var put = s3.Puts.Single();
        Assert.That(put.Key, Is.EqualTo("auctions/v2/HYPERION/2020-09/000.msgpack.br"));
        Assert.That(put.BucketName, Is.EqualTo("sky-auctions-archive"));
        Assert.That(put.DisablePayloadSigning, Is.True, "R2 rejects chunked payload signing");
        Assert.That(block.SameRows(await store.ReadBlockAsync("HYPERION", 202009, 0, uncached: true)), Is.True);
        Assert.That(await store.BlockExistsAsync("HYPERION", 202009, 1), Is.False);
    }

    [TestCase("false", "sky-auctions-archive")]
    [TestCase("true", "")]
    public async Task DisabledWithoutS3EnabledOrBucket(string enabled, string bucket)
    {
        var (store, _) = Store(("S3:ENABLED", enabled), ("S3:BUCKET_NAME", bucket));
        Assert.That(store.IsEnabled, Is.False);
        Assert.That(await store.ReadBlockAsync("HYPERION", 202009, 0), Is.Null);
    }

    [Test]
    public void BucketFallsBackToTheSkyIndexerKey()
    {
        Assert.That(S3AuctionBlobStore.BucketName(Config(("S3:BUCKET", "other"))), Is.EqualTo("other"));
        Assert.That(S3AuctionBlobStore.BucketName(Config(("S3:BUCKET", "other"), ("S3:BUCKET_NAME", "named"))), Is.EqualTo("named"));
    }

    [Test]
    public async Task MetaObjectsRoundTripUnderTheMetaPrefix()
    {
        var (store, s3) = Store();
        Assert.That(await store.ReadMetaAsync<ScyllaPartitionList>(ScyllaArchiveSweep.PartitionListKey), Is.Null);
        var list = new ScyllaPartitionList { CreatedAt = new DateTime(2026, 10, 23, 0, 0, 0, DateTimeKind.Utc), Partitions = new() { ("HYPERION", 87), ("ENCHANTED_BOOK", 1218) }, OldestMonth = 201907, NewestMonth = 202103 };
        await store.WriteMetaAsync(ScyllaArchiveSweep.PartitionListKey, list);
        Assert.That(s3.Puts.Single().Key, Is.EqualTo("auctions/v2/_meta/scylla_partitions.msgpack.br"));
        var read = await store.ReadMetaAsync<ScyllaPartitionList>(ScyllaArchiveSweep.PartitionListKey);
        Assert.That(read.Partitions, Is.EqualTo(list.Partitions));
        Assert.That(read.CreatedAt, Is.EqualTo(list.CreatedAt));
        Assert.That((read.OldestMonth, read.NewestMonth), Is.EqualTo((201907, 202103)));
    }

    private static (S3AuctionBlobStore store, ArchiveFakeS3 s3, ListLogger<S3AuctionBlobStore> logger) ConditionalStore()
    {
        var (client, s3) = ArchiveFakeS3.Create();
        var logger = new ListLogger<S3AuctionBlobStore>();
        var config = Config(("S3:ENABLED", "true"), ("S3:BUCKET_NAME", "sky-auctions-archive"));
        return (new S3AuctionBlobStore(client, config, logger), s3, logger);
    }

    [Test]
    public async Task IndexWritesAreConditional_AndAConflictSurfacesAsArchiveIndexConflictException()
    {
        var (store, s3, _) = ConditionalStore();
        var created = new ArchivedTagIndex { Tag = "HYPERION" }.Upsert(new ArchivedMonthEntry { Month = 202009, Rows = 1 });
        await store.WriteIndexAsync(created);
        Assert.That(s3.Puts[0].IfNoneMatch, Is.EqualTo("*"), "a new index is only created if nobody else created it");
        Assert.That(s3.Puts[0].IfMatch, Is.Null);
        Assert.That(created.ETag, Is.EqualTo("\"etag-1\""), "the write returns the new ETag");

        var first = await store.ReadIndexAsync("HYPERION");
        var second = await store.ReadIndexAsync("HYPERION");
        Assert.That(first.ETag, Is.EqualTo("\"etag-1\""));
        await store.WriteIndexAsync(first.Upsert(new ArchivedMonthEntry { Month = 202010, Rows = 2 }));
        Assert.That(s3.Puts[1].IfMatch, Is.EqualTo("\"etag-1\""));
        Assert.That(s3.Puts[1].IfNoneMatch, Is.Null);

        // the second reader still holds etag-1: 412 becomes the core conflict exception and nothing is overwritten
        var conflict = Assert.ThrowsAsync<ArchiveIndexConflictException>(() => store.WriteIndexAsync(second.Upsert(new ArchivedMonthEntry { Month = 202011, Rows = 3 })));
        Assert.That(conflict.Tag, Is.EqualTo("HYPERION"));
        var stored = await store.ReadIndexAsync("HYPERION");
        Assert.That(stored.Months.Select(m => m.Month), Is.EqualTo(new[] { 202009, 202010 }));

        // creating an index that meanwhile exists is a conflict as well
        Assert.ThrowsAsync<ArchiveIndexConflictException>(() => store.WriteIndexAsync(new ArchivedTagIndex { Tag = "HYPERION" }));
    }

    [TestCase(HttpStatusCode.PreconditionFailed, null)]
    [TestCase(HttpStatusCode.Conflict, "ConditionalRequestConflict")]
    [TestCase(HttpStatusCode.BadRequest, "PreconditionFailed")]
    public void PreconditionErrorsMapToTheConflictException(HttpStatusCode status, string code)
    {
        var (store, s3, _) = ConditionalStore();
        s3.FailNextPut = (status, code);
        Assert.ThrowsAsync<ArchiveIndexConflictException>(() => store.WriteIndexAsync(new ArchivedTagIndex { Tag = "HYPERION" }));
    }

    [Test]
    public void OtherWriteErrorsAreNotConflicts()
    {
        var (store, s3, _) = ConditionalStore();
        s3.FailNextPut = (HttpStatusCode.InternalServerError, "InternalError");
        Assert.ThrowsAsync<AmazonS3Exception>(() => store.WriteIndexAsync(new ArchivedTagIndex { Tag = "HYPERION" }));
    }

    [Test]
    public async Task EndpointWithoutConditionalPut_FallsBackToUnconditionalWritesAndWarnsOnce()
    {
        var (store, s3, logger) = ConditionalStore();
        s3.RejectConditionalPuts = true;
        await store.WriteIndexAsync(new ArchivedTagIndex { Tag = "HYPERION" }.Upsert(new ArchivedMonthEntry { Month = 202009 }));
        await store.WriteIndexAsync(new ArchivedTagIndex { Tag = "TERMINATOR" });
        Assert.That(s3.Puts.Select(p => (p.IfNoneMatch, p.IfMatch)), Is.EqualTo(new (string, string)[] { ("*", null), (null, null), (null, null) }), "one rejected conditional PUT, then plain PUTs");
        Assert.That((await store.ReadIndexAsync("HYPERION")).Find(202009), Is.Not.Null);
        Assert.That(logger.Entries.Count(e => e.level == Microsoft.Extensions.Logging.LogLevel.Warning && e.message.Contains("conditional PUT")), Is.EqualTo(1));
    }

    [Test]
    public async Task MetaObjects_ConditionalWritesVersionedReadsAndPaginatedListing()
    {
        var (store, s3, _) = ConditionalStore();
        var key = AuctionBlobKeys.ScyllaManifest(202009);
        Assert.That(await store.ReadMetaVersionedAsync<ScyllaMonthManifest>(key), Is.EqualTo(((ScyllaMonthManifest)null, (string)null)));

        var etag = await store.WriteMetaConditionalAsync(key, new ScyllaMonthManifest { Month = 202009 }, null);
        Assert.That(s3.Puts[^1].IfNoneMatch, Is.EqualTo("*"));
        Assert.That(s3.Puts[^1].Key, Is.EqualTo("auctions/v2/_meta/scylla/2020-09.msgpack.br"));
        var (read, readEtag) = await store.ReadMetaVersionedAsync<ScyllaMonthManifest>(key);
        Assert.That((read.Month, readEtag), Is.EqualTo((202009, etag)));

        await store.WriteMetaConditionalAsync(key, read.Upsert(new ScyllaTagImport { Tag = "HYPERION" }), readEtag);
        Assert.That(s3.Puts[^1].IfMatch, Is.EqualTo(readEtag));
        Assert.ThrowsAsync<ArchiveIndexConflictException>(() => store.WriteMetaConditionalAsync(key, read, readEtag), "stale etag");
        Assert.ThrowsAsync<ArchiveIndexConflictException>(() => store.WriteMetaConditionalAsync(key, read, null), "already exists");

        await store.WriteMetaAsync(AuctionBlobKeys.ScyllaManifest(202008), new ScyllaMonthManifest { Month = 202008 });
        await store.WriteMetaAsync("scylla_partitions.msgpack.br", new ScyllaMonthManifest());
        await store.WriteBlockAsync(ArchivedAuctionBlock.FromAuctions("HYPERION", 202009, 0, ArchivedAuctionBlock.SourceScylla, Array.Empty<SaveAuction>()));
        s3.ListPageSize = 1;
        Assert.That(await store.ListMetaKeysAsync(AuctionBlobKeys.ScyllaManifestPrefix), Is.EqualTo(new[]
        {
            "auctions/v2/_meta/scylla/2020-08.msgpack.br",
            "auctions/v2/_meta/scylla/2020-09.msgpack.br",
        }), "all pages, only the prefix");
    }
}
