using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Coflnet.Sky.Auctions.Services;

public class S3StorageService
{
    private readonly IAmazonS3 client;
    private readonly string bucketName;
    private readonly ILogger<S3StorageService> logger;

    public S3StorageService(IConfiguration config, ILogger<S3StorageService> logger)
        : this(CreateClient(config), config, logger)
    {
    }

    /// <summary>
    /// Creates a new S3StorageService using an injected S3 client
    /// </summary>
    public S3StorageService(IAmazonS3 client, IConfiguration config, ILogger<S3StorageService> logger)
    {
        this.client = client;
        this.logger = logger;
        bucketName = config["S3:BUCKET_NAME"] ?? config["S3:BucketName"] ?? "sky-auctions";
    }

    public async Task EnsureBucket(CancellationToken ct = default)
    {
        try
        {
            await client.PutBucketAsync(new PutBucketRequest { BucketName = bucketName }, ct);
        }
        catch (AmazonS3Exception e) when (e.ErrorCode == "BucketAlreadyOwnedByYou" || e.ErrorCode == "BucketAlreadyExists")
        {
            // bucket exists, nothing to do
        }
    }

    public async Task PutBlob(string key, byte[] data, CancellationToken ct = default)
    {
        await PutBlob(key, data, null, ct);
    }

    public async Task PutBlob(string key, byte[] data, string contentType, CancellationToken ct = default)
    {
        using var stream = new MemoryStream(data);
        var request = new PutObjectRequest
        {
            BucketName = bucketName,
            Key = key,
            InputStream = stream,
            UseChunkEncoding = false,
            ContentType = contentType
        };
        await client.PutObjectAsync(request, ct);
    }

    public async Task<byte[]> GetBlob(string key, CancellationToken ct = default)
    {
        var response = await client.GetObjectAsync(bucketName, key, ct);
        using var ms = new MemoryStream();
        await response.ResponseStream.CopyToAsync(ms, ct);
        return ms.ToArray();
    }

    public async Task<bool> HeadBlob(string key, CancellationToken ct = default)
    {
        try
        {
            await client.GetObjectMetadataAsync(bucketName, key, ct);
            return true;
        }
        catch (AmazonS3Exception e) when (e.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return false;
        }
    }

    public async Task<List<string>> ListBlobs(string prefix, CancellationToken ct = default)
    {
        var result = new List<string>();
        var request = new ListObjectsV2Request
        {
            BucketName = bucketName,
            Prefix = prefix
        };
        ListObjectsV2Response response;
        do
        {
            response = await client.ListObjectsV2Async(request, ct);
            foreach (var obj in response.S3Objects)
                result.Add(obj.Key);
            request.ContinuationToken = response.NextContinuationToken;
        } while (response.IsTruncated == true);
        return result;
    }

    public string GetSignedDownloadUrl(string key, TimeSpan duration)
    {
        var request = new GetPreSignedUrlRequest
        {
            BucketName = bucketName,
            Key = key,
            Expires = DateTime.UtcNow.Add(duration),
            Verb = HttpVerb.GET
        };

        return client.GetPreSignedURL(request);
    }

    public async Task DeleteBlob(string key, CancellationToken ct = default)
    {
        await client.DeleteObjectAsync(new DeleteObjectRequest
        {
            BucketName = bucketName,
            Key = key
        }, ct);
    }

    private static IAmazonS3 CreateClient(IConfiguration config)
    {
        var s3Config = new AmazonS3Config
        {
            ServiceURL = config["S3:SERVICE_URL"] ?? config["S3:Endpoint"] ?? "http://localhost:9000",
            ForcePathStyle = true
        };

        return new AmazonS3Client(
            config["S3:ACCESS_KEY"] ?? config["S3:AccessKey"] ?? "minioadmin",
            config["S3:SECRET_KEY"] ?? config["S3:SecretKey"] ?? "minioadmin",
            s3Config);
    }
}
