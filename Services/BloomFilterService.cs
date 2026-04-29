using System;
using System.Buffers.Binary;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3;
using Microsoft.Extensions.Logging;

namespace Coflnet.Sky.Auctions.Services;

/// <summary>
/// In-memory Bloom filter backed by S3 for UUID existence checks.
/// ~960 MB for 800M entries at 1% FPR (9.6 bits/entry, 7 hash functions).
/// </summary>
public class BloomFilterService
{
    private const string BlobKey = "uuid-index/bloom-global.bin";
    private const int HashCount = 7;
    private const int BitsPerWord = sizeof(int) * 8;
    private const int StorageMagic = 0x314D4C42; // BLM1
    private const int HeaderSize = sizeof(int) + sizeof(long);

    private readonly S3StorageService s3;
    private readonly ILogger<BloomFilterService> logger;
    private int[] words = Array.Empty<int>();
    private long bitCount;
    private int dirty;
    private int initialized;

    public BloomFilterService(S3StorageService s3, ILogger<BloomFilterService> logger)
    {
        this.s3 = s3;
        this.logger = logger;
    }

    public async Task Initialize(long expectedItems = 800_000_000, double fpr = 0.01, CancellationToken ct = default)
    {
        await s3.EnsureBucket(ct);
        bitCount = OptimalBitCount(expectedItems, fpr);
        logger.LogInformation("Bloom filter size: {SizeMB} MB for {Items} items", bitCount / 8 / 1024 / 1024, expectedItems);

        try
        {
            var data = await s3.GetBlob(BlobKey, ct);
            if (TryReadHeader(data, out var storedBitCount, out var payloadOffset))
            {
                bitCount = storedBitCount;
            }

            var wordCount = checked((int)((bitCount + BitsPerWord - 1) / BitsPerWord));
            words = new int[wordCount];
            var payloadLength = Math.Min(data.Length - payloadOffset, words.Length * sizeof(int));
            if (payloadLength > 0)
            {
                Buffer.BlockCopy(data, payloadOffset, words, 0, payloadLength);
            }

            if (payloadOffset == 0)
            {
                logger.LogWarning("Loaded legacy Bloom filter payload without size header; assuming configured bit count {BitCount}", bitCount);
            }

            logger.LogInformation("Loaded existing Bloom filter ({SizeMB} MB)", bitCount / 8 / 1024 / 1024);
        }
        catch (AmazonS3Exception e) when (
            e.StatusCode == System.Net.HttpStatusCode.NotFound
            || string.Equals(e.ErrorCode, "NoSuchKey", StringComparison.OrdinalIgnoreCase)
            || string.Equals(e.ErrorCode, "NoSuchBucket", StringComparison.OrdinalIgnoreCase))
        {
            var wordCount = checked((int)((bitCount + BitsPerWord - 1) / BitsPerWord));
            words = new int[wordCount];
            logger.LogInformation("Created new Bloom filter");
        }

        Volatile.Write(ref initialized, 1);
    }

    public bool MightExist(Guid uuid)
    {
        if (Volatile.Read(ref initialized) != 1 || bitCount == 0 || words.Length == 0)
        {
            return false;
        }

        Span<byte> uuidBytes = stackalloc byte[16];
        uuid.TryWriteBytes(uuidBytes);
        var (h1, h2) = DoubleHash(uuidBytes);
        for (int i = 0; i < HashCount; i++)
        {
            var idx = ((h1 + (long)i * h2) & long.MaxValue) % bitCount;
            if (!IsBitSet(idx))
                return false;
        }
        return true;
    }

    public void Add(Guid uuid)
    {
        if (Volatile.Read(ref initialized) != 1 || bitCount == 0 || words.Length == 0)
        {
            throw new InvalidOperationException("Bloom filter must be initialized before use");
        }

        Span<byte> uuidBytes = stackalloc byte[16];
        uuid.TryWriteBytes(uuidBytes);
        var (h1, h2) = DoubleHash(uuidBytes);
        for (int i = 0; i < HashCount; i++)
        {
            var idx = ((h1 + (long)i * h2) & long.MaxValue) % bitCount;
            SetBit(idx);
        }
        Interlocked.Exchange(ref dirty, 1);
    }

    public async Task FlushIfDirty(CancellationToken ct = default)
    {
        if (Volatile.Read(ref initialized) != 1 || words.Length == 0)
        {
            return;
        }

        if (Interlocked.CompareExchange(ref dirty, 0, 1) != 1)
            return;

        var payloadLength = words.Length * sizeof(int);
        var bytes = new byte[HeaderSize + payloadLength];
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(0, sizeof(int)), StorageMagic);
        BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(sizeof(int), sizeof(long)), bitCount);
        Buffer.BlockCopy(words, 0, bytes, HeaderSize, payloadLength);
        await s3.PutBlob(BlobKey, bytes, "application/octet-stream", ct);
        logger.LogInformation("Flushed Bloom filter to S3");
    }

    private bool IsBitSet(long index)
    {
        var wordIndex = checked((int)(index / BitsPerWord));
        var mask = 1 << (int)(index % BitsPerWord);
        return (Volatile.Read(ref words[wordIndex]) & mask) != 0;
    }

    private void SetBit(long index)
    {
        var wordIndex = checked((int)(index / BitsPerWord));
        var mask = 1 << (int)(index % BitsPerWord);

        while (true)
        {
            var current = Volatile.Read(ref words[wordIndex]);
            var updated = current | mask;
            if (current == updated)
            {
                return;
            }

            if (Interlocked.CompareExchange(ref words[wordIndex], updated, current) == current)
            {
                return;
            }
        }
    }

    private static (long h1, long h2) DoubleHash(ReadOnlySpan<byte> data)
    {
        Span<byte> hash = stackalloc byte[32];
        SHA256.HashData(data, hash);
        var h1 = BitConverter.ToInt64(hash) & long.MaxValue;
        var h2 = BitConverter.ToInt64(hash.Slice(8)) & long.MaxValue;
        if (h2 == 0) h2 = 1;
        return (h1, h2);
    }

    private static long OptimalBitCount(long n, double p)
    {
        return (long)Math.Ceiling(-n * Math.Log(p) / (Math.Log(2) * Math.Log(2)));
    }

    private static bool TryReadHeader(byte[] data, out long storedBitCount, out int payloadOffset)
    {
        storedBitCount = 0;
        payloadOffset = 0;

        if (data.Length < HeaderSize)
        {
            return false;
        }

        var magic = BinaryPrimitives.ReadInt32LittleEndian(data.AsSpan(0, sizeof(int)));
        if (magic != StorageMagic)
        {
            return false;
        }

        storedBitCount = BinaryPrimitives.ReadInt64LittleEndian(data.AsSpan(sizeof(int), sizeof(long)));
        payloadOffset = HeaderSize;
        return storedBitCount > 0;
    }
}
