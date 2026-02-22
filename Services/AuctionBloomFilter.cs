using System;
using System.Collections;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using MessagePack;

namespace Coflnet.Sky.Auctions.Services;

#nullable enable

/// <summary>
/// A bloom filter implementation for tracking auction UUIDs
/// Uses multiple hash functions for low false positive rates
/// </summary>
[MessagePackObject]
public class AuctionBloomFilter
{
    /// <summary>
    /// The tag this bloom filter is for (or "master" for the master filter)
    /// </summary>
    [Key(0)]
    public string Tag { get; set; }

    /// <summary>
    /// The bit array storing the bloom filter data
    /// </summary>
    [Key(1)]
    public byte[] BitArray { get; set; }

    /// <summary>
    /// Number of bits in the filter
    /// </summary>
    [Key(2)]
    public int BitCount { get; set; }

    /// <summary>
    /// Number of hash functions to use
    /// </summary>
    [Key(3)]
    public int HashCount { get; set; }

    /// <summary>
    /// Number of items added to the filter
    /// </summary>
    [Key(4)]
    public long ItemCount { get; set; }

    /// <summary>
    /// Version for future compatibility
    /// </summary>
    [Key(5)]
    public int Version { get; set; } = 1;

    /// <summary>
    /// Creates a new bloom filter
    /// </summary>
    /// <param name="tag">Tag identifier for this filter</param>
    /// <param name="expectedItems">Expected number of items to be stored</param>
    /// <param name="falsePositiveRate">Target false positive rate (e.g., 0.01 for 1%)</param>
    public AuctionBloomFilter(string tag, long expectedItems = 1_000_000, double falsePositiveRate = 0.01)
    {
        Tag = tag;

        // Calculate optimal size: m = -n * ln(p) / (ln(2)^2)
        double m = -expectedItems * Math.Log(falsePositiveRate) / (Math.Log(2) * Math.Log(2));
        BitCount = (int)Math.Max(m, 8);

        // Calculate optimal number of hash functions: k = m/n * ln(2)
        double k = (BitCount / (double)expectedItems) * Math.Log(2);
        HashCount = (int)Math.Max(Math.Round(k), 1);

        // Initialize bit array (each byte stores 8 bits)
        BitArray = new byte[(BitCount + 7) / 8];
        ItemCount = 0;
    }

    /// <summary>
    /// Parameterless constructor for deserialization
    /// </summary>
    public AuctionBloomFilter()
    {
        Tag = string.Empty;
        BitArray = Array.Empty<byte>();
    }

    /// <summary>
    /// Adds a UUID to the bloom filter
    /// </summary>
    public void Add(Guid uuid)
    {
        var hashes = GetHashes(uuid);
        foreach (var hash in hashes)
        {
            SetBit(hash);
        }
        ItemCount++;
    }

    /// <summary>
    /// Checks if a UUID might be in the bloom filter
    /// Returns false if definitely not present, true if possibly present
    /// </summary>
    public bool MayContain(Guid uuid)
    {
        var hashes = GetHashes(uuid);
        foreach (var hash in hashes)
        {
            if (!GetBit(hash))
            {
                return false;
            }
        }
        return true;
    }

    /// <summary>
    /// Gets the hash values for a UUID using double hashing technique
    /// </summary>
    private int[] GetHashes(Guid uuid)
    {
        var bytes = uuid.ToByteArray();
        var hashes = new int[HashCount];

        // Use SHA256 for good distribution
        using var sha256 = SHA256.Create();
        var hash = sha256.ComputeHash(bytes);

        // Extract two 64-bit hashes from SHA256 output
        long hash1 = BitConverter.ToInt64(hash, 0);
        long hash2 = BitConverter.ToInt64(hash, 8);

        // Generate k hashes using double hashing: h(i) = (h1 + i * h2) mod m
        for (int i = 0; i < HashCount; i++)
        {
            long combinedHash = hash1 + (i * hash2);
            hashes[i] = (int)(Math.Abs(combinedHash) % BitCount);
        }

        return hashes;
    }

    private void SetBit(int position)
    {
        int byteIndex = position / 8;
        int bitIndex = position % 8;
        BitArray[byteIndex] |= (byte)(1 << bitIndex);
    }

    private bool GetBit(int position)
    {
        int byteIndex = position / 8;
        int bitIndex = position % 8;
        return (BitArray[byteIndex] & (1 << bitIndex)) != 0;
    }

    /// <summary>
    /// Serializes the bloom filter to bytes
    /// </summary>
    public byte[] Serialize()
    {
        return MessagePackSerializer.Serialize(this);
    }

    /// <summary>
    /// Deserializes a bloom filter from bytes
    /// </summary>
    public static AuctionBloomFilter Deserialize(byte[] data)
    {
        return MessagePackSerializer.Deserialize<AuctionBloomFilter>(data);
    }

    /// <summary>
    /// Merges another bloom filter into this one (union operation)
    /// Both filters must have the same size and hash count
    /// </summary>
    public void Merge(AuctionBloomFilter other)
    {
        if (other.BitCount != BitCount || other.HashCount != HashCount)
        {
            throw new ArgumentException("Cannot merge bloom filters with different configurations");
        }

        for (int i = 0; i < BitArray.Length; i++)
        {
            BitArray[i] |= other.BitArray[i];
        }
        ItemCount += other.ItemCount;
    }

    /// <summary>
    /// Estimates the current false positive rate
    /// </summary>
    public double EstimatedFalsePositiveRate()
    {
        // Count set bits
        int setBits = 0;
        for (int i = 0; i < BitArray.Length; i++)
        {
            setBits += CountSetBits(BitArray[i]);
        }

        // FPR ≈ (setBits / m)^k
        double fillRatio = (double)setBits / BitCount;
        return Math.Pow(fillRatio, HashCount);
    }

    private static int CountSetBits(byte b)
    {
        int count = 0;
        while (b != 0)
        {
            count += b & 1;
            b >>= 1;
        }
        return count;
    }

    /// <summary>
    /// Gets approximate memory usage in bytes
    /// </summary>
    public long MemoryUsageBytes => BitArray.Length + 32; // BitArray + overhead
}

/// <summary>
/// Multi-level bloom filter for hierarchical lookup
/// Level 0: Master filter (all auctions)
/// Level 1: Per-tag filters
/// Level 2: Per-tag-per-year filters (optional, for very large tags)
/// </summary>
[MessagePackObject]
public class MultiLevelBloomFilterIndex
{
    /// <summary>
    /// Master bloom filter containing all auction UUIDs
    /// </summary>
    [Key(0)]
    public AuctionBloomFilter MasterFilter { get; set; }

    /// <summary>
    /// Mapping from tag to its location info (year/month combinations)
    /// </summary>
    [Key(1)]
    public Dictionary<string, TagLocationIndex> TagLocations { get; set; }

    /// <summary>
    /// Creates a new multi-level bloom filter index
    /// </summary>
    public MultiLevelBloomFilterIndex()
    {
        MasterFilter = new AuctionBloomFilter("master", expectedItems: 100_000_000, falsePositiveRate: 0.001);
        TagLocations = new Dictionary<string, TagLocationIndex>();
    }

    /// <summary>
    /// Adds an auction to the index
    /// </summary>
    public void Add(Guid uuid, string tag, int year, int month)
    {
        MasterFilter.Add(uuid);

        if (!TagLocations.TryGetValue(tag, out var tagLocation))
        {
            tagLocation = new TagLocationIndex { Tag = tag };
            TagLocations[tag] = tagLocation;
        }

        tagLocation.BloomFilter ??= new AuctionBloomFilter(tag, expectedItems: 1_000_000, falsePositiveRate: 0.01);
        tagLocation.BloomFilter.Add(uuid);

        var yearMonth = year * 100 + month;
        if (!tagLocation.YearMonths.Contains(yearMonth))
        {
            tagLocation.YearMonths.Add(yearMonth);
        }
    }

    /// <summary>
    /// Checks if an auction might exist and returns possible locations
    /// </summary>
    public (bool mayExist, List<(string tag, int year, int month)> possibleLocations) MayContain(Guid uuid)
    {
        if (!MasterFilter.MayContain(uuid))
        {
            return (false, new List<(string, int, int)>());
        }

        var locations = new List<(string tag, int year, int month)>();

        foreach (var (tag, tagLocation) in TagLocations)
        {
            if (tagLocation.BloomFilter?.MayContain(uuid) == true)
            {
                foreach (var yearMonth in tagLocation.YearMonths)
                {
                    locations.Add((tag, yearMonth / 100, yearMonth % 100));
                }
            }
        }

        return (true, locations);
    }

    /// <summary>
    /// Serializes the index to bytes
    /// </summary>
    public byte[] Serialize()
    {
        return MessagePackSerializer.Serialize(this);
    }

    /// <summary>
    /// Deserializes an index from bytes
    /// </summary>
    public static MultiLevelBloomFilterIndex Deserialize(byte[] data)
    {
        return MessagePackSerializer.Deserialize<MultiLevelBloomFilterIndex>(data);
    }
}

/// <summary>
/// Index information for a specific item tag
/// </summary>
[MessagePackObject]
public class TagLocationIndex
{
    /// <summary>
    /// The item tag
    /// </summary>
    [Key(0)]
    public string Tag { get; set; } = string.Empty;

    /// <summary>
    /// Bloom filter for this tag's auctions
    /// </summary>
    [Key(1)]
    public AuctionBloomFilter? BloomFilter { get; set; }

    /// <summary>
    /// List of year*100+month combinations that have data
    /// </summary>
    [Key(2)]
    public List<int> YearMonths { get; set; } = new();
}
