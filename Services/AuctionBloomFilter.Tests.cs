using System;
using System.Collections.Generic;
using NUnit.Framework;

#pragma warning disable CS1591 // Missing XML comment

namespace Coflnet.Sky.Auctions.Services;

[TestFixture]
public class AuctionBloomFilterTests
{
    [Test]
    public void Add_AndMayContain_ReturnsTrueForAddedItems()
    {
        // Arrange
        var filter = new AuctionBloomFilter("test", expectedItems: 1000, falsePositiveRate: 0.01);
        var uuid1 = Guid.NewGuid();
        var uuid2 = Guid.NewGuid();

        // Act
        filter.Add(uuid1);
        filter.Add(uuid2);

        // Assert
        Assert.That(filter.MayContain(uuid1), Is.True);
        Assert.That(filter.MayContain(uuid2), Is.True);
    }

    [Test]
    public void MayContain_ReturnsFalseForItemsNotAdded_WithHighProbability()
    {
        // Arrange
        var filter = new AuctionBloomFilter("test", expectedItems: 1000, falsePositiveRate: 0.01);
        
        // Add some items
        for (int i = 0; i < 100; i++)
        {
            filter.Add(Guid.NewGuid());
        }

        // Act & Assert - Test with items not added
        var falsePositiveCount = 0;
        var testCount = 1000;
        for (int i = 0; i < testCount; i++)
        {
            if (filter.MayContain(Guid.NewGuid()))
            {
                falsePositiveCount++;
            }
        }

        // False positive rate should be around 1% (with some margin)
        var actualFalsePositiveRate = (double)falsePositiveCount / testCount;
        Assert.That(actualFalsePositiveRate, Is.LessThan(0.05), 
            $"False positive rate {actualFalsePositiveRate:P2} is too high");
    }

    [Test]
    public void Serialize_AndDeserialize_PreservesData()
    {
        // Arrange
        var filter = new AuctionBloomFilter("test-serialize", expectedItems: 1000, falsePositiveRate: 0.01);
        var uuids = new List<Guid>();
        for (int i = 0; i < 50; i++)
        {
            var uuid = Guid.NewGuid();
            uuids.Add(uuid);
            filter.Add(uuid);
        }

        // Act
        var serialized = filter.Serialize();
        var deserialized = AuctionBloomFilter.Deserialize(serialized);

        // Assert
        Assert.That(deserialized.Tag, Is.EqualTo("test-serialize"));
        Assert.That(deserialized.BitCount, Is.EqualTo(filter.BitCount));
        Assert.That(deserialized.HashCount, Is.EqualTo(filter.HashCount));
        Assert.That(deserialized.ItemCount, Is.EqualTo(50));

        foreach (var uuid in uuids)
        {
            Assert.That(deserialized.MayContain(uuid), Is.True, 
                $"UUID {uuid} should be in deserialized filter");
        }
    }

    [Test]
    public void Merge_CombinesFilters()
    {
        // Arrange
        var filter1 = new AuctionBloomFilter("test", expectedItems: 1000, falsePositiveRate: 0.01);
        var filter2 = new AuctionBloomFilter("test", expectedItems: 1000, falsePositiveRate: 0.01);
        
        var uuid1 = Guid.NewGuid();
        var uuid2 = Guid.NewGuid();
        
        filter1.Add(uuid1);
        filter2.Add(uuid2);

        // Act
        filter1.Merge(filter2);

        // Assert
        Assert.That(filter1.MayContain(uuid1), Is.True);
        Assert.That(filter1.MayContain(uuid2), Is.True);
        Assert.That(filter1.ItemCount, Is.EqualTo(2));
    }

    [Test]
    public void EstimatedFalsePositiveRate_IsReasonable()
    {
        // Arrange
        var filter = new AuctionBloomFilter("test", expectedItems: 10000, falsePositiveRate: 0.01);
        
        // Add items
        for (int i = 0; i < 5000; i++)
        {
            filter.Add(Guid.NewGuid());
        }

        // Act
        var estimatedRate = filter.EstimatedFalsePositiveRate();

        // Assert - should be lower than target since we only added half the expected items
        Assert.That(estimatedRate, Is.LessThan(0.01));
        Assert.That(estimatedRate, Is.GreaterThan(0)); // Should be non-zero
    }
}

[TestFixture]
public class MultiLevelBloomFilterIndexTests
{
    [Test]
    public void Add_AndMayContain_ReturnsCorrectLocations()
    {
        // Arrange
        var index = new MultiLevelBloomFilterIndex();
        var uuid = Guid.NewGuid();

        // Act
        index.Add(uuid, "DIAMOND_SWORD", 2024, 1);

        // Assert
        var (mayExist, locations) = index.MayContain(uuid);
        Assert.That(mayExist, Is.True);
        Assert.That(locations, Has.Count.EqualTo(1));
        Assert.That(locations[0].tag, Is.EqualTo("DIAMOND_SWORD"));
        Assert.That(locations[0].year, Is.EqualTo(2024));
        Assert.That(locations[0].month, Is.EqualTo(1));
    }

    [Test]
    public void MayContain_ReturnsFalseForUnknownUuid()
    {
        // Arrange
        var index = new MultiLevelBloomFilterIndex();
        index.Add(Guid.NewGuid(), "DIAMOND_SWORD", 2024, 1);

        // Act
        var (mayExist, locations) = index.MayContain(Guid.NewGuid());

        // Assert - with high probability, a new GUID shouldn't be found
        // (could have false positives, but unlikely)
        Assert.That(mayExist, Is.False);
        Assert.That(locations, Has.Count.EqualTo(0));
    }

    [Test]
    public void Serialize_AndDeserialize_PreservesData()
    {
        // Arrange
        var index = new MultiLevelBloomFilterIndex();
        var uuid = Guid.NewGuid();
        index.Add(uuid, "ENCHANTED_BOOK", 2023, 6);

        // Act
        var serialized = index.Serialize();
        var deserialized = MultiLevelBloomFilterIndex.Deserialize(serialized);

        // Assert
        var (mayExist, locations) = deserialized.MayContain(uuid);
        Assert.That(mayExist, Is.True);
        Assert.That(locations, Has.Count.EqualTo(1));
    }

    [Test]
    public void Add_MultipleTagsAndMonths_TracksAll()
    {
        // Arrange
        var index = new MultiLevelBloomFilterIndex();
        var uuid1 = Guid.NewGuid();
        var uuid2 = Guid.NewGuid();
        var uuid3 = Guid.NewGuid();

        // Act
        index.Add(uuid1, "DIAMOND_SWORD", 2024, 1);
        index.Add(uuid2, "DIAMOND_SWORD", 2024, 2);
        index.Add(uuid3, "GOLDEN_APPLE", 2024, 1);

        // Assert
        Assert.That(index.TagLocations.Keys, Has.Count.EqualTo(2));
        Assert.That(index.TagLocations["DIAMOND_SWORD"].YearMonths, Has.Count.EqualTo(2));
        Assert.That(index.TagLocations["GOLDEN_APPLE"].YearMonths, Has.Count.EqualTo(1));
    }
}
