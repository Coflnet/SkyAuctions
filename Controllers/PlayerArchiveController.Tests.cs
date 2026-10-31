using System;
using NUnit.Framework;

namespace Coflnet.Sky.Auctions.Controllers;

public class PlayerArchiveControllerTests
{
    [TestCase(2020, 2021)]
    [TestCase(0, 0)]
    [TestCase(-5, 99999)]
    [TestCase(int.MinValue, int.MaxValue)]
    public void ArchiveYearRangeNeverThrows(int startYear, int endYear)
    {
        Assert.DoesNotThrow(() => PlayerArchiveController.ArchiveYearRange(startYear, endYear));
    }

    [Test]
    public void ArchiveYearRangeClampsToArchiveYears()
    {
        var (from, to) = PlayerArchiveController.ArchiveYearRange(0, 99999);

        Assert.That(from, Is.EqualTo(new DateTime(2019, 1, 1, 0, 0, 0, DateTimeKind.Utc)));
        Assert.That(to, Is.EqualTo(new DateTime(DateTime.UtcNow.Year + 2, 1, 1, 0, 0, 0, DateTimeKind.Utc)));
    }

    [Test]
    public void ArchiveYearRangeKeepsValidYears()
    {
        var (from, to) = PlayerArchiveController.ArchiveYearRange(2021, 2022);

        Assert.That(from, Is.EqualTo(new DateTime(2021, 1, 1, 0, 0, 0, DateTimeKind.Utc)));
        Assert.That(to, Is.EqualTo(new DateTime(2023, 1, 1, 0, 0, 0, DateTimeKind.Utc)));
    }
}
