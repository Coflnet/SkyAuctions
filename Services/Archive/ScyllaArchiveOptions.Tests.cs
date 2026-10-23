using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using NUnit.Framework;

namespace Coflnet.Sky.Auctions.Archive;

public class ScyllaArchiveOptionsTests
{
    private static IConfiguration Config(params (string key, string value)[] values)
    {
        var dict = new Dictionary<string, string>();
        foreach (var (key, value) in values)
            dict["ARCHIVE:" + key] = value;
        return new ConfigurationBuilder().AddInMemoryCollection(dict).Build();
    }

    [Test]
    public void DefaultsCoverTheScyllaOnlyMonths()
    {
        var o = ScyllaArchiveOptions.From(Config());
        Assert.That(o.Enabled, Is.False);
        Assert.That((o.OldestMonth, o.NewestMonth), Is.EqualTo((201907, 202103)));
        Assert.That(o.PageSize, Is.EqualTo(500));
        Assert.That(o.EffectiveMaxPart, Is.EqualTo(254), "archived_months.parts is a tinyint part count");
        Assert.That((o.ImportWaitMinutes, o.IdlePauseMinutes), Is.EqualTo((5, 60)));
    }

    [Test]
    public void ShippedAppsettings_MatchTheCodeDefaults()
    {
        // appsettings.json wins over the code defaults (SkyIndexer shipped stale values once, see its ArchiveOptionsTests)
        var dir = new System.IO.DirectoryInfo(System.AppContext.BaseDirectory);
        string path = null;
        for (; dir != null && path == null; dir = dir.Parent)
        {
            var candidate = System.IO.Path.Combine(dir.FullName, "appsettings.json");
            if (System.IO.File.Exists(candidate) && System.IO.File.Exists(System.IO.Path.Combine(dir.FullName, "SkyAuctions.csproj")))
                path = candidate;
        }
        Assert.That(path, Is.Not.Null, "appsettings.json next to SkyAuctions.csproj not found");
        var configuration = new ConfigurationBuilder().AddJsonFile(path).Build();
        Assert.That(configuration.GetSection("ARCHIVE").Exists(), Is.True, "ARCHIVE section shipped");
        var shipped = ScyllaArchiveOptions.From(configuration);
        Assert.That(shipped, Is.EqualTo(new ScyllaArchiveOptions()));
    }

    [Test]
    public void OverridesAreParsed_InvalidValuesFallBack()
    {
        var o = ScyllaArchiveOptions.From(Config(("ENABLED", "true"), ("NEWEST_MONTH", "2020-12"), ("PAGE_SIZE", "nope"), ("MAX_PART", "999"), ("PAGE_RETRIES", "-3")));
        Assert.That(o.Enabled, Is.True);
        Assert.That(o.NewestMonth, Is.EqualTo(202012));
        Assert.That(o.PageSize, Is.EqualTo(500));
        Assert.That(o.EffectiveMaxPart, Is.EqualTo(254));
        Assert.That(o.PageRetries, Is.Zero);
    }
}
