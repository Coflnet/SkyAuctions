using System;
using Cassandra;
using Coflnet.Sky.Core.Archive;
using Microsoft.Extensions.Configuration;

namespace Coflnet.Sky.Auctions.Archive;

/// <summary>
/// <c>ARCHIVE:AUDIT_*</c> settings of the read-only Scylla audit (<see cref="ScyllaArchiveAudit"/>), defaults as in appsettings.json.
/// The page size, retries and pauses come from <see cref="ScyllaArchiveOptions"/>.
/// </summary>
public record ScyllaArchiveAuditOptions
{
    /// <summary>Master switch, off by default.</summary>
    public bool Enabled { get; init; }
    /// <summary>Scylla consistency level of the audit reads (ALL by default: a row only one replica has is still seen).</summary>
    public ConsistencyLevel Consistency { get; init; } = ConsistencyLevel.All;
    /// <summary>First month audited; defaults to the sweep's <c>OLDEST_MONTH</c>.</summary>
    public int OldestMonth { get; init; } = new ScyllaArchiveOptions().OldestMonth;
    /// <summary>Last month audited; defaults to the sweep's <c>NEWEST_MONTH</c>.</summary>
    public int NewestMonth { get; init; } = new ScyllaArchiveOptions().NewestMonth;
    /// <summary>Throttle: partitions read per minute.</summary>
    public int PartitionsPerMinute { get; init; } = 30;
    /// <summary>Random archived auctions per month that are fetched through the public reader path and compared with Scylla.</summary>
    public int SampleReadBack { get; init; } = 200;
    /// <summary>Partitions between two checkpoint writes of a month's partial result.</summary>
    public int CheckpointEveryPartitions { get; init; } = 200;
    /// <summary>Samples kept per class and month.</summary>
    public int MaxSamplesPerClass { get; init; } = 1000;
    /// <summary>Pause between two passes once every month is audited.</summary>
    public int IdlePauseMinutes { get; init; } = 60;

    /// <summary>Read <c>ARCHIVE:AUDIT_*</c> (and the sweep's months as defaults); invalid values fall back to the defaults.</summary>
    public static ScyllaArchiveAuditOptions From(IConfiguration config)
    {
        var sweep = ScyllaArchiveOptions.From(config);
        var d = new ScyllaArchiveAuditOptions { OldestMonth = sweep.OldestMonth, NewestMonth = sweep.NewestMonth };
        var s = config.GetSection("ARCHIVE");
        var (oldest, newest) = Months(s["AUDIT_MONTHS"], d.OldestMonth, d.NewestMonth);
        return new ScyllaArchiveAuditOptions
        {
            Enabled = bool.TryParse(s["AUDIT_ENABLED"], out var enabled) && enabled,
            Consistency = Enum.TryParse<ConsistencyLevel>(s["AUDIT_CONSISTENCY"], true, out var cl) ? cl : d.Consistency,
            OldestMonth = oldest,
            NewestMonth = newest,
            PartitionsPerMinute = Math.Max(1, Int(s["AUDIT_PARTITIONS_PER_MINUTE"], d.PartitionsPerMinute)),
            SampleReadBack = Math.Max(0, Int(s["AUDIT_SAMPLE_READBACK"], d.SampleReadBack)),
            CheckpointEveryPartitions = Math.Max(1, Int(s["AUDIT_CHECKPOINT_PARTITIONS"], d.CheckpointEveryPartitions)),
            MaxSamplesPerClass = Math.Max(0, Int(s["AUDIT_MAX_SAMPLES"], d.MaxSamplesPerClass)),
            IdlePauseMinutes = Math.Max(1, Int(s["AUDIT_IDLE_PAUSE_MINUTES"], d.IdlePauseMinutes)),
        };
    }

    /// <summary><c>yyyy-MM..yyyy-MM</c> (or a single month); anything else falls back.</summary>
    private static (int Oldest, int Newest) Months(string value, int oldest, int newest)
    {
        if (string.IsNullOrWhiteSpace(value))
            return (oldest, newest);
        try
        {
            var parts = value.Split("..", StringSplitOptions.TrimEntries);
            var from = ArchiveMonths.Parse(parts[0]);
            var to = parts.Length > 1 ? ArchiveMonths.Parse(parts[1]) : from;
            return from <= to ? (from, to) : (oldest, newest);
        }
        catch (Exception ex) when (ex is FormatException || ex is ArgumentOutOfRangeException)
        {
            return (oldest, newest);
        }
    }

    private static int Int(string value, int fallback) => int.TryParse(value, out var v) ? v : fallback;
}
