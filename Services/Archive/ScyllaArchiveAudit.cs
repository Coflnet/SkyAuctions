using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using Coflnet.Sky.Core;
using Coflnet.Sky.Core.Archive;
using MessagePack;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Coflnet.Sky.Auctions.Archive;

/// <summary>Classes a Scylla row is audited into.</summary>
public static class AuditClass
{
    /// <summary>R2 has the uid and every non-empty raw Scylla column is equivalent to the R2 row (<see cref="ScyllaAuditEquivalence"/>).</summary>
    public const string Equal = "equal";
    /// <summary>The R2 copy is better (<see cref="ScyllaArchiveRows.IsBetter(Fingerprint, Fingerprint)"/>) and nothing non-empty of the Scylla row is lost.</summary>
    public const string R2BetterSuperset = "r2_better_superset";
    /// <summary>The uid is in R2 but the Scylla row has a non-empty column value R2 lacks or holds differently (the columns are recorded).</summary>
    public const string NeedsMerge = "needs_merge";
    /// <summary>The uid is not in R2 at all (no <c>auction_locations</c> row, or the located block does not hold it).</summary>
    public const string Missing = "missing";
    /// <summary>
    /// End before <see cref="ScyllaArchiveRows.BogusEndBefore"/>: random-timekey garbage the sweep never archives. Every such row is still looked up in R2 and
    /// whether a matching R2 row exists is recorded (<see cref="ScyllaArchiveAudit.BogusInR2"/> / <see cref="ScyllaArchiveAudit.BogusNotInR2"/> in the sample columns,
    /// <see cref="ScyllaAuditPartition.BogusEndInR2"/>), but the row is never compared and <b>bogus_end is never "covered"</b>: a delete step must treat these rows as not archived.
    /// </summary>
    public const string BogusEnd = "bogus_end";
    /// <summary>
    /// The partition could not be read at the configured consistency level even after the page retries. Counted per <b>partition</b> (not per row) in the month's
    /// class counts and never covered; the audit goes on with the next partition and retries the partition in the next pass.
    /// </summary>
    public const string Unreadable = "unreadable";

    /// <summary>Every class.</summary>
    public static readonly string[] All = { Equal, R2BetterSuperset, NeedsMerge, Missing, BogusEnd, Unreadable };
}

/// <summary>One audited row kept as evidence (up to <see cref="ScyllaArchiveAuditOptions.MaxSamplesPerClass"/> per class and month).</summary>
[MessagePackObject]
public class ScyllaAuditSample
{
    /// <summary>Auction uid.</summary>
    [Key(0)] public long Uid { get; set; }
    /// <summary>Auction uuid (N).</summary>
    [Key(1)] public string Uuid { get; set; }
    /// <summary>Scylla partition tag.</summary>
    [Key(2)] public string Tag { get; set; }
    /// <summary>Scylla timekey.</summary>
    [Key(3)] public short TimeKey { get; set; }
    /// <summary>End of the Scylla row.</summary>
    [Key(4)] public DateTime End { get; set; }
    /// <summary>Differing columns (needs_merge), a hint such as <c>block_row_missing</c> (missing) or empty.</summary>
    [Key(5)] public List<string> Columns { get; set; } = new();
}

/// <summary>The audit of one Scylla partition (all of its rows, whatever month they end in).</summary>
[MessagePackObject]
public class ScyllaAuditPartition
{
    /// <summary>Partition tag.</summary>
    [Key(0)] public string Tag { get; set; }
    /// <summary>Partition timekey.</summary>
    [Key(1)] public short TimeKey { get; set; }
    /// <summary>Rows read.</summary>
    [Key(2)] public int Rows { get; set; }
    /// <summary>Smallest End, null for an empty partition.</summary>
    [Key(3)] public DateTime? MinEnd { get; set; }
    /// <summary>Largest End.</summary>
    [Key(4)] public DateTime? MaxEnd { get; set; }
    /// <summary>Largest <c>WRITETIME</c> (microseconds since the epoch) of the audited column.</summary>
    [Key(5)] public long? MaxWriteTime { get; set; }
    /// <summary>Rows per class.</summary>
    [Key(6)] public Dictionary<string, int> ClassCounts { get; set; } = new();
    /// <summary>SHA-256 (hex) over the (uid, end ticks, highest bid, writetime) tuples sorted by those values, 4 x 8 bytes big endian each: changes when the partition does.</summary>
    [Key(7)] public string Sha256 { get; set; }
    /// <summary>True if the partition also holds rows of a neighbouring month (it appears in the results of both).</summary>
    [Key(8)] public bool SpansMonths { get; set; }
    /// <summary>bogus_end rows of the partition for which R2 has a matching row (the rows are still never covered).</summary>
    [Key(9)] public int BogusEndInR2 { get; set; }
    /// <summary>True if the partition could not be read; the other fields are then empty and the partition counts as <see cref="AuditClass.Unreadable"/>.</summary>
    [Key(10)] public bool Unreadable { get; set; }
    /// <summary>Why the partition was unreadable.</summary>
    [Key(11)] public string Error { get; set; }
}

/// <summary>Outcome of the sample read-back of a month.</summary>
[MessagePackObject]
public class ScyllaAuditReadBack
{
    /// <summary>Archived auctions picked.</summary>
    [Key(0)] public int Sampled { get; set; }
    /// <summary>Fetched through the public reader path and equivalent to every Scylla version.</summary>
    [Key(1)] public int Matched { get; set; }
    /// <summary>Not found through the reader or different from a Scylla version.</summary>
    [Key(2)] public int Mismatched { get; set; }
    /// <summary>The Scylla row was gone when it was read again (not counted as match or mismatch).</summary>
    [Key(3)] public int ScyllaGone { get; set; }
    /// <summary>The mismatches (column <c>not_found_by_reader</c> when the reader did not return the auction).</summary>
    [Key(4)] public List<ScyllaAuditSample> Samples { get; set; } = new();
}

/// <summary>
/// Result object of one month, <c>auctions/v2/_meta/audit/scylla/{yyyy-MM}.msgpack.br</c>. Written as a checkpoint every few partitions
/// (<see cref="CompletedAt"/> null, <see cref="NextPartition"/> is where a restart continues) and finally complete.
/// Partitions overlapping two months are in the results of both; the class counts are sums over the partitions listed.
/// </summary>
[MessagePackObject]
public class ScyllaAuditMonth
{
    /// <summary>Month, <c>yyyyMM</c>.</summary>
    [Key(0)] public int Month { get; set; }
    /// <summary>Scylla consistency level the rows were read at.</summary>
    [Key(1)] public string Consistency { get; set; }
    /// <summary>UTC start of the audit of this month.</summary>
    [Key(2)] public DateTime AuditedAt { get; set; }
    /// <summary>UTC time the last partition and the read-back were done; null while running.</summary>
    [Key(3)] public DateTime? CompletedAt { get; set; }
    /// <summary>Index into the sorted partition list of the month where a restart continues.</summary>
    [Key(4)] public int NextPartition { get; set; }
    /// <summary>Partitions of the month.</summary>
    [Key(5)] public int TotalPartitions { get; set; }
    /// <summary>Rows per class.</summary>
    [Key(6)] public Dictionary<string, long> ClassCounts { get; set; } = new();
    /// <summary>needs_merge rows per differing column.</summary>
    [Key(7)] public Dictionary<string, long> ColumnDiffs { get; set; } = new();
    /// <summary>Rows per column that only R2 has a value for (informational).</summary>
    [Key(8)] public Dictionary<string, long> R2OnlyColumns { get; set; } = new();
    /// <summary>Rows with a non-null itemlore / extra / extraattributesjson (columns a block can not carry), keyed by column.</summary>
    [Key(9)] public Dictionary<string, long> NonNullColumns { get; set; } = new();
    /// <summary>Up to 1000 samples per class.</summary>
    [Key(10)] public Dictionary<string, List<ScyllaAuditSample>> Samples { get; set; } = new();
    /// <summary>The partitions audited so far, in order.</summary>
    [Key(11)] public List<ScyllaAuditPartition> Partitions { get; set; } = new();
    /// <summary>Reservoir of archived uids the read-back draws from.</summary>
    [Key(12)] public List<long> SampleCandidates { get; set; } = new();
    /// <summary>Archived rows seen (the reservoir's population).</summary>
    [Key(13)] public long CandidatesSeen { get; set; }
    /// <summary>The sample read-back, null until the month is complete.</summary>
    [Key(14)] public ScyllaAuditReadBack ReadBack { get; set; }
    /// <summary>SHA-256 (hex) over the sorted partition keys (tag, timekey) of the month; a resume only continues when the list is exactly the same.</summary>
    [Key(15)] public string PartitionListSha256 { get; set; }
    /// <summary>bogus_end rows for which R2 has a matching row (informational; bogus_end is never covered).</summary>
    [Key(16)] public long BogusEndInR2 { get; set; }
}

/// <summary>A raw <c>weekly_auctions_2</c> row with the <c>WRITETIME</c> of a regular column.</summary>
public record ScyllaAuditRow(ScyllaAuction Row, long? WriteTime);

/// <summary>
/// Read-only audit of Scylla against the R2 archive (<c>ARCHIVE:AUDIT_ENABLED</c>, default off). For each month of
/// <c>ARCHIVE:AUDIT_MONTHS</c> (default: the sweep's range) it streams every row of every partition overlapping the month at
/// <c>AUDIT_CONSISTENCY</c> (default ALL), looks every uid up in <c>auction_locations</c> (never assuming the tag-month: partitions span two
/// months and re-tagged <c>unknown</c> rows live under another tag), compares it with the located R2 row under the documented
/// <see cref="ScyllaAuditEquivalence"/> and classifies it (<see cref="AuditClass"/>). The only things it writes are its result objects
/// (<see cref="ResultKey"/>, conditional writes) and its Redis progress key (<see cref="ScyllaArchiveMarkerKeys.Audit"/>); it never writes
/// to Scylla, MariaDB or the archive blocks, tag indexes and manifests.
/// </summary>
public class ScyllaArchiveAudit : BackgroundService
{
    /// <summary>Prefix of the result objects.</summary>
    public const string ResultPrefix = AuctionBlobKeys.MetaPrefix + "audit/scylla/";
    /// <summary>Uids per <c>auction_locations</c> lookup (MariaDB stops index dives at 200 IN values).</summary>
    internal const int LocationChunkSize = 100;
    /// <summary>Decoded blocks kept.</summary>
    internal const int BlockCacheSize = 16;
    /// <summary>Archived uids kept per partition as read-back candidates.</summary>
    internal const int CandidatesPerPartition = 5;
    /// <summary>Wait after a failed pass.</summary>
    internal static readonly TimeSpan PassFailureDelay = TimeSpan.FromMinutes(5);

    /// <summary>Sample column of a bogus_end row that has a matching row in R2.</summary>
    public const string BogusInR2 = "r2_row_present";
    /// <summary>Sample column of a bogus_end row R2 does not hold.</summary>
    public const string BogusNotInR2 = "r2_row_absent";
    /// <summary>Sample column of a located row whose part is not (or not with this hash) in the current tag index.</summary>
    public const string BlockNotInIndex = "block_not_in_index";
    /// <summary>Sample column of a located row whose part hash differs from the tag index entry.</summary>
    public const string BlockHashMismatch = "block_hash_mismatch";

    private static readonly Prometheus.Counter auditRows = Prometheus.Metrics.CreateCounter("sky_auctions_archive_audit_rows", "Scylla rows audited against R2, by class", "class");
    private static readonly Prometheus.Counter auditUnreadable = Prometheus.Metrics.CreateCounter("sky_auctions_archive_audit_partitions_unreadable", "Scylla partitions that could not be read at the audit consistency");
    private static readonly Prometheus.Counter auditPartitionsDone = Prometheus.Metrics.CreateCounter("sky_auctions_archive_audit_partitions_done", "Scylla partitions audited");
    private static readonly Prometheus.Counter auditColumnDiffs = Prometheus.Metrics.CreateCounter("sky_auctions_archive_audit_column_diffs", "needs_merge rows by differing column", "column");
    private static readonly Prometheus.Counter auditErrors = Prometheus.Metrics.CreateCounter("sky_auctions_archive_audit_errors", "Scylla audit pass failures");
    private static readonly Prometheus.Counter auditMonthsDone = Prometheus.Metrics.CreateCounter("sky_auctions_archive_audit_months_done", "Months whose audit result is complete");
    private static readonly Prometheus.Counter readBackMismatches = Prometheus.Metrics.CreateCounter("sky_auctions_archive_audit_readback_mismatches", "Sample read-backs through the public reader that differ from Scylla");

    private readonly IAuctionBlobStore store;
    private readonly IArchiveMetaStore meta;
    private readonly IScyllaArchiveMarkers markers;
    private readonly IArchiveLocationLookup locations;
    private readonly ArchivedAuctionReader reader;
    private readonly ScyllaService scylla;
    private readonly IConfiguration config;
    private readonly ILogger<ScyllaArchiveAudit> logger;
    private readonly Dictionary<(string Tag, int Month, int Part), CachedBlock> blocks = new();
    private readonly Queue<(string Tag, int Month, int Part)> blockOrder = new();
    private readonly Dictionary<(string Tag, short TimeKey), PartitionOutcome> carry = new();
    private readonly Dictionary<string, ArchivedTagIndex> indexes = new();
    private ScyllaAuditSchema schema;
    private PreparedStatement partitionStatement;
    private PreparedStatement uidStatement;

    /// <summary>DI constructor.</summary>
    public ScyllaArchiveAudit(IAuctionBlobStore store, IArchiveMetaStore meta, IScyllaArchiveMarkers markers, IArchiveLocationLookup locations,
        ArchivedAuctionReader reader, ScyllaService scylla, IConfiguration config, ILogger<ScyllaArchiveAudit> logger)
    {
        this.store = store;
        this.meta = meta;
        this.markers = markers;
        this.locations = locations;
        this.reader = reader;
        this.scylla = scylla;
        this.config = config;
        this.logger = logger;
    }

    /// <summary>Key of a month's result object.</summary>
    public static string ResultKey(int month) => ResultPrefix + ArchiveMonths.Format(month) + ".msgpack.br";

    private sealed class CachedBlock
    {
        public ArchivedAuctionBlock Block;
        public Dictionary<long, int> RowOf;
        /// <summary>Why the block can not serve as proof (<see cref="BlockNotInIndex"/>, <see cref="BlockHashMismatch"/>), null when it is the part the tag index lists.</summary>
        public string Problem;
    }

    /// <summary>A page of a partition kept failing at the configured consistency level.</summary>
    internal sealed class PartitionUnreadableException : Exception
    {
        /// <summary>Create.</summary>
        public PartitionUnreadableException(string message, Exception inner) : base(message, inner) { }
    }

    private sealed class PartitionOutcome
    {
        public ScyllaAuditPartition Partition;
        public Dictionary<string, List<ScyllaAuditSample>> Samples = new();
        public Dictionary<string, long> ColumnDiffs = new();
        public Dictionary<string, long> R2Only = new();
        public Dictionary<string, long> NonNull = new();
        public List<long> Candidates = new();
    }

    /// <inheritdoc/>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var options = ScyllaArchiveAuditOptions.From(config);
        if (!options.Enabled)
            return;
        if (!store.IsEnabled || meta == null)
        {
            logger.LogWarning("ARCHIVE:AUDIT_ENABLED=true but the S3 archive store is not configured; the Scylla audit will not run.");
            return;
        }
        try
        {
            await Delay(TimeSpan.FromSeconds(ScyllaArchiveOptions.From(config).StartDelaySeconds), stoppingToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return;
        }
        logger.LogInformation("Scylla audit started for {Oldest}..{Newest} at {Consistency}, {Rate} partitions/min",
            ArchiveMonths.Format(options.OldestMonth), ArchiveMonths.Format(options.NewestMonth), options.Consistency, options.PartitionsPerMinute);
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var done = await RunOnceAsync(DateTime.UtcNow, stoppingToken).ConfigureAwait(false);
                logger.LogInformation("Scylla audit pass done ({Done} months completed); next pass in {Minutes} min", done, options.IdlePauseMinutes);
                await Delay(TimeSpan.FromMinutes(options.IdlePauseMinutes), stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                auditErrors.Inc();
                logger.LogError(ex, "Scylla audit pass failed: {Error}; retrying in {Minutes} min", ScyllaArchiveSweep.DescribeException(ex), PassFailureDelay.TotalMinutes);
                try
                {
                    await Delay(PassFailureDelay, stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }
    }

    /// <summary>One pass over the months of the audit range, oldest first. Returns the number of months completed in this pass.</summary>
    internal async Task<int> RunOnceAsync(DateTime nowUtc, CancellationToken cancellationToken)
    {
        var options = ScyllaArchiveAuditOptions.From(config);
        if (!await CheckSchemaAsync(cancellationToken).ConfigureAwait(false))
            return 0;
        var list = await meta.ReadMetaAsync<ScyllaPartitionList>(ScyllaArchiveSweep.PartitionListKey, cancellationToken).ConfigureAwait(false);
        if (list?.Partitions == null)
        {
            logger.LogWarning("Scylla audit: there is no cached partition list ({Key}) yet; the sweep writes it. Nothing to audit", ScyllaArchiveSweep.PartitionListKey);
            return 0;
        }
        if (list.OldestMonth > options.OldestMonth || list.NewestMonth < options.NewestMonth)
        {
            logger.LogError("Scylla audit: the cached partition list covers {ListOldest}..{ListNewest}, the audit range is {Oldest}..{Newest}; the list has to be rescanned for that range first",
                ArchiveMonths.Format(list.OldestMonth), ArchiveMonths.Format(list.NewestMonth), ArchiveMonths.Format(options.OldestMonth), ArchiveMonths.Format(options.NewestMonth));
            return 0;
        }
        var completed = 0;
        carry.Clear(); // partition results are only reused within one pass
        for (var month = options.OldestMonth; month <= options.NewestMonth; month = ArchiveMonths.Next(month))
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (await AuditMonthAsync(month, list, options, nowUtc, cancellationToken).ConfigureAwait(false))
                completed++;
        }
        return completed;
    }

    /// <summary>Partitions of the sweep's list that overlap the month, sorted by tag then timekey.</summary>
    internal static List<(string Tag, short TimeKey)> PartitionsOfMonth(ScyllaPartitionList list, int month)
    {
        var result = new List<(string, short)>();
        foreach (var group in list.Partitions.Where(p => !string.IsNullOrEmpty(p.Tag)).GroupBy(p => p.Tag, StringComparer.Ordinal).OrderBy(g => g.Key, StringComparer.Ordinal))
        {
            var (first, last) = ScyllaArchiveRows.TimeKeysForMonth(group.Key, month);
            result.AddRange(group.Select(p => p.TimeKey).Distinct().Where(k => k >= first && k <= last).OrderBy(k => k).Select(k => (group.Key, k)));
        }
        return result;
    }

    /// <summary>
    /// Read <c>system_schema.columns</c> of the audited table and plan the statements. False (error logged, nothing audited, no result written) when the table has a
    /// column the audit does not know or lacks one it needs.
    /// </summary>
    internal async Task<bool> CheckSchemaAsync(CancellationToken cancellationToken)
    {
        var planned = ScyllaAuditSchema.Build(await ReadSchemaAsync(cancellationToken).ConfigureAwait(false));
        if (!planned.IsUsable)
        {
            auditErrors.Inc();
            logger.LogError("Scylla audit: the schema of {Table} is not the one the audit knows ({Problem}); nothing is audited until the audit (ScyllaAuditSchema.KnownColumns) covers it", ScyllaAuditSchema.Table, planned.Problem);
            return false;
        }
        if (planned.WriteTimeSkipped.Count > 0)
            logger.LogWarning("Scylla audit: no WRITETIME available for {Columns} (absent or non-frozen collection); the partition hash does not see rewrites of only those columns", string.Join(", ", planned.WriteTimeSkipped));
        if (schema == null || schema.PartitionCql != planned.PartitionCql)
        {
            partitionStatement = null;
            uidStatement = null;
        }
        schema = planned;
        return true;
    }

    /// <summary>SHA-256 (hex) over the sorted (tag, timekey) list; equal lists give equal hashes, any added, removed or changed partition changes it.</summary>
    internal static string PartitionListHash(IEnumerable<(string Tag, short TimeKey)> partitions)
    {
        using var sha = SHA256.Create();
        foreach (var (tag, timeKey) in partitions.OrderBy(p => p.Tag, StringComparer.Ordinal).ThenBy(p => p.TimeKey))
        {
            var bytes = Encoding.UTF8.GetBytes($"{tag}\0{timeKey}\n");
            sha.TransformBlock(bytes, 0, bytes.Length, null, 0);
        }
        sha.TransformFinalBlock(Array.Empty<byte>(), 0, 0);
        return Convert.ToHexString(sha.Hash).ToLowerInvariant();
    }

    /// <summary>Audit (or resume, or skip) one month; true when this call completed it.</summary>
    internal async Task<bool> AuditMonthAsync(int month, ScyllaPartitionList list, ScyllaArchiveAuditOptions options, DateTime nowUtc, CancellationToken cancellationToken)
    {
        var partitions = PartitionsOfMonth(list, month);
        var listHash = PartitionListHash(partitions);
        var key = ResultKey(month);
        var (existing, etag) = await meta.ReadMetaVersionedAsync<ScyllaAuditMonth>(key, cancellationToken).ConfigureAwait(false);
        if (IsComplete(existing, options, listHash))
            return await FinishedMonthAsync(existing, options, key, etag, cancellationToken).ConfigureAwait(false);
        if (!await IsMonthImportedAsync(month, cancellationToken).ConfigureAwait(false))
        {
            logger.LogInformation("Scylla audit: {Month} is not imported by SkyIndexer yet (no scylla_at on its sentinel); skipped for now", ArchiveMonths.Format(month));
            return false;
        }
        var state = ResumeOrStart(existing, month, partitions.Count, listHash, options, nowUtc);
        etag = await AuditPartitionsAsync(state, partitions, options, key, etag, cancellationToken).ConfigureAwait(false);
        return await CompleteMonthAsync(state, options, key, etag, cancellationToken).ConfigureAwait(false);
    }

    private static bool IsComplete(ScyllaAuditMonth state, ScyllaArchiveAuditOptions options, string listHash)
        => state?.CompletedAt != null && state.Consistency == options.Consistency.ToString() && state.PartitionListSha256 == listHash;

    /// <summary>The checkpointed state when it is for exactly this partition list and consistency, a new state otherwise.</summary>
    private ScyllaAuditMonth ResumeOrStart(ScyllaAuditMonth existing, int month, int partitionCount, string listHash, ScyllaArchiveAuditOptions options, DateTime nowUtc)
    {
        var monthText = ArchiveMonths.Format(month);
        var resumable = existing != null && existing.CompletedAt == null && existing.Consistency == options.Consistency.ToString()
            && existing.TotalPartitions == partitionCount && existing.PartitionListSha256 == listHash;
        if (resumable)
        {
            logger.LogInformation("Scylla audit: resuming {Month} at partition {Next}/{Total}", monthText, existing.NextPartition, existing.TotalPartitions);
            return existing;
        }
        if (existing != null)
            logger.LogInformation("Scylla audit: {Month} starts over (existing result is complete, or for another consistency or another partition list)", monthText);
        return new ScyllaAuditMonth { Month = month, Consistency = options.Consistency.ToString(), AuditedAt = nowUtc, TotalPartitions = partitionCount, PartitionListSha256 = listHash };
    }

    /// <summary>A finished month: heals the Redis marker and retries its unreadable partitions. True when it became clean in this call.</summary>
    private async Task<bool> FinishedMonthAsync(ScyllaAuditMonth state, ScyllaArchiveAuditOptions options, string key, string etag, CancellationToken cancellationToken)
    {
        if (await markers.GetAsync(ScyllaArchiveMarkerKeys.Audit(state.Month)).ConfigureAwait(false) == null)
            await markers.SetAsync(ScyllaArchiveMarkerKeys.Audit(state.Month), MarkerValue(state)).ConfigureAwait(false);
        if (UnreadableCount(state) == 0)
            return false;
        return await RetryUnreadableAsync(state, options, key, etag, cancellationToken).ConfigureAwait(false);
    }

    private static long UnreadableCount(ScyllaAuditMonth state) => state.ClassCounts.GetValueOrDefault(AuditClass.Unreadable);

    private static string MarkerValue(ScyllaAuditMonth state)
    {
        var iso = ScyllaArchiveMarkerKeys.Iso(state.CompletedAt.Value);
        return UnreadableCount(state) == 0 ? "done:" + iso : FormattableString.Invariant($"unreadable:{UnreadableCount(state)}:{iso}");
    }

    /// <summary>Audit the partitions from <see cref="ScyllaAuditMonth.NextPartition"/> on, checkpointing as configured. Returns the etag of the last write.</summary>
    private async Task<string> AuditPartitionsAsync(ScyllaAuditMonth state, List<(string Tag, short TimeKey)> partitions, ScyllaArchiveAuditOptions options, string key, string etag, CancellationToken cancellationToken)
    {
        var sweepOptions = ScyllaArchiveOptions.From(config);
        var monthStart = ArchiveMonths.MonthRange(state.Month).start;
        var nextMonthStart = ArchiveMonths.MonthRange(ArchiveMonths.Next(state.Month)).start;
        var sinceCheckpoint = 0;
        var newCarry = new Dictionary<(string, short), PartitionOutcome>();
        for (var i = state.NextPartition; i < partitions.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var (tag, timeKey) = partitions[i];
            var read = !carry.Remove((tag, timeKey), out var outcome);
            if (read)
                outcome = await AuditPartitionOrUnreadableAsync(tag, timeKey, options, sweepOptions, cancellationToken).ConfigureAwait(false);
            var (pStart, pEnd) = ScyllaArchiveRows.PartitionRange(tag, timeKey);
            outcome.Partition.SpansMonths = pStart < monthStart || pEnd > nextMonthStart;
            if (pEnd > nextMonthStart)
                newCarry[(tag, timeKey)] = outcome;
            Merge(state, outcome, options);
            state.NextPartition = i + 1;
            auditPartitionsDone.Inc();
            if (++sinceCheckpoint >= options.CheckpointEveryPartitions && state.NextPartition < partitions.Count)
            {
                etag = await Checkpoint(key, state, etag, cancellationToken).ConfigureAwait(false);
                sinceCheckpoint = 0;
            }
            if (read)
                await Delay(TimeSpan.FromMilliseconds(60000.0 / options.PartitionsPerMinute), cancellationToken).ConfigureAwait(false);
        }
        carry.Clear();
        foreach (var (k, v) in newCarry)
            carry[k] = v;
        return etag;
    }

    private async Task<bool> CompleteMonthAsync(ScyllaAuditMonth state, ScyllaArchiveAuditOptions options, string key, string etag, CancellationToken cancellationToken)
    {
        state.ReadBack = await ReadBackAsync(state, options, cancellationToken).ConfigureAwait(false);
        state.CompletedAt = DateTime.UtcNow;
        await Checkpoint(key, state, etag, cancellationToken).ConfigureAwait(false);
        auditMonthsDone.Inc();
        logger.LogInformation("Scylla audit {Month} complete: {Partitions} partitions, {Counts}; read-back {Matched}/{Sampled} matched",
            ArchiveMonths.Format(state.Month), state.Partitions.Count, string.Join(", ", state.ClassCounts.OrderBy(c => c.Key, StringComparer.Ordinal).Select(c => c.Key + "=" + c.Value)), state.ReadBack.Matched, state.ReadBack.Sampled);
        if (UnreadableCount(state) > 0)
            logger.LogWarning("Scylla audit {Month}: {Count} partitions were unreadable and are NOT covered; they are retried in the next pass", ArchiveMonths.Format(state.Month), UnreadableCount(state));
        return true;
    }

    /// <summary>Audit the unreadable partitions of a finished month again and replace their placeholders. True when none is left.</summary>
    private async Task<bool> RetryUnreadableAsync(ScyllaAuditMonth state, ScyllaArchiveAuditOptions options, string key, string etag, CancellationToken cancellationToken)
    {
        var sweepOptions = ScyllaArchiveOptions.From(config);
        var changed = false;
        foreach (var failed in state.Partitions.Where(p => p.Unreadable).ToList())
        {
            cancellationToken.ThrowIfCancellationRequested();
            var outcome = await AuditPartitionOrUnreadableAsync(failed.Tag, failed.TimeKey, options, sweepOptions, cancellationToken).ConfigureAwait(false);
            if (outcome.Partition.Unreadable)
                continue;
            outcome.Partition.SpansMonths = failed.SpansMonths;
            ReplaceUnreadable(state, failed, outcome, options);
            changed = true;
            await Delay(TimeSpan.FromMilliseconds(60000.0 / options.PartitionsPerMinute), cancellationToken).ConfigureAwait(false);
        }
        if (changed)
            await Checkpoint(key, state, etag, cancellationToken).ConfigureAwait(false);
        return UnreadableCount(state) == 0;
    }

    private void ReplaceUnreadable(ScyllaAuditMonth state, ScyllaAuditPartition failed, PartitionOutcome outcome, ScyllaArchiveAuditOptions options)
    {
        state.Partitions.Remove(failed);
        if (state.ClassCounts.GetValueOrDefault(AuditClass.Unreadable) <= 1)
            state.ClassCounts.Remove(AuditClass.Unreadable);
        else
            state.ClassCounts[AuditClass.Unreadable]--;
        state.Samples.GetValueOrDefault(AuditClass.Unreadable)?.RemoveAll(sample => sample.Tag == failed.Tag && sample.TimeKey == failed.TimeKey);
        Merge(state, outcome, options);
    }

    private async Task<string> Checkpoint(string key, ScyllaAuditMonth state, string etag, CancellationToken cancellationToken)
    {
        var newEtag = await meta.WriteMetaConditionalAsync(key, state, etag, cancellationToken).ConfigureAwait(false);
        await markers.SetAsync(ScyllaArchiveMarkerKeys.Audit(state.Month),
            state.CompletedAt != null ? MarkerValue(state)
                : FormattableString.Invariant($"progress:{state.NextPartition}/{state.TotalPartitions}:{ScyllaArchiveMarkerKeys.Iso(DateTime.UtcNow)}")).ConfigureAwait(false);
        return newEtag;
    }

    private void Merge(ScyllaAuditMonth state, PartitionOutcome outcome, ScyllaArchiveAuditOptions options)
    {
        state.Partitions.Add(outcome.Partition);
        state.BogusEndInR2 += outcome.Partition.BogusEndInR2;
        foreach (var (cls, count) in outcome.Partition.ClassCounts)
            state.ClassCounts[cls] = state.ClassCounts.GetValueOrDefault(cls) + count;
        foreach (var (column, count) in outcome.ColumnDiffs)
            state.ColumnDiffs[column] = state.ColumnDiffs.GetValueOrDefault(column) + count;
        foreach (var (column, count) in outcome.R2Only)
            state.R2OnlyColumns[column] = state.R2OnlyColumns.GetValueOrDefault(column) + count;
        foreach (var (column, count) in outcome.NonNull)
            state.NonNullColumns[column] = state.NonNullColumns.GetValueOrDefault(column) + count;
        foreach (var (cls, samples) in outcome.Samples)
        {
            if (!state.Samples.TryGetValue(cls, out var kept))
                state.Samples[cls] = kept = new List<ScyllaAuditSample>();
            kept.AddRange(samples.Take(Math.Max(0, options.MaxSamplesPerClass - kept.Count)));
        }
        var capacity = Math.Max(options.SampleReadBack * 10, 1000);
        foreach (var uid in outcome.Candidates)
        {
            state.CandidatesSeen++;
            if (state.SampleCandidates.Count < capacity)
                state.SampleCandidates.Add(uid);
            else
            {
                var slot = NextInt(state.CandidatesSeen > int.MaxValue ? int.MaxValue : (int)state.CandidatesSeen);
                if (slot < capacity)
                    state.SampleCandidates[slot] = uid;
            }
        }
    }

    // ---------------------------------------------------------------- one partition

    /// <summary>Audit a partition; one that stays unreadable after the page retries is recorded as <see cref="AuditClass.Unreadable"/> instead of failing the pass.</summary>
    private async Task<PartitionOutcome> AuditPartitionOrUnreadableAsync(string tag, short timeKey, ScyllaArchiveAuditOptions options, ScyllaArchiveOptions sweepOptions, CancellationToken cancellationToken)
    {
        try
        {
            return await AuditPartitionAsync(tag, timeKey, options, sweepOptions, cancellationToken).ConfigureAwait(false);
        }
        catch (PartitionUnreadableException ex)
        {
            logger.LogError(ex, "Scylla audit: partition {Tag} timekey {TimeKey} is unreadable at {Consistency}; recorded as unreadable, the audit goes on", tag, timeKey, options.Consistency);
            return UnreadableOutcome(tag, timeKey, ex);
        }
    }

    private static PartitionOutcome UnreadableOutcome(string tag, short timeKey, Exception ex)
    {
        var error = ScyllaArchiveSweep.DescribeException(ex);
        var partition = new ScyllaAuditPartition { Tag = tag, TimeKey = timeKey, Unreadable = true, Error = error };
        partition.ClassCounts[AuditClass.Unreadable] = 1;
        var outcome = new PartitionOutcome { Partition = partition };
        outcome.Samples[AuditClass.Unreadable] = new List<ScyllaAuditSample> { new() { Tag = tag, TimeKey = timeKey, Columns = new List<string> { "partition_unreadable", error } } };
        auditUnreadable.Inc();
        return outcome;
    }

    private async Task<PartitionOutcome> AuditPartitionAsync(string tag, short timeKey, ScyllaArchiveAuditOptions options, ScyllaArchiveOptions sweepOptions, CancellationToken cancellationToken)
    {
        ForgetProofCaches();
        var outcome = new PartitionOutcome { Partition = new ScyllaAuditPartition { Tag = tag, TimeKey = timeKey } };
        var tuples = new List<(long Uid, long EndTicks, long HighestBid, long WriteTime, long Sold)>();
        await foreach (var page in ReadPagesAsync(tag, timeKey, options, sweepOptions, cancellationToken).ConfigureAwait(false))
        {
            var located = await LocateAsync(page.Select(r => ScyllaArchiveRows.UidOf(r.Row)).Distinct().ToList(), cancellationToken).ConfigureAwait(false);
            foreach (var item in page)
                await AuditRowAsync(outcome, tuples, item, located, options, cancellationToken).ConfigureAwait(false);
            await Delay(TimeSpan.FromMilliseconds(sweepOptions.PagePauseMs), cancellationToken).ConfigureAwait(false);
        }
        outcome.Partition.Sha256 = HashTuples(tuples);
        return outcome;
    }

    /// <summary>Blocks and tag indexes are re-read for every partition so that a proof never rests on data cached from before the partition was read.</summary>
    private void ForgetProofCaches()
    {
        blocks.Clear();
        blockOrder.Clear();
        indexes.Clear();
    }

    private async Task AuditRowAsync(PartitionOutcome outcome, List<(long Uid, long EndTicks, long HighestBid, long WriteTime, long Sold)> tuples, ScyllaAuditRow item,
        IReadOnlyDictionary<long, AuctionLocation> located, ScyllaArchiveAuditOptions options, CancellationToken cancellationToken)
    {
        var row = item.Row;
        var uid = ScyllaArchiveRows.UidOf(row);
        var end = ScyllaArchiveRows.EndOf(row);
        var partition = outcome.Partition;
        partition.Rows++;
        partition.MinEnd = partition.MinEnd == null || end < partition.MinEnd ? end : partition.MinEnd;
        partition.MaxEnd = partition.MaxEnd == null || end > partition.MaxEnd ? end : partition.MaxEnd;
        if (item.WriteTime != null)
            partition.MaxWriteTime = Math.Max(partition.MaxWriteTime ?? long.MinValue, item.WriteTime.Value);
        tuples.Add((uid, end.Ticks, row.HighestBidAmount, item.WriteTime ?? 0, row.IsSold ? 1 : 0));
        CountNonNullColumns(outcome, row);
        var (cls, columns) = await ClassifyAsync(row, uid, end, located, cancellationToken).ConfigureAwait(false);
        Record(outcome, row, uid, end, cls, columns, options);
    }

    private static void CountNonNullColumns(PartitionOutcome outcome, ScyllaAuction row)
    {
        if (row.ItemLore != null) outcome.NonNull["itemlore"] = outcome.NonNull.GetValueOrDefault("itemlore") + 1;
        if (row.Extra != null) outcome.NonNull["extra"] = outcome.NonNull.GetValueOrDefault("extra") + 1;
        if (row.ExtraAttributesJson != null) outcome.NonNull["extraattributesjson"] = outcome.NonNull.GetValueOrDefault("extraattributesjson") + 1;
    }

    private void Record(PartitionOutcome outcome, ScyllaAuction row, long uid, DateTime end, string cls, IReadOnlyList<string> columns, ScyllaArchiveAuditOptions options)
    {
        var partition = outcome.Partition;
        partition.ClassCounts[cls] = partition.ClassCounts.GetValueOrDefault(cls) + 1;
        auditRows.WithLabels(cls).Inc();
        if (cls == AuditClass.BogusEnd && columns.Contains(BogusInR2))
            partition.BogusEndInR2++;
        if (cls == AuditClass.NeedsMerge)
        {
            foreach (var column in columns)
            {
                outcome.ColumnDiffs[column] = outcome.ColumnDiffs.GetValueOrDefault(column) + 1;
                auditColumnDiffs.WithLabels(column).Inc();
            }
        }
        if (!outcome.Samples.TryGetValue(cls, out var samples))
            outcome.Samples[cls] = samples = new List<ScyllaAuditSample>();
        if (samples.Count < options.MaxSamplesPerClass)
            samples.Add(new ScyllaAuditSample { Uid = uid, Uuid = row.Uuid.ToString("N"), Tag = row.Tag, TimeKey = partition.TimeKey, End = end, Columns = columns.ToList() });
        if (cls != AuditClass.Missing && cls != AuditClass.BogusEnd && outcome.Candidates.Count < CandidatesPerPartition)
            outcome.Candidates.Add(uid);
    }

    /// <summary>The class of one row and, for needs_merge / missing / bogus_end, the differing columns / a hint.</summary>
    private async Task<(string Class, IReadOnlyList<string> Columns)> ClassifyAsync(ScyllaAuction row, long uid, DateTime end, IReadOnlyDictionary<long, AuctionLocation> located, CancellationToken cancellationToken)
    {
        var (found, miss) = await FindArchivedAsync(uid, located, cancellationToken).ConfigureAwait(false);
        if (end < ScyllaArchiveRows.BogusEndBefore)
            return (AuditClass.BogusEnd, new[] { miss == null ? BogusInR2 : BogusNotInR2 });
        if (miss != null)
            return (AuditClass.Missing, new[] { miss });
        var archived = found.Block.ToAuction(found.Index);
        var extras = found.Block.ExtrasAt(found.Index);
        return ClassifyAgainst(row, archived, extras, Fingerprint.ForAudit(found.Block, found.Index));
    }

    /// <summary>The R2 row of a uid (located by <c>auction_locations</c>, proven by a part that is in the tag index), or the reason there is none.</summary>
    private async Task<((ArchivedAuctionBlock Block, int Index) Row, string Miss)> FindArchivedAsync(long uid, IReadOnlyDictionary<long, AuctionLocation> located, CancellationToken cancellationToken)
    {
        if (!located.TryGetValue(uid, out var location))
            return (default, "no_location");
        var tag = await locations.GetTagNameAsync(location.TagId, cancellationToken).ConfigureAwait(false);
        if (tag == null)
            return (default, "location_tag_unknown");
        var cached = await GetBlockAsync(tag, ArchiveMonths.FromIndex(location.Month), location.Part, cancellationToken).ConfigureAwait(false);
        if (cached == null)
            return (default, "block_missing");
        if (cached.Problem != null)
            return (default, cached.Problem);
        if (!cached.RowOf.TryGetValue(uid, out var index))
            return (default, "block_row_missing");
        return ((cached.Block, index), null);
    }

    /// <summary>Class of a Scylla row against its R2 row (shared by the partition scan and the sample read-back).</summary>
    internal (string Class, IReadOnlyList<string> Columns) ClassifyAgainst(ScyllaAuction row, SaveAuction archived, ScyllaAuctionExtras extras, Fingerprint archivedFingerprint)
    {
        var r2Better = ScyllaArchiveRows.IsBetter(archivedFingerprint, Fingerprint.ForAudit(row));
        var comparison = ScyllaAuditEquivalence.Compare(row, archived, extras, r2Better);
        if (comparison.Differing.Count > 0)
            return (AuditClass.NeedsMerge, comparison.Differing);
        return (r2Better ? AuditClass.R2BetterSuperset : AuditClass.Equal, comparison.R2Only);
    }

    /// <summary>Order independent hash of the rows: (uid, end ticks, highest bid, largest write time, issold), 5 x 8 bytes big endian each.</summary>
    internal static string HashTuples(List<(long Uid, long EndTicks, long HighestBid, long WriteTime, long Sold)> tuples)
    {
        tuples.Sort();
        using var sha = SHA256.Create();
        var buffer = new byte[40];
        foreach (var (uid, ticks, bid, writeTime, sold) in tuples)
        {
            System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(buffer.AsSpan(0), uid);
            System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(buffer.AsSpan(8), ticks);
            System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(buffer.AsSpan(16), bid);
            System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(buffer.AsSpan(24), writeTime);
            System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(buffer.AsSpan(32), sold);
            sha.TransformBlock(buffer, 0, buffer.Length, null, 0);
        }
        sha.TransformFinalBlock(Array.Empty<byte>(), 0, 0);
        return Convert.ToHexString(sha.Hash).ToLowerInvariant();
    }

    private async Task<IReadOnlyDictionary<long, AuctionLocation>> LocateAsync(List<long> uids, CancellationToken cancellationToken)
    {
        var result = new Dictionary<long, AuctionLocation>();
        for (var offset = 0; offset < uids.Count; offset += LocationChunkSize)
        {
            var chunk = uids.GetRange(offset, Math.Min(LocationChunkSize, uids.Count - offset));
            foreach (var (uid, location) in await locations.GetLocationsAsync(chunk, cancellationToken).ConfigureAwait(false))
                result[uid] = location;
        }
        return result;
    }

    /// <summary>The part read straight from storage (never the store's cache) and checked against the current tag index.</summary>
    private async Task<CachedBlock> GetBlockAsync(string tag, int month, int part, CancellationToken cancellationToken)
    {
        var key = (tag, month, part);
        if (blocks.TryGetValue(key, out var cached))
            return cached;
        var block = await store.ReadBlockAsync(tag, month, part, true, cancellationToken).ConfigureAwait(false);
        if (block == null)
            return null;
        var rowOf = new Dictionary<long, int>(block.RowCount);
        for (var i = 0; i < block.RowCount; i++)
            rowOf.TryAdd(block.AuctionUid[i], i);
        cached = new CachedBlock { Block = block, RowOf = rowOf, Problem = await IndexProblemAsync(tag, month, part, block, cancellationToken).ConfigureAwait(false) };
        blocks[key] = cached;
        blockOrder.Enqueue(key);
        while (blockOrder.Count > BlockCacheSize)
            blocks.Remove(blockOrder.Dequeue());
        return cached;
    }

    /// <summary>Null when the tag index lists this part with exactly this block's <see cref="ArchivedAuctionBlock.Sha256"/>; an orphan part (or a stale copy) is no proof.</summary>
    private async Task<string> IndexProblemAsync(string tag, int month, int part, ArchivedAuctionBlock block, CancellationToken cancellationToken)
    {
        if (!indexes.TryGetValue(tag, out var index))
            indexes[tag] = index = await store.ReadIndexAsync(tag, cancellationToken).ConfigureAwait(false);
        var entry = index?.Months?.FirstOrDefault(m => m.Month == month);
        if (entry?.PartSha256 == null || part >= entry.PartSha256.Count)
            return BlockNotInIndex;
        return string.Equals(entry.PartSha256[part], block.Sha256(), StringComparison.OrdinalIgnoreCase) ? null : BlockHashMismatch;
    }

    // ---------------------------------------------------------------- sample read-back through the public reader path

    private async Task<ScyllaAuditReadBack> ReadBackAsync(ScyllaAuditMonth state, ScyllaArchiveAuditOptions options, CancellationToken cancellationToken)
    {
        var result = new ScyllaAuditReadBack();
        var candidates = state.SampleCandidates.ToList();
        // partial Fisher-Yates: the first SampleReadBack entries are a uniform random sample
        var take = Math.Min(options.SampleReadBack, candidates.Count);
        for (var i = 0; i < take; i++)
        {
            var j = i + NextInt(candidates.Count - i);
            (candidates[i], candidates[j]) = (candidates[j], candidates[i]);
        }
        for (var i = 0; i < take; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var uid = candidates[i];
            var versions = await FetchRowsByUidAsync(uid, options.Consistency, cancellationToken).ConfigureAwait(false);
            if (versions.Count == 0)
            {
                result.ScyllaGone++;
                continue;
            }
            result.Sampled++;
            var uuid = versions[0].Row.Uuid.ToString("N");
            var archived = await reader.FindByUuidAsync(uuid, cancellationToken).ConfigureAwait(false);
            if (archived == null)
            {
                result.Mismatched++;
                readBackMismatches.Inc();
                result.Samples.Add(new ScyllaAuditSample { Uid = uid, Uuid = uuid, Tag = versions[0].Row.Tag, TimeKey = versions[0].Row.TimeKey, End = ScyllaArchiveRows.EndOf(versions[0].Row), Columns = new List<string> { "not_found_by_reader" } });
                continue;
            }
            var row = archived.Row;
            var fingerprint = Fingerprint.ForAudit(row);
            var columns = new List<string>();
            foreach (var version in versions)
            {
                var (cls, differing) = ClassifyAgainst(version.Row, row, archived.Extras, fingerprint);
                if (cls == AuditClass.NeedsMerge)
                    columns.AddRange(differing);
            }
            if (columns.Count == 0)
            {
                result.Matched++;
                continue;
            }
            result.Mismatched++;
            readBackMismatches.Inc();
            result.Samples.Add(new ScyllaAuditSample { Uid = uid, Uuid = uuid, Tag = versions[0].Row.Tag, TimeKey = versions[0].Row.TimeKey, End = ScyllaArchiveRows.EndOf(versions[0].Row), Columns = columns.Distinct().ToList() });
        }
        return result;
    }

    // ---------------------------------------------------------------- Scylla access (read only)

    /// <summary>
    /// Pages of one partition at the audit consistency. Same retry rule as the sweep: each page is fetched with up to <c>PAGE_RETRIES</c>
    /// retries (2 s, 4 s, 8 s, ...) from the same paging state, so a timeout mid-partition never restarts or skips rows.
    /// </summary>
    protected virtual async IAsyncEnumerable<IReadOnlyList<ScyllaAuditRow>> ReadPagesAsync(string tag, short timeKey, ScyllaArchiveAuditOptions options, ScyllaArchiveOptions sweepOptions, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        byte[] pagingState = null;
        while (true)
        {
            var page = await FetchPageWithRetriesAsync(tag, timeKey, pagingState, options, sweepOptions, cancellationToken).ConfigureAwait(false);
            yield return page.Rows;
            if (page.Next == null || page.Next.Length == 0)
                yield break;
            pagingState = page.Next;
        }
    }

    private async Task<(IReadOnlyList<ScyllaAuditRow> Rows, byte[] Next)> FetchPageWithRetriesAsync(string tag, short timeKey, byte[] pagingState, ScyllaArchiveAuditOptions options, ScyllaArchiveOptions sweepOptions, CancellationToken cancellationToken)
    {
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                return await FetchPageAsync(tag, timeKey, pagingState, sweepOptions.PageSize, options.Consistency, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
            {
                if (attempt >= sweepOptions.PageRetries)
                    throw new PartitionUnreadableException($"Audit page of {tag} timekey {timeKey} failed {attempt + 1} times", ex);
                var backoff = TimeSpan.FromSeconds(2 << attempt);
                logger.LogWarning(ex, "Scylla audit page of {Tag} timekey {TimeKey} failed (attempt {Attempt}); retrying from the same paging state in {Backoff}", tag, timeKey, attempt + 1, backoff);
                await Delay(backoff, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>One page of <c>WHERE tag = ? AND timekey = ?</c> (SELECT only), with the <c>WRITETIME</c> of <c>highestbidamount</c>.</summary>
    protected virtual async Task<(IReadOnlyList<ScyllaAuditRow> Rows, byte[] Next)> FetchPageAsync(string tag, short timeKey, byte[] pagingState, int pageSize, ConsistencyLevel consistency, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        partitionStatement ??= await scylla.Session.PrepareAsync(schema.PartitionCql).ConfigureAwait(false);
        var bound = partitionStatement.Bind(tag, timeKey);
        bound.SetPageSize(pageSize);
        bound.SetConsistencyLevel(consistency);
        bound.SetAutoPage(false);
        if (pagingState != null)
            bound.SetPagingState(pagingState);
        var rows = await scylla.Session.ExecuteAsync(bound).ConfigureAwait(false);
        var result = new List<ScyllaAuditRow>();
        foreach (var row in rows) // auto paging is off: only this page
            result.Add(MapRow(row, schema.WriteTimeSelected.Count));
        return (result, rows.PagingState);
    }

    /// <summary>Every row (versions) of an auction uid through the <c>auctionuid</c> secondary index, at the audit consistency.</summary>
    protected virtual async Task<IReadOnlyList<ScyllaAuditRow>> FetchRowsByUidAsync(long uid, ConsistencyLevel consistency, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        uidStatement ??= await scylla.Session.PrepareAsync(schema.UidCql).ConfigureAwait(false);
        var bound = uidStatement.Bind(uid);
        bound.SetConsistencyLevel(consistency);
        var rows = await scylla.Session.ExecuteAsync(bound).ConfigureAwait(false);
        return rows.Select(row => MapRow(row, schema.WriteTimeSelected.Count)).ToList();
    }

    private static ScyllaAuditRow MapRow(global::Cassandra.Row row, int writeTimeColumns)
    {
        T V<T>(string column) => row.IsNull(column) ? default : row.GetValue<T>(column);
        DateTime Date(string column) => row.IsNull(column) ? default : DateTime.SpecifyKind(row.GetValue<DateTimeOffset>(column).UtcDateTime, DateTimeKind.Utc);
        var scylla = new ScyllaAuction
        {
            Tag = V<string>("tag"),
            TimeKey = V<short>("timekey"),
            IsSold = V<bool>("issold"),
            End = Date("end"),
            AuctionUid = V<long>("auctionuid"),
            Uuid = V<Guid>("uuid"),
            ItemName = V<string>("itemname"),
            ItemLore = V<string>("itemlore"),
            Extra = V<string>("extra"),
            Category = V<string>("category"),
            Tier = V<string>("tier"),
            StartingBid = V<long>("startingbid"),
            HighestBidAmount = V<long>("highestbidamount"),
            Bin = V<bool>("bin"),
            Start = Date("start"),
            ItemCreatedAt = Date("itemcreatedat"),
            Auctioneer = V<Guid>("auctioneer"),
            ProfileId = V<Guid>("profileid"),
            ProfileName = V<string>("profilename"),
            Coop = V<IEnumerable<string>>("coop")?.ToList(),
            CoopName = V<string>("coopname"),
            HighestBidder = V<Guid>("highestbidder"),
            HighestBidderName = V<string>("highestbiddername"),
            ExtraAttributesJson = V<string>("extraattributesjson"),
            NbtLookup = V<IDictionary<string, string>>("nbtlookup")?.ToDictionary(kv => kv.Key, kv => kv.Value),
            ItemBytes = V<byte[]>("itembytes"),
            ItemUid = V<long>("itemuid"),
            ItemId = V<Guid>("itemid"),
            Enchantments = V<IDictionary<string, int>>("enchantments")?.ToDictionary(kv => kv.Key, kv => kv.Value),
            SerialisedBids = V<byte[]>("serialisedbids"),
            Count = V<int>("count"),
            Color = row.IsNull("color") ? null : row.GetValue<int>("color"),
        };
        return new ScyllaAuditRow(scylla, MaxWriteTime(row, writeTimeColumns));
    }

    /// <summary>Largest <c>WRITETIME</c> of the selected columns (<c>wt0..</c>), null columns ignored, null when all are null.</summary>
    private static long? MaxWriteTime(global::Cassandra.Row row, int writeTimeColumns)
    {
        long? max = null;
        for (var i = 0; i < writeTimeColumns; i++)
        {
            if (!row.IsNull("wt" + i))
                max = Math.Max(max ?? long.MinValue, row.GetValue<long>("wt" + i));
        }
        return max;
    }

    /// <summary>The columns of the audited table from <c>system_schema.columns</c> (overridden in tests).</summary>
    protected virtual async Task<IReadOnlyList<ScyllaColumnInfo>> ReadSchemaAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var rows = await scylla.Session.ExecuteAsync(new SimpleStatement(ScyllaAuditSchema.ColumnsCql)).ConfigureAwait(false);
        return rows.Select(r => new ScyllaColumnInfo(r.GetValue<string>("column_name"), r.GetValue<string>("kind"), r.GetValue<string>("type"))).ToList();
    }

    /// <summary>Has SkyIndexer imported the month (sentinel <c>scylla_at</c>)? Without it the archive does not hold the Scylla-only rows yet. Read only.</summary>
    protected virtual async Task<bool> IsMonthImportedAsync(int month, CancellationToken cancellationToken)
    {
        var index = ArchiveMonths.ToIndex(month);
        using var db = new ArchiveContext();
        return await db.ArchivedMonths.AsNoTracking().AnyAsync(m => m.TagId == 0 && m.Month == index && m.ScyllaAt != null, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Random number in <c>[0, max)</c> (overridden in tests).</summary>
    protected virtual int NextInt(int max) => Random.Shared.Next(max);

    /// <summary>Wait between steps (overridden in tests).</summary>
    protected virtual Task Delay(TimeSpan delay, CancellationToken cancellationToken)
        => delay <= TimeSpan.Zero ? Task.CompletedTask : Task.Delay(delay, cancellationToken);
}
