using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Data.Linq;
using Coflnet.Sky.Core;
using Coflnet.Sky.Core.Archive;
using MessagePack;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Coflnet.Sky.Auctions.Archive
{
    /// <summary>Cached list of the Scylla partitions that can hold rows of the swept months (R2 meta object).</summary>
    [MessagePackObject]
    public class ScyllaPartitionList
    {
        /// <summary>UTC time of the <c>SELECT DISTINCT</c> scan.</summary>
        [Key(0)] public DateTime CreatedAt { get; set; }
        /// <summary>Partitions in scope, sorted by tag then timekey.</summary>
        [Key(1)] public List<(string Tag, short TimeKey)> Partitions { get; set; } = new();
        /// <summary><c>ARCHIVE:OLDEST_MONTH</c> the list was filtered with.</summary>
        [Key(2)] public int OldestMonth { get; set; }
        /// <summary><c>ARCHIVE:NEWEST_MONTH</c> the list was filtered with.</summary>
        [Key(3)] public int NewestMonth { get; set; }
    }

    /// <summary>MariaDB state of a month: does the phase-1 sentinel exist, and when did SkyIndexer import the sweep's manifest (<c>scylla_at</c>).</summary>
    public record ScyllaMonthState(bool HasSentinel, DateTime? ScyllaAt);

    /// <summary>Result of one tag-month.</summary>
    public enum TagMonthResult
    {
        /// <summary>Scylla-only rows were archived, the tag index and the manifest updated and the marker set.</summary>
        Archived,
        /// <summary>Phase 1 already had every Scylla row; only the marker was set.</summary>
        NothingNew,
        /// <summary>No phase-1 row and no Scylla row in the month; only the marker was set.</summary>
        Empty,
        /// <summary>No marker; retried next pass.</summary>
        Failed,
    }

    /// <summary>What happened to one tag-month (for logs and tests).</summary>
    public class TagMonthOutcome
    {
        /// <summary>Item tag.</summary>
        public string Tag { get; init; }
        /// <summary>Month, <c>yyyyMM</c>.</summary>
        public int Month { get; init; }
        /// <summary>Result.</summary>
        public TagMonthResult Result { get; set; }
        /// <summary>Why it failed.</summary>
        public string Error { get; set; }
        /// <summary>Parts phase 1 archived.</summary>
        public int PhaseOneParts { get; set; }
        /// <summary>Rows phase 1 archived.</summary>
        public int PhaseOneRows { get; set; }
        /// <summary>Rows read from Scylla (all partitions of the tag that overlap the month).</summary>
        public int ScyllaRowsRead { get; set; }
        /// <summary>Rows that ended in the month.</summary>
        public int ScyllaRowsInMonth { get; set; }
        /// <summary>Rows ending before 2019-06.</summary>
        public int BogusEndRows { get; set; }
        /// <summary>Rows ending in another month.</summary>
        public int OutsideMonthRows { get; set; }
        /// <summary>Distinct auctions (after dedupe) that phase 1 already had.</summary>
        public int OverlapRows { get; set; }
        /// <summary>Overlapping auctions whose cross-check fields differ.</summary>
        public int Mismatches { get; set; }
        /// <summary>Distinct auctions (after dedupe) that were only in Scylla and are written.</summary>
        public int ScyllaOnlyRows { get; set; }
        /// <summary>Scylla-only copies skipped because the auction is located in another tag-month.</summary>
        public int SupersededRows { get; set; }
        /// <summary>New parts written.</summary>
        public int PartsWritten { get; set; }
    }

    /// <summary>
    /// Phase 2 of the auction archive: adds the Scylla rows (<c>weekly_auctions_2</c>) of the months
    /// [<c>ARCHIVE:OLDEST_MONTH</c>, <c>ARCHIVE:NEWEST_MONTH</c>] that phase 1 (SkyIndexer, MariaDB) does not have.
    /// SkyAuctions only reads MariaDB (its user is read-only): the phase-1 sentinel and tag rows, and <c>auction_locations</c>.
    /// Per tag-month: phase-1 parts are read (never rewritten), the Scylla partitions overlapping the month are paged (LocalOne,
    /// per-page retry resuming from the paging state), rows are deduped per auction, rows phase 1 has are only cross-checked,
    /// copies of auctions located in another tag-month are skipped, Scylla-only rows are written as parts N, N+1, ... and verified by
    /// uncached read-back. Then, in this order: the R2 tag index (conditional), the month's <see cref="ScyllaMonthManifest"/>
    /// (conditional) and the Redis marker. SkyIndexer's <c>ScyllaArchiveImportWorker</c> indexes the manifest in MariaDB and sets
    /// <c>scylla_at</c>; month M+1 is only started once month M's sentinel has it, so the superseded check sees every earlier location.
    /// Nothing is ever deleted.
    /// </summary>
    public class ScyllaArchiveSweep : BackgroundService
    {
        /// <summary>Meta key of the cached partition list (<c>auctions/v2/_meta/scylla_partitions.msgpack.br</c>).</summary>
        public const string PartitionListKey = "scylla_partitions.msgpack.br";
        /// <summary>Uids per <c>auction_locations</c> lookup; MariaDB stops index dives at 200 IN values (phase 1 outage), stay well below.</summary>
        internal const int LocationLookupChunkSize = 100;
        /// <summary>Partition keys per page of the token-range scan.</summary>
        internal const int PartitionScanPageSize = 1000;
        /// <summary>A token range that keeps timing out is halved at most this often (to 1/256 of its width) before the pass fails.</summary>
        internal const int MaxRangeSplitDepth = 8;
        /// <summary>Progress is logged every this many ranges.</summary>
        internal const int PartitionScanLogEvery = 256;
        private const string FirstRangeCql = "SELECT DISTINCT tag, timekey FROM weekly_auctions_2 WHERE token(tag, timekey) >= ? AND token(tag, timekey) <= ? BYPASS CACHE";
        private const string RangeCql = "SELECT DISTINCT tag, timekey FROM weekly_auctions_2 WHERE token(tag, timekey) > ? AND token(tag, timekey) <= ? BYPASS CACHE";
        /// <summary>Wait before retrying when MariaDB or Redis is not reachable.</summary>
        internal static readonly TimeSpan DatabaseRetryDelay = TimeSpan.FromMinutes(10);
        /// <summary>Wait after a pass failed with an exception.</summary>
        internal static readonly TimeSpan PassFailureDelay = TimeSpan.FromMinutes(5);

        private readonly IAuctionBlobStore store;
        private readonly IArchiveMetaStore meta;
        private readonly IScyllaArchiveMarkers markers;
        private readonly ScyllaService scylla;
        private readonly IConfiguration config;
        private readonly ILogger<ScyllaArchiveSweep> logger;
        private readonly Dictionary<string, ushort> tagIds = new(StringComparer.Ordinal);
        private readonly ConcurrentDictionary<string, bool> warnedValues = new();
        private bool completeLogged;
        private int? waitingLoggedFor;
        private bool lastPassWaitingForImport;
        private int? redoResetLoggedFor;
        private int? redoWaitLoggedFor;
        private PreparedStatement firstRangeStatement;
        private PreparedStatement rangeStatement;
        private ItemNameTagResolver nameResolver;
        /// <summary>Scylla tag whose rows are remapped to a real tag by item name.</summary>
        internal const string UnknownTag = "unknown";

        private static readonly Prometheus.Counter tagMonthsTotal = Prometheus.Metrics.CreateCounter("sky_auctions_archive_tag_months_total", "Tag-months finished by the Scylla sweep (archived, nothing new or empty)");
        private static readonly Prometheus.Counter tagMonthsEmpty = Prometheus.Metrics.CreateCounter("sky_auctions_archive_tag_months_empty", "Tag-months without phase-1 row and without Scylla rows");
        private static readonly Prometheus.Counter partsWritten = Prometheus.Metrics.CreateCounter("sky_auctions_archive_parts_written", "Scylla parts written");
        private static readonly Prometheus.Counter partsVerified = Prometheus.Metrics.CreateCounter("sky_auctions_archive_parts_verified", "Scylla parts verified by uncached read-back");
        private static readonly Prometheus.Counter verifyFailures = Prometheus.Metrics.CreateCounter("sky_auctions_archive_verify_failures", "Scylla parts whose read-back did not match");
        private static readonly Prometheus.Counter rowsArchived = Prometheus.Metrics.CreateCounter("sky_auctions_archive_rows_archived", "Scylla-only rows in verified parts");
        private static readonly Prometheus.Counter rowsAlreadyArchived = Prometheus.Metrics.CreateCounter("sky_auctions_archive_rows_already_archived", "Scylla auctions phase 1 already archived (cross-checked, never rewritten)");
        private static readonly Prometheus.Counter rowsSuperseded = Prometheus.Metrics.CreateCounter("sky_auctions_archive_rows_superseded", "Scylla-only copies skipped because the auction is already archived in another tag-month");
        private static readonly Prometheus.Counter rowsSkippedBogusEnd = Prometheus.Metrics.CreateCounter("sky_auctions_archive_rows_skipped_bogus_end", "Scylla rows ending before 2019-06 (random-timekey garbage)");
        private static readonly Prometheus.Counter crosscheckRows = Prometheus.Metrics.CreateCounter("sky_auctions_archive_crosscheck_rows", "Overlapping rows compared with phase 1");
        private static readonly Prometheus.Counter crosscheckMismatches = Prometheus.Metrics.CreateCounter("sky_auctions_archive_crosscheck_mismatches", "Overlapping rows whose field differs from phase 1", "field");
        private static readonly Prometheus.Counter rowsRetagged = Prometheus.Metrics.CreateCounter("sky_auctions_archive_retagged_rows", "Rows of the Scylla tag unknown moved to the tag their item name resolves to");
        private static readonly Prometheus.Counter rowsUnresolved = Prometheus.Metrics.CreateCounter("sky_auctions_archive_unresolved_rows", "Rows of the Scylla tag unknown whose item name does not resolve to exactly one tag (kept under unknown)");
        private static readonly Prometheus.Counter pages = Prometheus.Metrics.CreateCounter("sky_auctions_archive_pages", "Scylla pages read");
        private static readonly Prometheus.Counter partitionsRead = Prometheus.Metrics.CreateCounter("sky_auctions_archive_partitions_read", "Scylla partitions read completely");
        private static readonly Prometheus.Counter pageRetries = Prometheus.Metrics.CreateCounter("sky_auctions_archive_page_retries", "Scylla page retries");
        private static readonly Prometheus.Counter errors = Prometheus.Metrics.CreateCounter("sky_auctions_archive_errors", "Scylla sweep errors (failed tag-months, failed passes, MariaDB/Redis unavailable)");
        private static readonly Prometheus.Gauge currentMonth = Prometheus.Metrics.CreateGauge("sky_auctions_archive_current_month", "Month (yyyyMM) being swept");
        private static readonly Prometheus.Counter partitionScanRetries = Prometheus.Metrics.CreateCounter("sky_auctions_archive_partition_scan_retries", "Partition scan pages retried after a Scylla timeout");
        private static readonly Prometheus.Counter partitionScanSplits = Prometheus.Metrics.CreateCounter("sky_auctions_archive_partition_scan_splits", "Token ranges halved because they kept timing out");
        private static readonly Prometheus.Gauge partitionScanRangesDone = Prometheus.Metrics.CreateGauge("sky_auctions_archive_partition_scan_ranges_done", "Token ranges of the current partition scan done");
        private static readonly Prometheus.Gauge partitionScanFound = Prometheus.Metrics.CreateGauge("sky_auctions_archive_partition_scan_found", "In-scope partitions found by the current partition scan");
        private static readonly Prometheus.Gauge partitionsKnown = Prometheus.Metrics.CreateGauge("sky_auctions_archive_partitions_known", "Scylla partitions in the swept months");
        private static readonly Prometheus.Gauge lastMonthCompleted = Prometheus.Metrics.CreateGauge("sky_auctions_archive_last_month_completed_timestamp", "Unix time the sweep last finished a month (manifest completed, month marker set)");
        private static readonly Prometheus.Gauge redoWaiting = Prometheus.Metrics.CreateGauge("sky_auctions_archive_redo_waiting_month", "Month (yyyyMM) whose redo waits for the operator to reset its MariaDB rows, 0 when not waiting");
        private static readonly Prometheus.Gauge waitingForImport = Prometheus.Metrics.CreateGauge("sky_auctions_archive_waiting_for_import_month", "Month (yyyyMM) the sweep waits for SkyIndexer to import, 0 when not waiting");

        /// <summary>DI constructor.</summary>
        public ScyllaArchiveSweep(IAuctionBlobStore store, IArchiveMetaStore meta, IScyllaArchiveMarkers markers, ScyllaService scylla, IConfiguration config, ILogger<ScyllaArchiveSweep> logger)
        {
            this.store = store;
            this.meta = meta;
            this.markers = markers;
            this.scylla = scylla;
            this.config = config;
            this.logger = logger;
        }

        /// <inheritdoc/>
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var options = ScyllaArchiveOptions.From(config);
            if (!options.Enabled)
                return;
            if (!store.IsEnabled || meta == null)
            {
                logger.LogWarning("ARCHIVE:ENABLED=true but the S3 archive store is not configured; Scylla archive sweep will not run.");
                return;
            }
            try
            {
                await Delay(TimeSpan.FromSeconds(options.StartDelaySeconds), stoppingToken).ConfigureAwait(false);
                await WaitForPrerequisitesAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
            logger.LogInformation("Scylla archive sweep started for {Oldest}..{Newest}, page size {PageSize}, max {Rows} rows per part",
                ArchiveMonths.Format(options.OldestMonth), ArchiveMonths.Format(options.NewestMonth), options.PageSize, options.MaxRowsPerPart);

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var completed = await RunOnceAsync(DateTime.UtcNow, stoppingToken).ConfigureAwait(false);
                    var pause = PauseAfterPass(ScyllaArchiveOptions.From(config));
                    logger.LogInformation("Scylla archive pass done ({Completed} months completed); next pass in {Pause} min", completed, pause.TotalMinutes);
                    await Delay(pause, stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    errors.Inc();
                    logger.LogError(ex, "Scylla archive pass failed, retrying in {Minutes} min", PassFailureDelay.TotalMinutes);
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

        /// <summary>
        /// One cheap read of MariaDB (the sentinel query, which also fails until SkyIndexer has added <c>archived_months.scylla_at</c>)
        /// and one of Redis. Until both answer, log one error naming the config key and retry every <see cref="DatabaseRetryDelay"/>.
        /// </summary>
        internal async Task WaitForPrerequisitesAsync(CancellationToken cancellationToken)
        {
            while (true)
            {
                try
                {
                    await VerifyDatabaseAsync(cancellationToken).ConfigureAwait(false);
                    try
                    {
                        await markers.GetAsync(ScyllaArchiveMarkerKeys.SweepComplete).ConfigureAwait(false);
                        return;
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
                    {
                        errors.Inc();
                        logger.LogError(ex, "Auction archive: Redis (ARCHIVE:REDIS_HOST / SETTINGS_REDIS_HOST) unavailable, retrying in 10 min");
                    }
                }
                catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
                {
                    errors.Inc();
                    logger.LogError(ex, "Auction archive: MariaDB (DBConnection) unavailable, retrying in 10 min");
                }
                await Delay(DatabaseRetryDelay, cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// One pass, oldest month first. A month is skipped once SkyIndexer imported it (sentinel <c>scylla_at</c>). The first month
        /// that is not imported is the only one worked on: without its phase-1 sentinel, or when the sweep already finished it
        /// (month marker) and waits for the import, the pass ends; otherwise its tags are swept and, when all are done, the manifest
        /// is completed and the month marker set, and the pass ends as well (the next month waits for the import).
        /// Returns the number of months completed in this pass.
        /// </summary>
        internal async Task<int> RunOnceAsync(DateTime nowUtc, CancellationToken cancellationToken)
        {
            var options = ScyllaArchiveOptions.From(config);
            lastPassWaitingForImport = false;
            nameResolver = null; // reloaded (lazily) once per pass
            await DropOutOfRangeRedoKeysAsync(options).ConfigureAwait(false);
            if (await markers.GetAsync(ScyllaArchiveMarkerKeys.SweepComplete).ConfigureAwait(false) != null)
            {
                if (!await AnyRedoRequestedAsync(options).ConfigureAwait(false))
                {
                    LogCompleteOnce(options);
                    return 0;
                }
                // a redo of a month of a finished sweep: walk the months again (imported ones are skipped)
                await markers.DeleteAsync(ScyllaArchiveMarkerKeys.SweepComplete).ConfigureAwait(false);
                completeLogged = false;
            }
            redoWaiting.Set(0);
            for (var month = options.OldestMonth; month <= options.NewestMonth; month = ArchiveMonths.Next(month))
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (await markers.GetAsync(ScyllaArchiveMarkerKeys.Redo(month)).ConfigureAwait(false) != null
                    && !await PrepareRedoAsync(month, nowUtc, options, cancellationToken).ConfigureAwait(false))
                {
                    lastPassWaitingForImport = true;
                    return 0;
                }
                var state = await GetMonthStateAsync(month, cancellationToken).ConfigureAwait(false);
                if (state.ScyllaAt != null)
                    continue;
                var monthText = ArchiveMonths.Format(month);
                if (await markers.GetAsync(ScyllaArchiveMarkerKeys.Month(month)).ConfigureAwait(false) != null)
                {
                    // pipeline gate: the next month's superseded check needs this month's locations, which SkyIndexer writes
                    LogWaitingOnce(month);
                    return 0;
                }
                waitingForImport.Set(0);
                if (!state.HasSentinel)
                {
                    logger.LogInformation("Scylla archive: {Month} has no phase-1 sentinel yet (SkyIndexer has not archived it); waiting", monthText);
                    return 0;
                }
                if (!await SweepMonthAsync(month, nowUtc, options, cancellationToken).ConfigureAwait(false))
                    return 0;
                if (month == options.NewestMonth)
                {
                    await markers.SetAsync(ScyllaArchiveMarkerKeys.SweepComplete, ScyllaArchiveMarkerKeys.Iso(DateTime.UtcNow)).ConfigureAwait(false);
                    LogCompleteOnce(options);
                }
                else
                {
                    LogWaitingOnce(month);
                }
                return 1;
            }
            // every month in scope is imported (also covers lost Redis markers)
            await markers.SetAsync(ScyllaArchiveMarkerKeys.SweepComplete, ScyllaArchiveMarkerKeys.Iso(DateTime.UtcNow)).ConfigureAwait(false);
            LogCompleteOnce(options);
            return 0;
        }

        private async Task<bool> AnyRedoRequestedAsync(ScyllaArchiveOptions options)
        {
            for (var month = options.OldestMonth; month <= options.NewestMonth; month = ArchiveMonths.Next(month))
                if (await markers.GetAsync(ScyllaArchiveMarkerKeys.Redo(month)).ConfigureAwait(false) != null)
                    return true;
            return false;
        }

        /// <summary>Months checked on each side of the configured range for a stray redo key.</summary>
        private const int RedoStrayWindowMonths = 24;

        /// <summary>A redo key for a month outside OLDEST..NEWEST can never be served: warn and delete it (checks a window around the range).</summary>
        private async Task DropOutOfRangeRedoKeysAsync(ScyllaArchiveOptions options)
        {
            var before = options.OldestMonth;
            for (var i = 0; i < RedoStrayWindowMonths; i++)
                before = before % 100 == 1 ? (before / 100 - 1) * 100 + 12 : before - 1;
            var end = options.NewestMonth;
            for (var i = 0; i < RedoStrayWindowMonths; i++)
                end = ArchiveMonths.Next(end);
            for (var month = before; month <= end; month = ArchiveMonths.Next(month))
            {
                if (month == options.OldestMonth)
                {
                    month = options.NewestMonth;
                    continue;
                }
                if (await markers.GetAsync(ScyllaArchiveMarkerKeys.Redo(month)).ConfigureAwait(false) == null)
                    continue;
                logger.LogWarning("Scylla archive: redo of {Month} requested but it is outside {Oldest}..{Newest}; ignoring and deleting the key",
                    ArchiveMonths.Format(month), ArchiveMonths.Format(options.OldestMonth), ArchiveMonths.Format(options.NewestMonth));
                await markers.DeleteAsync(ScyllaArchiveMarkerKeys.Redo(month)).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Operator-triggered redo of a month. First resets the R2 side (tag-month and month markers, manifest tags and completion) so
        /// SkyIndexer cannot import the stale manifest, then waits until the operator reset the month's MariaDB rows. True when the
        /// month may be swept again (redo key deleted); false while the MariaDB reset is pending (the pass ends).
        /// </summary>
        private async Task<bool> PrepareRedoAsync(int month, DateTime nowUtc, ScyllaArchiveOptions options, CancellationToken cancellationToken)
        {
            var monthText = ArchiveMonths.Format(month);
            var (manifest, _) = await meta.ReadMetaVersionedAsync<ScyllaMonthManifest>(AuctionBlobKeys.ScyllaManifest(month), cancellationToken).ConfigureAwait(false);
            var tags = new HashSet<string>(manifest?.Tags.Select(t => t.Tag) ?? Enumerable.Empty<string>(), StringComparer.Ordinal);
            // tag-months without manifest entry (empty / overlap-only) carry markers too, and a retagged row needs its tag unmarked
            var partitions = await GetPartitionsAsync(nowUtc, options, cancellationToken).ConfigureAwait(false);
            var byTag = partitions.GroupBy(p => p.Tag, StringComparer.Ordinal)
                .ToDictionary(g => g.Key, g => g.Select(p => p.TimeKey).Distinct().ToList(), StringComparer.Ordinal);
            foreach (var (tag, _) in TagsOfMonth(byTag, month))
                tags.Add(tag);
            var keys = tags.Select(t => ScyllaArchiveMarkerKeys.TagMonth(t, month)).Append(ScyllaArchiveMarkerKeys.Month(month)).ToArray();
            await markers.DeleteAsync(keys).ConfigureAwait(false);
            if (manifest != null && (manifest.Tags.Count > 0 || manifest.CompletedAt != null))
            {
                var removed = manifest.Tags.Count;
                var error = await UpdateManifestAsync(month, m => { m.Tags.Clear(); m.CompletedAt = null; }, cancellationToken).ConfigureAwait(false);
                if (error != null)
                    throw new InvalidOperationException($"Scylla archive: redo of {monthText} could not reset the manifest: {error}");
                if (redoResetLoggedFor != month)
                    logger.LogInformation("Scylla archive: redo of {Month} requested; manifest and markers reset ({Tags} tag entries removed)", monthText, removed);
            }
            redoResetLoggedFor = month;

            var (sentinelImported, tagRows, hasLocations) = await GetRedoStateAsync(month, cancellationToken).ConfigureAwait(false);
            if (sentinelImported || tagRows > 0 || hasLocations)
            {
                var idx = ArchiveMonths.ToIndex(month);
                redoWaiting.Set(month);
                if (redoWaitLoggedFor != month)
                {
                    redoWaitLoggedFor = month;
                    logger.LogWarning("Scylla archive: redo of {Month} waits for the MariaDB reset: run `DELETE FROM auction_locations WHERE month = {Idx}; DELETE FROM archived_months WHERE month = {Idx} AND tag_id <> 0; UPDATE archived_months SET scylla_at = NULL WHERE month = {Idx} AND tag_id = 0;` (currently {TagRows} tag rows, locations present: {HasLocations}, sentinel imported: {SentinelImported})",
                        monthText, idx, idx, idx, tagRows, hasLocations, sentinelImported);
                }
                return false;
            }
            await markers.DeleteAsync(ScyllaArchiveMarkerKeys.Redo(month)).ConfigureAwait(false);
            redoWaiting.Set(0);
            redoResetLoggedFor = null;
            redoWaitLoggedFor = null;
            waitingLoggedFor = null;
            logger.LogInformation("Scylla archive: redo of {Month} starts", monthText);
            return true;
        }

        /// <summary>
        /// Whether the operator reset the MariaDB side of a month for a redo: sentinel imported (<c>scylla_at</c>), number of tag rows
        /// (<c>tag_id &lt;&gt; 0</c>) and of <c>auction_locations</c> rows of the month. Read only.
        /// </summary>
        protected virtual async Task<(bool SentinelImported, int TagRows, bool HasLocations)> GetRedoStateAsync(int month, CancellationToken cancellationToken)
        {
            var index = ArchiveMonths.ToIndex(month);
            using var db = new ArchiveContext();
            var imported = await db.ArchivedMonths.AsNoTracking().AnyAsync(m => m.TagId == 0 && m.Month == index && m.ScyllaAt != null, cancellationToken).ConfigureAwait(false);
            var tagRows = await db.ArchivedMonths.AsNoTracking().CountAsync(m => m.TagId != 0 && m.Month == index, cancellationToken).ConfigureAwait(false);
            var locations = await db.AuctionLocations.AsNoTracking().AnyAsync(l => l.Month == index, cancellationToken).ConfigureAwait(false); // EXISTS, not COUNT: auction_locations has no month index
            return (imported, tagRows, locations);
        }

        /// <summary>
        /// Pause before the next pass: <c>IMPORT_WAIT_MINUTES</c> when the last pass ended at the pipeline gate (SkyIndexer polls every
        /// few minutes, so the next month can usually start soon), <c>IDLE_PAUSE_MINUTES</c> otherwise (sweep complete, waiting for
        /// phase 1, or failed tags).
        /// </summary>
        internal TimeSpan PauseAfterPass(ScyllaArchiveOptions options)
            => TimeSpan.FromMinutes(lastPassWaitingForImport ? options.ImportWaitMinutes : options.IdlePauseMinutes);

        private void LogWaitingOnce(int month)
        {
            lastPassWaitingForImport = true;
            waitingForImport.Set(month);
            if (waitingLoggedFor == month)
                return;
            waitingLoggedFor = month;
            logger.LogInformation("Scylla archive: waiting for SkyIndexer to import {Month}", ArchiveMonths.Format(month));
        }

        private void LogCompleteOnce(ScyllaArchiveOptions options)
        {
            waitingForImport.Set(0);
            if (completeLogged)
                return;
            completeLogged = true;
            logger.LogInformation("Scylla archive sweep complete through {Newest}", ArchiveMonths.Format(options.NewestMonth));
        }

        /// <summary>Sweep every tag of a month; true when all are done (manifest completed, month marker set).</summary>
        private async Task<bool> SweepMonthAsync(int month, DateTime nowUtc, ScyllaArchiveOptions options, CancellationToken cancellationToken)
        {
            var monthText = ArchiveMonths.Format(month);
            var partitions = await GetPartitionsAsync(nowUtc, options, cancellationToken).ConfigureAwait(false);
            var byTag = partitions.GroupBy(p => p.Tag, StringComparer.Ordinal)
                .ToDictionary(g => g.Key, g => g.Select(p => p.TimeKey).Distinct().OrderBy(k => k).ToList(), StringComparer.Ordinal);
            currentMonth.Set(month);
            var tags = TagsOfMonth(byTag, month);
            RetagSpill retag = null;
            try
            {
                var unknownKeys = tags.FirstOrDefault(t => t.Tag == UnknownTag).TimeKeys;
                if (unknownKeys != null && await markers.GetAsync(ScyllaArchiveMarkerKeys.TagMonth(UnknownTag, month)).ConfigureAwait(false) == null)
                {
                    try
                    {
                        retag = await PreScanUnknownAsync(month, unknownKeys, options, cancellationToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        throw;
                    }
                    catch (Exception ex)
                    {
                        errors.Inc();
                        logger.LogError(ex, "Scylla archive: pre-scan of the unknown rows of {Month} failed; retried next pass", monthText);
                        return false;
                    }
                    var known = tags.Select(t => t.Tag).ToHashSet(StringComparer.Ordinal);
                    foreach (var resolved in retag.Tags)
                        if (!known.Contains(resolved))
                            tags.Add((resolved, new List<short>()));
                    tags = tags.OrderBy(t => t.Tag, StringComparer.Ordinal).ToList();
                }
                return await SweepTagsAsync(month, tags, retag, options, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                retag?.Dispose();
            }
        }

        /// <summary>
        /// Page every <c>unknown</c> partition of the month once and spill the rows whose item name resolves to a tag (grouped by that tag).
        /// Rows outside the month or ending before 2019-06 are not spilled (the normal tag-month would skip them as well); rows whose
        /// resolved tag already has its marker are dropped, that tag-month received them in an earlier pass.
        /// </summary>
        private async Task<RetagSpill> PreScanUnknownAsync(int month, IReadOnlyList<short> timeKeys, ScyllaArchiveOptions options, CancellationToken cancellationToken)
        {
            var monthText = ArchiveMonths.Format(month);
            nameResolver ??= await LoadNameResolverAsync(cancellationToken).ConfigureAwait(false);
            var spill = new RetagSpill(month);
            try
            {
                var markedTags = new Dictionary<string, bool>(StringComparer.Ordinal);
                var unresolvedNames = new Dictionary<string, int>(StringComparer.Ordinal);
                var resolved = 0;
                var unresolved = 0;
                var resolvedTags = new HashSet<string>(StringComparer.Ordinal);
                foreach (var timeKey in timeKeys)
                {
                    await foreach (var page in ReadPartitionPagesAsync(UnknownTag, timeKey, options, cancellationToken).ConfigureAwait(false))
                    {
                        foreach (var scyllaRow in page)
                        {
                            var end = ScyllaArchiveRows.EndOf(scyllaRow);
                            if (end < ScyllaArchiveRows.BogusEndBefore || ArchiveMonths.MonthOf(end) != month)
                                continue;
                            if (!nameResolver.TryResolve(scyllaRow.ItemName, out var resolvedTag))
                            {
                                unresolved++;
                                var key = ItemNameTagResolver.Normalise(scyllaRow.ItemName) ?? "(empty)";
                                unresolvedNames[key] = unresolvedNames.GetValueOrDefault(key) + 1;
                                continue;
                            }
                            resolved++;
                            resolvedTags.Add(resolvedTag);
                            if (!markedTags.TryGetValue(resolvedTag, out var marked))
                                markedTags[resolvedTag] = marked = await markers.GetAsync(ScyllaArchiveMarkerKeys.TagMonth(resolvedTag, month)).ConfigureAwait(false) != null;
                            if (marked)
                                continue;
                            var (mapped, extras) = ScyllaArchiveRows.Map(scyllaRow, resolvedTag, WarnUnknownValue);
                            spill.Add(mapped, extras, Fingerprint.Of(scyllaRow));
                        }
                        await Delay(TimeSpan.FromMilliseconds(options.PagePauseMs), cancellationToken).ConfigureAwait(false);
                    }
                    partitionsRead.Inc();
                    await Delay(TimeSpan.FromMilliseconds(options.PartitionPauseMs), cancellationToken).ConfigureAwait(false);
                }
                rowsRetagged.Inc(resolved);
                rowsUnresolved.Inc(unresolved);
                var top = string.Join(", ", unresolvedNames.OrderByDescending(n => n.Value).ThenBy(n => n.Key, StringComparer.Ordinal).Take(15).Select(n => $"{n.Key} ({n.Value})"));
                logger.LogInformation("Scylla archive: {Month} unknown rows: {Resolved} retagged to {Tags} tags, {Unresolved} kept; top unresolved names: {Names}",
                    monthText, resolved, resolvedTags.Count, unresolved, top);
                return spill;
            }
            catch
            {
                spill.Dispose();
                throw;
            }
        }

        /// <summary>Loads the item-name resolver (Items and AltItemNames, read only); a seam for tests.</summary>
        protected virtual async Task<ItemNameTagResolver> LoadNameResolverAsync(CancellationToken cancellationToken)
        {
            using var db = new HypixelContext();
            return await ItemNameTagResolver.LoadAsync(db, cancellationToken).ConfigureAwait(false);
        }

        private async Task<bool> SweepTagsAsync(int month, List<(string Tag, List<short> TimeKeys)> tags, RetagSpill retag, ScyllaArchiveOptions options, CancellationToken cancellationToken)
        {
            var monthText = ArchiveMonths.Format(month);
            logger.LogInformation("Scylla archive: sweeping {Month} ({Tags} tags)", monthText, tags.Count);
            var monthComplete = true;
            var failedTags = 0;
            var consecutiveFailures = 0;
            foreach (var (tag, timeKeys) in tags)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (await markers.GetAsync(ScyllaArchiveMarkerKeys.TagMonth(tag, month)).ConfigureAwait(false) != null)
                    continue;
                var row = await GetTagRowAsync(tag, month, cancellationToken).ConfigureAwait(false);
                if (row?.ScyllaAt != null)
                    continue; // SkyIndexer already imported it (marker lost)
                TagMonthOutcome outcome;
                try
                {
                    outcome = await ProcessTagMonthAsync(tag, month, row, timeKeys, options, cancellationToken, retag?.ReadTag(tag)).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Scylla archive of {Tag} {Month} failed; nothing marked, retried next pass", tag, monthText);
                    outcome = new TagMonthOutcome { Tag = tag, Month = month, Result = TagMonthResult.Failed, Error = ex.Message };
                }
                if (outcome.Result == TagMonthResult.Failed)
                {
                    errors.Inc();
                    monthComplete = false;
                    failedTags++;
                    if (++consecutiveFailures >= options.CircuitBreakerFailures)
                    {
                        logger.LogWarning("Scylla archive: {Failures} tag-months failed in a row (last {Tag} {Month}: {Error}); pausing {Minutes} min",
                            consecutiveFailures, tag, monthText, outcome.Error, options.CircuitBreakerPauseMinutes);
                        await Delay(TimeSpan.FromMinutes(options.CircuitBreakerPauseMinutes), cancellationToken).ConfigureAwait(false);
                        consecutiveFailures = 0;
                    }
                    continue;
                }
                consecutiveFailures = 0;
                tagMonthsTotal.Inc();
                if (outcome.ScyllaRowsRead > 0)
                    await Delay(TimeSpan.FromMilliseconds(options.TagPauseMs), cancellationToken).ConfigureAwait(false);
            }
            if (!monthComplete)
            {
                logger.LogWarning("Scylla archive: {Month} is not complete ({Failed} tags failed); they are retried next pass", monthText, failedTags);
                return false;
            }
            var manifestError = await UpdateManifestAsync(month, m => m.CompletedAt ??= DateTime.UtcNow, cancellationToken).ConfigureAwait(false);
            if (manifestError != null)
            {
                errors.Inc();
                logger.LogError("Scylla archive: could not complete the manifest of {Month}: {Error}; retried next pass", monthText, manifestError);
                return false;
            }
            await markers.SetAsync(ScyllaArchiveMarkerKeys.Month(month), ScyllaArchiveMarkerKeys.Iso(DateTime.UtcNow)).ConfigureAwait(false);
            lastMonthCompleted.Set(DateTimeOffset.UtcNow.ToUnixTimeSeconds());
            logger.LogInformation("Scylla archive: every tag of {Month} done; manifest completed for SkyIndexer", monthText);
            await Delay(TimeSpan.FromMilliseconds(options.MonthPauseMs), cancellationToken).ConfigureAwait(false);
            return true;
        }

        /// <summary>Tags with at least one partition overlapping the month, sorted, with those partitions' timekeys.</summary>
        internal static List<(string Tag, List<short> TimeKeys)> TagsOfMonth(IReadOnlyDictionary<string, List<short>> byTag, int month)
        {
            var result = new List<(string, List<short>)>();
            foreach (var (tag, keys) in byTag.OrderBy(t => t.Key, StringComparer.Ordinal))
            {
                var (first, last) = ScyllaArchiveRows.TimeKeysForMonth(tag, month);
                var inMonth = keys.Where(k => k >= first && k <= last).ToList();
                if (inMonth.Count > 0)
                    result.Add((tag, inMonth));
            }
            return result;
        }

        /// <summary>The cached partition list, or a fresh scan (filtered to the configured months) when missing, stale or for other months.</summary>
        internal async Task<List<(string Tag, short TimeKey)>> GetPartitionsAsync(DateTime nowUtc, ScyllaArchiveOptions options, CancellationToken cancellationToken)
        {
            ScyllaPartitionList cached = null;
            try
            {
                cached = await meta.ReadMetaAsync<ScyllaPartitionList>(PartitionListKey, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning(ex, "Scylla archive: cached partition list unreadable, rescanning");
            }
            var fresh = cached != null
                && cached.OldestMonth == options.OldestMonth && cached.NewestMonth == options.NewestMonth
                && cached.CreatedAt > nowUtc.AddDays(-options.PartitionListMaxAgeDays) && cached.CreatedAt <= nowUtc.AddDays(1)
                && cached.Partitions != null;
            if (!fresh)
            {
                var found = await ScanPartitionsAsync(options, cancellationToken).ConfigureAwait(false);
                var inScope = found.Where(p => !string.IsNullOrEmpty(p.Tag) && ScyllaArchiveRows.OverlapsMonths(p.Tag, p.TimeKey, options.OldestMonth, options.NewestMonth))
                    .Distinct().OrderBy(p => p.Tag, StringComparer.Ordinal).ThenBy(p => p.TimeKey).ToList();
                cached = new ScyllaPartitionList { CreatedAt = nowUtc, Partitions = inScope, OldestMonth = options.OldestMonth, NewestMonth = options.NewestMonth };
                logger.LogInformation("Scylla archive: partition scan complete, {InScope} partitions of weekly_auctions_2 overlap {Oldest}..{Newest}",
                    inScope.Count, ArchiveMonths.Format(options.OldestMonth), ArchiveMonths.Format(options.NewestMonth));
                try
                {
                    await meta.WriteMetaAsync(PartitionListKey, cached, cancellationToken).ConfigureAwait(false);
                    // only now: if the cache write fails, the next pass takes the finished scan from Redis instead of rescanning
                    await markers.DeleteAsync(ScyllaArchiveMarkerKeys.PartitionScanFound, ScyllaArchiveMarkerKeys.PartitionScanNext).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    logger.LogWarning(ex, "Scylla archive: could not cache the partition list; the scan result stays in Redis for the next pass");
                }
            }
            partitionsKnown.Set(cached.Partitions.Count);
            return cached.Partitions;
        }

        /// <summary>
        /// Every in-scope partition key of <c>weekly_auctions_2</c>, scanned as <c>PARTITION_SCAN_RANGES</c> token ranges (a single
        /// <c>SELECT DISTINCT</c> over the 925 GB table times out). Progress is kept in Redis (<see cref="ScyllaArchiveMarkerKeys.PartitionScanFound"/>,
        /// <see cref="ScyllaArchiveMarkerKeys.PartitionScanNext"/>), so a restart continues with the next range; another range count
        /// or month scope restarts the scan.
        /// </summary>
        internal async Task<List<(string Tag, short TimeKey)>> ScanPartitionsAsync(ScyllaArchiveOptions options, CancellationToken cancellationToken)
        {
            var ranges = ScyllaTokenRange.Split(options.PartitionScanRanges);
            var progress = await markers.GetAsync(ScyllaArchiveMarkerKeys.PartitionScanNext).ConfigureAwait(false);
            var next = ScyllaArchiveMarkerKeys.ResumeRange(progress, ranges.Count, options.OldestMonth, options.NewestMonth);
            var found = new HashSet<(string Tag, short TimeKey)>();
            if (next == 0)
            {
                await markers.DeleteAsync(ScyllaArchiveMarkerKeys.PartitionScanFound, ScyllaArchiveMarkerKeys.PartitionScanNext).ConfigureAwait(false);
            }
            else
            {
                foreach (var member in await markers.GetSetAsync(ScyllaArchiveMarkerKeys.PartitionScanFound).ConfigureAwait(false))
                    if (ScyllaArchiveMarkerKeys.TryParsePartition(member, out var partition))
                        found.Add(partition);
                logger.LogInformation("Scylla archive: resuming the partition scan at range {Next}/{Total} with {Found} in scope", next, ranges.Count, found.Count);
            }
            partitionScanFound.Set(found.Count);
            partitionScanRangesDone.Set(next);
            for (var i = next; i < ranges.Count; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var inRange = await ScanTokenRangeAsync(ranges[i], options, 0, cancellationToken).ConfigureAwait(false);
                var added = inRange.Where(found.Add).Select(p => ScyllaArchiveMarkerKeys.Partition(p.Tag, p.TimeKey)).ToList();
                // found entries first, then the progress: a crash in between only re-reads this range
                await markers.AddToSetAsync(ScyllaArchiveMarkerKeys.PartitionScanFound, added).ConfigureAwait(false);
                await markers.SetAsync(ScyllaArchiveMarkerKeys.PartitionScanNext,
                    ScyllaArchiveMarkerKeys.ScanProgress(i + 1, ranges.Count, options.OldestMonth, options.NewestMonth)).ConfigureAwait(false);
                partitionScanRangesDone.Set(i + 1);
                partitionScanFound.Set(found.Count);
                if ((i + 1) % PartitionScanLogEvery == 0 || i + 1 == ranges.Count)
                    logger.LogInformation("Scylla archive: partition scan {Done}/{Total} ranges, {Found} in scope", i + 1, ranges.Count, found.Count);
                if (i + 1 < ranges.Count)
                    await Delay(TimeSpan.FromMilliseconds(options.PagePauseMs), cancellationToken).ConfigureAwait(false);
            }
            return found.ToList();
        }

        /// <summary>
        /// The in-scope partitions of one token range. Each page gets up to <c>PAGE_RETRIES</c> retries (2/4/8 s) on a Scylla timeout
        /// or unavailable error, resuming from its paging state; a range that still fails is halved and both halves are scanned,
        /// down to <see cref="MaxRangeSplitDepth"/> halvings, after which the pass fails (the scan resumes with this range).
        /// </summary>
        private async Task<HashSet<(string Tag, short TimeKey)>> ScanTokenRangeAsync(ScyllaTokenRange range, ScyllaArchiveOptions options, int depth, CancellationToken cancellationToken)
        {
            var found = new HashSet<(string Tag, short TimeKey)>();
            byte[] pagingState = null;
            while (true)
            {
                (IReadOnlyList<(string Tag, short TimeKey)> Rows, byte[] Next) page = default;
                Exception failure = null;
                for (var attempt = 0; ; attempt++)
                {
                    try
                    {
                        page = await FetchPartitionKeyPageAsync(range, pagingState, PartitionScanPageSize, cancellationToken).ConfigureAwait(false);
                        failure = null;
                        break;
                    }
                    catch (Exception ex) when (IsTransientScyllaError(ex))
                    {
                        failure = ex;
                        if (attempt >= options.PageRetries)
                            break;
                        partitionScanRetries.Inc();
                        var backoff = TimeSpan.FromSeconds(2 << attempt);
                        logger.LogWarning("Scylla archive: partition scan of token range {Range} timed out (attempt {Attempt}): {Error}; retrying in {Backoff}", range, attempt + 1, ex.Message, backoff);
                        await Delay(backoff, cancellationToken).ConfigureAwait(false);
                    }
                }
                if (failure != null)
                {
                    if (depth >= MaxRangeSplitDepth || !range.CanSplit)
                        throw new InvalidOperationException($"Partition scan of token range {range} kept timing out after {depth} halvings", failure);
                    partitionScanSplits.Inc();
                    logger.LogWarning("Scylla archive: token range {Range} keeps timing out; scanning its two halves", range);
                    var (low, high) = range.Halve();
                    found.UnionWith(await ScanTokenRangeAsync(low, options, depth + 1, cancellationToken).ConfigureAwait(false));
                    found.UnionWith(await ScanTokenRangeAsync(high, options, depth + 1, cancellationToken).ConfigureAwait(false));
                    return found;
                }
                pages.Inc();
                foreach (var (tag, timeKey) in page.Rows)
                    if (!string.IsNullOrEmpty(tag) && ScyllaArchiveRows.OverlapsMonths(tag, timeKey, options.OldestMonth, options.NewestMonth))
                        found.Add((tag, timeKey));
                if (page.Next == null || page.Next.Length == 0)
                    return found;
                pagingState = page.Next;
            }
        }

        /// <summary>Read/operation timeouts and unavailable replicas, which a retry or a smaller range can get past.</summary>
        internal static bool IsTransientScyllaError(Exception ex)
            => ex is QueryTimeoutException || ex is OperationTimedOutException || ex is UnavailableException;

        /// <summary>
        /// The phase-1 view of a tag-month, derived from its <c>archived_months</c> row (SkyIndexer only changes it when it imports
        /// the manifest, which also sets <c>scylla_at</c>) and the matching leading hashes of the R2 tag index entry.
        /// An index entry with more parts than the row holds the sweep's parts that SkyIndexer has not imported yet; they are
        /// written again (same part numbers). A mariadb row without index entry returns an entry with <c>PartSha256 = null</c>: the
        /// hashes are recomputed from the parts and must combine to the row's sha256. Returns null when phase 1 archived nothing;
        /// <paramref name="error"/> is set when the two do not agree.
        /// </summary>
        internal static ArchivedMonthEntry PhaseOneEntry(int month, ArchivedMonth row, ArchivedMonthEntry entry, out string error)
        {
            error = null;
            if (row == null)
            {
                if (entry != null && (entry.Source == ArchivedAuctionBlock.SourceMariaDb || entry.Source == ArchivedAuctionBlock.SourceMerged))
                    error = $"tag index has a {entry.Source} entry with {entry.Parts} parts but archived_months has no row";
                return null;
            }
            if (row.Parts == 0 && entry == null)
                return null;
            if (entry == null)
            {
                if (row.Source != ArchivedAuctionBlock.SourceMariaDb)
                {
                    error = $"archived_months has {row.Parts} {row.Source} parts but the tag index has no entry";
                    return null;
                }
                // the entry was lost (tag index race before writes were conditional): PartSha256 null = recompute from the parts
                return new ArchivedMonthEntry
                {
                    Month = month,
                    Parts = row.Parts,
                    Rows = (int)row.Rows,
                    SoldRows = (int)row.SoldRows,
                    PartSha256 = null,
                    Source = row.Source,
                    VerifiedAt = row.VerifiedAt ?? default,
                };
            }
            var hashes = entry.PartSha256 ?? new List<string>();
            if (hashes.Count != entry.Parts || entry.Parts < row.Parts)
            {
                error = $"tag index entry has {entry.Parts} parts and {hashes.Count} hashes, archived_months {row.Parts} parts";
                return null;
            }
            if (entry.Source == ArchivedAuctionBlock.SourceMariaDb && entry.Parts != row.Parts)
            {
                error = $"tag index has {entry.Parts} mariadb parts, archived_months {row.Parts}";
                return null;
            }
            var phaseOneHashes = hashes.Take(row.Parts).ToList();
            if (!string.Equals(ArchivedAuctionBlock.CombinedSha256(phaseOneHashes), row.Sha256, StringComparison.OrdinalIgnoreCase))
            {
                error = "archived_months sha256 does not match the tag index part hashes";
                return null;
            }
            return new ArchivedMonthEntry
            {
                Month = month,
                Parts = row.Parts,
                Rows = (int)row.Rows,
                SoldRows = (int)row.SoldRows,
                PartSha256 = phaseOneHashes,
                Source = row.Source,
                VerifiedAt = row.VerifiedAt ?? entry.VerifiedAt,
            };
        }

        private sealed class Winner
        {
            public Fingerprint Fingerprint;
            /// <summary>Spill sequence of the Scylla-only copy, -1 for an auction phase 1 has.</summary>
            public int Seq;
            /// <summary>Scylla-only copy of an auction that already has an auction_locations row in another tag-month.</summary>
            public bool Superseded;
        }

        private sealed class NewParts
        {
            public readonly List<string> Hashes = new();
            public int Rows;
            public int Sold;
        }

        /// <summary>Sweep one tag-month (see the class summary). Never throws for data problems; returns <see cref="TagMonthResult.Failed"/>.</summary>
        internal async Task<TagMonthOutcome> ProcessTagMonthAsync(string tag, int month, ArchivedMonth row, IReadOnlyList<short> timeKeys, ScyllaArchiveOptions options, CancellationToken cancellationToken,
            IEnumerable<(SaveAuction Row, ScyllaAuctionExtras Extras, Fingerprint Fp)> retagged = null)
        {
            var outcome = new TagMonthOutcome { Tag = tag, Month = month };
            var monthText = ArchiveMonths.Format(month);
            TagMonthOutcome Fail(string error)
            {
                outcome.Result = TagMonthResult.Failed;
                outcome.Error = error;
                logger.LogError("Scylla archive of {Tag} {Month} failed: {Error}; nothing marked, retried next pass", tag, monthText, error);
                return outcome;
            }

            // a. phase-1 state
            var index = await store.ReadIndexAsync(tag, cancellationToken).ConfigureAwait(false);
            var entry = index?.Find(month);
            var phaseOne = PhaseOneEntry(month, row, entry, out var inconsistency);
            if (inconsistency != null)
                return Fail(inconsistency);
            var n = phaseOne?.Parts ?? 0;
            outcome.PhaseOneParts = n;

            // b. phase-1 rows (uncached): uid -> fingerprint
            var phaseOneRows = new Dictionary<long, Fingerprint>();
            var recomputed = phaseOne != null && phaseOne.PartSha256 == null ? new List<string>() : null;
            for (var p = 0; p < n; p++)
            {
                var block = await store.ReadBlockAsync(tag, month, p, uncached: true, cancellationToken).ConfigureAwait(false);
                if (block == null)
                    return Fail($"phase-1 part {p} is missing");
                var sha = block.Sha256();
                if (recomputed != null)
                    recomputed.Add(sha);
                else if (sha != phaseOne.PartSha256[p])
                    return Fail($"phase-1 part {p} hashes to {sha}, the tag index says {phaseOne.PartSha256[p]}");
                for (var i = 0; i < block.RowCount; i++)
                    phaseOneRows[block.AuctionUid[i]] = Fingerprint.Of(block, i);
                outcome.PhaseOneRows += block.RowCount;
            }
            if (phaseOne != null && outcome.PhaseOneRows != phaseOne.Rows)
                return Fail($"phase-1 parts hold {outcome.PhaseOneRows} rows, archived_months says {phaseOne.Rows}");
            if (recomputed != null)
            {
                if (!string.Equals(ArchivedAuctionBlock.CombinedSha256(recomputed), row.Sha256, StringComparison.OrdinalIgnoreCase))
                    return Fail("the tag index has no entry and the recomputed phase-1 part hashes do not match archived_months sha256");
                phaseOne.PartSha256 = recomputed;
                logger.LogWarning("Scylla archive: tag index of {Tag} had no entry for {Month}; rebuilt from the {Parts} verified phase-1 parts", tag, monthText, n);
            }

            // c+d. stream Scylla, keep the best copy of every auction; Scylla-only candidates go to the spill
            var winners = new Dictionary<long, Winner>();
            using var spill = new ScyllaRowSpill(tag, month);
            foreach (var timeKey in timeKeys)
            {
                await foreach (var page in ReadPartitionPagesAsync(tag, timeKey, options, cancellationToken).ConfigureAwait(false))
                {
                    foreach (var scyllaRow in page)
                    {
                        outcome.ScyllaRowsRead++;
                        var end = ScyllaArchiveRows.EndOf(scyllaRow);
                        if (end < ScyllaArchiveRows.BogusEndBefore)
                        {
                            outcome.BogusEndRows++;
                            rowsSkippedBogusEnd.Inc();
                            continue;
                        }
                        if (ArchiveMonths.MonthOf(end) != month)
                        {
                            outcome.OutsideMonthRows++;
                            continue;
                        }
                        if (tag == UnknownTag && nameResolver != null && nameResolver.TryResolve(scyllaRow.ItemName, out _))
                            continue; // archived under its real tag by the pre-scan
                        outcome.ScyllaRowsInMonth++;
                        var uid = ScyllaArchiveRows.UidOf(scyllaRow);
                        var fingerprint = Fingerprint.Of(scyllaRow);
                        if (winners.TryGetValue(uid, out var current) && !ScyllaArchiveRows.IsBetter(fingerprint, current.Fingerprint))
                            continue;
                        var seq = -1;
                        if (!phaseOneRows.ContainsKey(uid))
                        {
                            var (mapped, extras) = ScyllaArchiveRows.Map(scyllaRow, tag, WarnUnknownValue);
                            seq = spill.Add(mapped, extras);
                        }
                        winners[uid] = new Winner { Fingerprint = fingerprint, Seq = seq };
                    }
                    await Delay(TimeSpan.FromMilliseconds(options.PagePauseMs), cancellationToken).ConfigureAwait(false);
                }
                partitionsRead.Inc();
                await Delay(TimeSpan.FromMilliseconds(options.PartitionPauseMs), cancellationToken).ConfigureAwait(false);
            }

            foreach (var (retaggedRow, retaggedExtras, retaggedFp) in retagged ?? Enumerable.Empty<(SaveAuction, ScyllaAuctionExtras, Fingerprint)>())
            {
                outcome.ScyllaRowsRead++;
                if (retaggedRow.End < ScyllaArchiveRows.BogusEndBefore)
                {
                    outcome.BogusEndRows++;
                    rowsSkippedBogusEnd.Inc();
                    continue;
                }
                if (ArchiveMonths.MonthOf(retaggedRow.End) != month)
                {
                    outcome.OutsideMonthRows++;
                    continue;
                }
                outcome.ScyllaRowsInMonth++;
                var retaggedUid = retaggedRow.UId;
                if (winners.TryGetValue(retaggedUid, out var retaggedCurrent) && !ScyllaArchiveRows.IsBetter(retaggedFp, retaggedCurrent.Fingerprint))
                    continue;
                var retaggedSeq = -1;
                if (!phaseOneRows.ContainsKey(retaggedUid))
                    retaggedSeq = spill.Add(retaggedRow, retaggedExtras);
                winners[retaggedUid] = new Winner { Fingerprint = retaggedFp, Seq = retaggedSeq };
            }

            // d2. a Scylla-only auction that is already located in another tag-month is a stale copy (an unsold snapshot whose
            // End moved into an earlier, already archived and imported month when it was bought): skipped. One located in this very
            // tag-month comes from an earlier run of it that SkyIndexer partly imported and is written again (its parts are rewritten).
            var candidates = winners.Where(w => w.Value.Seq >= 0).Select(w => w.Key).ToList();
            var located = await LookupLocationsAsync(candidates, cancellationToken).ConfigureAwait(false);
            if (located.Count > 0)
            {
                var thisTag = row?.TagId ?? await GetTagIdAsync(tag, cancellationToken).ConfigureAwait(false);
                var thisMonth = ArchiveMonths.ToIndex(month);
                foreach (var (uid, location) in located)
                {
                    if (location.TagId == thisTag && location.Month == thisMonth)
                        continue;
                    winners[uid].Superseded = true;
                    outcome.SupersededRows++;
                }
                rowsSuperseded.Inc(outcome.SupersededRows);
            }

            // e. cross-check the auctions phase 1 has (phase-1 data always wins)
            foreach (var (uid, winner) in winners)
            {
                if (winner.Seq >= 0)
                    continue;
                outcome.OverlapRows++;
                crosscheckRows.Inc();
                var archived = phaseOneRows[uid];
                var fields = Fingerprint.Compare(archived, winner.Fingerprint);
                if (fields.Count == 0)
                    continue;
                outcome.Mismatches++;
                foreach (var field in fields)
                    crosscheckMismatches.WithLabels(field).Inc();
                if (outcome.Mismatches <= options.CrosscheckLogLimit)
                    logger.LogWarning("Scylla archive cross-check {Tag} {Month} uid {Uid} differs in {Fields}: phase 1 {PhaseOne}, Scylla {Scylla}",
                        tag, monthText, uid, string.Join(",", fields), archived, winner.Fingerprint);
            }
            rowsAlreadyArchived.Inc(outcome.OverlapRows);
            outcome.ScyllaOnlyRows = winners.Count - outcome.OverlapRows - outcome.SupersededRows;

            if (outcome.ScyllaOnlyRows == 0)
                return await FinishWithoutNewRowsAsync(outcome, row, entry, phaseOne, Fail, cancellationToken).ConfigureAwait(false);

            // f. Scylla-only rows as parts N, N+1, ...; each verified by uncached read-back
            var parts = new NewParts();
            var buffer = new List<(SaveAuction Row, ScyllaAuctionExtras Extras)>();
            foreach (var (seq, mapped, extras) in spill.ReadAll())
            {
                var winner = winners[mapped.UId];
                if (winner.Seq != seq || winner.Superseded)
                    continue; // a better copy of the same auction came later, or the auction is archived elsewhere
                buffer.Add((mapped, extras));
                if (buffer.Count >= options.MaxRowsPerPart)
                {
                    var error = await FlushPartAsync(tag, month, n, buffer, parts, options, cancellationToken).ConfigureAwait(false);
                    if (error != null)
                        return Fail(error);
                }
            }
            if (buffer.Count > 0)
            {
                var error = await FlushPartAsync(tag, month, n, buffer, parts, options, cancellationToken).ConfigureAwait(false);
                if (error != null)
                    return Fail(error);
            }
            outcome.PartsWritten = parts.Hashes.Count;

            // g. tag index: phase-1 entry followed by the new parts (conditional)
            var now = DateTime.UtcNow;
            var merged = ScyllaArchiveRows.MergeMonthEntry(phaseOne, month, parts.Hashes, parts.Rows, parts.Sold, now);
            var indexError = await WriteTagIndexEntryAsync(tag, merged, cancellationToken).ConfigureAwait(false);
            if (indexError != null)
                return Fail(indexError);

            // h. hand-off to SkyIndexer (conditional, entry replaced by tag on a re-run)
            var import = new ScyllaTagImport
            {
                Tag = tag,
                FirstNewPart = n,
                NewParts = parts.Hashes.Count,
                NewRows = parts.Rows,
                NewSold = parts.Sold,
                NewPartSha256 = parts.Hashes.ToList(),
                VerifiedAt = now,
            };
            var manifestError = await UpdateManifestAsync(month, m => m.Upsert(import), cancellationToken).ConfigureAwait(false);
            if (manifestError != null)
                return Fail(manifestError);

            // i. the marker, last
            await markers.SetAsync(ScyllaArchiveMarkerKeys.TagMonth(tag, month), ScyllaArchiveMarkerKeys.Archived(outcome.ScyllaOnlyRows, parts.Hashes.Count, now)).ConfigureAwait(false);
            outcome.Result = TagMonthResult.Archived;
            logger.LogInformation("Scylla archive {Tag} {Month}: +{New} rows in {Parts} parts after {PhaseOne} phase-1 parts, {Overlap} overlap, {Mismatches} mismatches, {Superseded} superseded",
                tag, monthText, outcome.ScyllaOnlyRows, parts.Hashes.Count, n, outcome.OverlapRows, outcome.Mismatches, outcome.SupersededRows);
            return outcome;
        }

        /// <summary>No Scylla-only row: <c>overlap_only</c> for a tag-month phase 1 has, <c>empty</c> for one neither side has.</summary>
        private async Task<TagMonthOutcome> FinishWithoutNewRowsAsync(TagMonthOutcome outcome, ArchivedMonth row, ArchivedMonthEntry entry, ArchivedMonthEntry phaseOne, Func<string, TagMonthOutcome> fail, CancellationToken cancellationToken)
        {
            var now = DateTime.UtcNow;
            var markerKey = ScyllaArchiveMarkerKeys.TagMonth(outcome.Tag, outcome.Month);
            if (row == null)
            {
                if (entry != null)
                    return fail($"tag index has a {entry.Source} entry ({entry.Parts} parts) from an earlier run but Scylla has no new rows now; left as is for a manual look");
                await markers.SetAsync(markerKey, ScyllaArchiveMarkerKeys.Empty(now)).ConfigureAwait(false);
                outcome.Result = TagMonthResult.Empty;
                tagMonthsEmpty.Inc();
                return outcome;
            }
            if (phaseOne != null && entry != null && entry.Parts > phaseOne.Parts)
                return fail($"tag index has {entry.Parts - phaseOne.Parts} parts from an earlier run that Scylla no longer yields; left as is for a manual look");
            if (phaseOne != null && entry == null)
            {
                // the lost entry is recreated from the recomputed phase-1 hashes
                var restoreError = await WriteTagIndexEntryAsync(outcome.Tag, phaseOne, cancellationToken).ConfigureAwait(false);
                if (restoreError != null)
                    return fail(restoreError);
            }
            await markers.SetAsync(markerKey, ScyllaArchiveMarkerKeys.OverlapOnly(now)).ConfigureAwait(false);
            outcome.Result = TagMonthResult.NothingNew;
            return outcome;
        }

        /// <summary>Write the buffered rows as the next part, verify by uncached read-back; clears the buffer. Returns an error or null.</summary>
        private async Task<string> FlushPartAsync(string tag, int month, int phaseOneParts, List<(SaveAuction Row, ScyllaAuctionExtras Extras)> buffer, NewParts parts, ScyllaArchiveOptions options, CancellationToken cancellationToken)
        {
            var part = phaseOneParts + parts.Hashes.Count;
            if (part > options.EffectiveMaxPart)
                return $"needs part {part}, above MAX_PART {options.EffectiveMaxPart}; raise MAX_ROWS_PER_PART";
            var block = ArchivedAuctionBlock.FromAuctions(tag, month, part, ArchivedAuctionBlock.SourceScylla, buffer);
            await store.WriteBlockAsync(block, cancellationToken).ConfigureAwait(false);
            partsWritten.Inc();
            var readBack = await store.ReadBlockAsync(tag, month, part, uncached: true, cancellationToken).ConfigureAwait(false);
            var difference = block.FirstDifference(readBack);
            if (difference != null)
            {
                verifyFailures.Inc();
                logger.LogWarning("Read-back of {Tag} {Month} part {Part} differs in {Column}", tag, ArchiveMonths.Format(month), part, difference);
                return $"read-back of part {part} differs in {difference}";
            }
            partsVerified.Inc();
            rowsArchived.Inc(block.RowCount);
            parts.Hashes.Add(block.Sha256());
            parts.Rows += block.RowCount;
            parts.Sold += block.IsSold.Count(s => s);
            buffer.Clear();
            return null;
        }

        /// <summary><c>auction_locations</c> of the given uids, looked up in chunks of at most <see cref="LocationLookupChunkSize"/>.</summary>
        private async Task<Dictionary<long, (ushort TagId, ushort Month)>> LookupLocationsAsync(List<long> uids, CancellationToken cancellationToken)
        {
            var result = new Dictionary<long, (ushort, ushort)>();
            for (var offset = 0; offset < uids.Count; offset += LocationLookupChunkSize)
            {
                var chunk = uids.GetRange(offset, Math.Min(LocationLookupChunkSize, uids.Count - offset));
                foreach (var (uid, location) in await LookupLocatedUidsAsync(chunk, cancellationToken).ConfigureAwait(false))
                    result[uid] = location;
            }
            return result;
        }

        /// <summary>
        /// Upsert one month entry into the tag index. The write is conditional on the ETag that was read (SkyIndexer writes the same
        /// per-tag object for other months); on a conflict the index is re-read and the entry re-applied, up to
        /// <see cref="ArchiveIndexConflictException.MaxWriteAttempts"/> times. Read back afterwards. Returns an error or null.
        /// </summary>
        private async Task<string> WriteTagIndexEntryAsync(string tag, ArchivedMonthEntry entry, CancellationToken cancellationToken)
        {
            for (var attempt = 1; ; attempt++)
            {
                var index = await store.ReadIndexAsync(tag, cancellationToken).ConfigureAwait(false) ?? new ArchivedTagIndex { Tag = tag };
                index.Tag ??= tag;
                index.Upsert(entry);
                try
                {
                    await store.WriteIndexAsync(index, cancellationToken).ConfigureAwait(false);
                    break;
                }
                catch (ArchiveIndexConflictException) when (attempt < ArchiveIndexConflictException.MaxWriteAttempts)
                {
                    logger.LogInformation("Scylla archive: tag index of {Tag} changed by another writer (attempt {Attempt}); re-reading", tag, attempt);
                }
                catch (ArchiveIndexConflictException)
                {
                    return $"tag index kept changing ({attempt} conditional writes rejected)";
                }
            }
            var check = (await store.ReadIndexAsync(tag, cancellationToken).ConfigureAwait(false))?.Find(entry.Month);
            if (check == null || check.Parts != entry.Parts || check.Source != entry.Source || !(check.PartSha256 ?? new List<string>()).SequenceEqual(entry.PartSha256))
                return "tag index entry was not persisted as written (concurrent writer?)";
            return null;
        }

        /// <summary>
        /// Read-modify-write of the month's <see cref="ScyllaMonthManifest"/> with a conditional PUT, re-read and re-applied on a
        /// conflict up to <see cref="ArchiveIndexConflictException.MaxWriteAttempts"/> times. Returns an error or null.
        /// </summary>
        private async Task<string> UpdateManifestAsync(int month, Action<ScyllaMonthManifest> change, CancellationToken cancellationToken)
        {
            var key = AuctionBlobKeys.ScyllaManifest(month);
            for (var attempt = 1; ; attempt++)
            {
                var (manifest, etag) = await meta.ReadMetaVersionedAsync<ScyllaMonthManifest>(key, cancellationToken).ConfigureAwait(false);
                manifest ??= new ScyllaMonthManifest { Month = month };
                change(manifest);
                try
                {
                    await meta.WriteMetaConditionalAsync(key, manifest, etag, cancellationToken).ConfigureAwait(false);
                    return null;
                }
                catch (ArchiveIndexConflictException) when (attempt < ArchiveIndexConflictException.MaxWriteAttempts)
                {
                    logger.LogInformation("Scylla archive: manifest of {Month} changed meanwhile (attempt {Attempt}); re-reading", ArchiveMonths.Format(month), attempt);
                }
                catch (ArchiveIndexConflictException)
                {
                    return $"manifest kept changing ({attempt} conditional writes rejected)";
                }
            }
        }

        private void WarnUnknownValue(string column, string value)
        {
            if (warnedValues.TryAdd(column + ":" + value, true))
                logger.LogWarning("Scylla archive: unknown {Column} value {Value} archived as UNKNOWN", column, value);
        }

        // ---------------------------------------------------------------- data access seams (MariaDB is read only)

        /// <summary>
        /// One cheap read: the newest sentinel's <c>scylla_at</c>. Fails while MariaDB is unreachable and until SkyIndexer has applied
        /// the <c>ArchiveScyllaAt</c> migration (SkyAuctions never migrates; deploy SkyIndexer first).
        /// </summary>
        protected virtual async Task VerifyDatabaseAsync(CancellationToken cancellationToken)
        {
            using var db = new ArchiveContext();
            await db.ArchivedMonths.AsNoTracking().Where(m => m.TagId == 0).OrderByDescending(m => m.Month)
                .Select(m => m.ScyllaAt).Take(1).ToListAsync(cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// One page of the partition keys in a token range (<c>SELECT DISTINCT tag, timekey ... WHERE token(tag, timekey) ... BYPASS CACHE</c>,
        /// prepared, LocalOne, explicit paging) and the next paging state (null at the end of the range).
        /// </summary>
        protected virtual async Task<(IReadOnlyList<(string Tag, short TimeKey)> Rows, byte[] Next)> FetchPartitionKeyPageAsync(ScyllaTokenRange range, byte[] pagingState, int pageSize, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            PreparedStatement prepared;
            if (range.StartInclusive)
                prepared = firstRangeStatement ??= await scylla.Session.PrepareAsync(FirstRangeCql).ConfigureAwait(false);
            else
                prepared = rangeStatement ??= await scylla.Session.PrepareAsync(RangeCql).ConfigureAwait(false);
            // token() bounds are CQL bigint: bind them as long
            var bound = prepared.Bind(range.Start, range.End);
            bound.SetPageSize(pageSize);
            bound.SetConsistencyLevel(ConsistencyLevel.LocalOne);
            bound.SetAutoPage(false);
            if (pagingState != null)
                bound.SetPagingState(pagingState);
            var rows = await scylla.Session.ExecuteAsync(bound).ConfigureAwait(false);
            var result = new List<(string, short)>();
            foreach (var row in rows) // auto paging is off: only this page
                result.Add((row.GetValue<string>("tag"), row.GetValue<short>("timekey")));
            return (result, rows.PagingState);
        }

        /// <summary>Sentinel (<c>tag_id = 0</c>) of a month: does it exist, and its <c>scylla_at</c>.</summary>
        protected virtual async Task<ScyllaMonthState> GetMonthStateAsync(int month, CancellationToken cancellationToken)
        {
            var index = ArchiveMonths.ToIndex(month);
            using var db = new ArchiveContext();
            var sentinel = await db.ArchivedMonths.AsNoTracking().Where(m => m.TagId == 0 && m.Month == index)
                .Select(m => new { m.ScyllaAt }).FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
            return new ScyllaMonthState(sentinel != null, sentinel?.ScyllaAt);
        }

        /// <summary>Id of a tag in <c>auction_tags</c>, null when it has none (read only, cached).</summary>
        protected virtual async Task<ushort?> GetTagIdAsync(string tag, CancellationToken cancellationToken)
        {
            if (tagIds.TryGetValue(tag, out var cached))
                return cached;
            using var db = new ArchiveContext();
            var id = await db.AuctionTags.AsNoTracking().Where(t => t.Tag == tag).Select(t => (ushort?)t.Id).FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
            if (id != null)
                tagIds[tag] = id.Value;
            return id;
        }

        /// <summary>The columns of a tag-month's <c>archived_months</c> row the sweep needs (projection), null when the tag or the row does not exist.</summary>
        protected virtual async Task<ArchivedMonth> GetTagRowAsync(string tag, int month, CancellationToken cancellationToken)
        {
            var tagId = await GetTagIdAsync(tag, cancellationToken).ConfigureAwait(false);
            if (tagId == null)
                return null;
            var index = ArchiveMonths.ToIndex(month);
            using var db = new ArchiveContext();
            return await db.ArchivedMonths.AsNoTracking().Where(m => m.TagId == tagId && m.Month == index)
                .Select(m => new ArchivedMonth
                {
                    TagId = m.TagId,
                    Month = m.Month,
                    Parts = m.Parts,
                    Rows = m.Rows,
                    SoldRows = m.SoldRows,
                    Source = m.Source,
                    Sha256 = m.Sha256,
                    ArchivedAt = m.ArchivedAt,
                    VerifiedAt = m.VerifiedAt,
                    ScyllaAt = m.ScyllaAt,
                }).FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Pages of one partition. Each page is fetched with up to <c>PAGE_RETRIES</c> retries (2 s, 4 s, 8 s, ...) from the same
        /// paging state, so a timeout mid-partition never restarts or skips rows; after the last retry the exception propagates
        /// and the tag-month fails without markers.
        /// </summary>
        protected virtual async IAsyncEnumerable<IReadOnlyList<ScyllaAuction>> ReadPartitionPagesAsync(string tag, short timeKey, ScyllaArchiveOptions options, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            byte[] pagingState = null;
            while (true)
            {
                (IReadOnlyList<ScyllaAuction> Rows, byte[] Next) page;
                for (var attempt = 0; ; attempt++)
                {
                    try
                    {
                        page = await FetchPageAsync(tag, timeKey, pagingState, options.PageSize, cancellationToken).ConfigureAwait(false);
                        break;
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
                    {
                        if (attempt >= options.PageRetries)
                            throw new InvalidOperationException($"Page of {tag} timekey {timeKey} failed {attempt + 1} times", ex);
                        pageRetries.Inc();
                        var backoff = TimeSpan.FromSeconds(2 << attempt);
                        logger.LogWarning(ex, "Scylla page of {Tag} timekey {TimeKey} failed (attempt {Attempt}); retrying from the same paging state in {Backoff}", tag, timeKey, attempt + 1, backoff);
                        await Delay(backoff, cancellationToken).ConfigureAwait(false);
                    }
                }
                pages.Inc();
                yield return page.Rows;
                if (page.Next == null || page.Next.Length == 0)
                    yield break;
                pagingState = page.Next;
            }
        }

        /// <summary>One page of <c>WHERE tag = ? AND timekey = ?</c> (raw rows, LocalOne, explicit paging state) and the next paging state (null at the end).</summary>
        protected virtual async Task<(IReadOnlyList<ScyllaAuction> Rows, byte[] Next)> FetchPageAsync(string tag, short timeKey, byte[] pagingState, int pageSize, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var query = scylla.AuctionsTable.Where(a => a.Tag == tag && a.TimeKey == timeKey);
            query.SetPageSize(pageSize);
            query.SetConsistencyLevel(ConsistencyLevel.LocalOne);
            if (pagingState != null)
                query.SetPagingState(pagingState);
            var page = await query.ExecutePagedAsync().ConfigureAwait(false);
            return (page.ToList(), page.PagingState);
        }

        /// <summary>Location (tag id, month index) of those of up to <see cref="LocationLookupChunkSize"/> uids that have an <c>auction_locations</c> row (primary key lookups).</summary>
        protected virtual async Task<IReadOnlyDictionary<long, (ushort TagId, ushort Month)>> LookupLocatedUidsAsync(IReadOnlyList<long> uids, CancellationToken cancellationToken)
        {
            using var db = new ArchiveContext();
            var list = uids.ToList();
            var rows = await db.AuctionLocations.AsNoTracking().Where(l => list.Contains(l.Uid)).Select(l => new { l.Uid, l.TagId, l.Month }).ToListAsync(cancellationToken).ConfigureAwait(false);
            var result = new Dictionary<long, (ushort, ushort)>();
            foreach (var r in rows)
                result[r.Uid] = (r.TagId, r.Month);
            return result;
        }

        /// <summary>Wait between steps (overridden in tests).</summary>
        protected virtual Task Delay(TimeSpan delay, CancellationToken cancellationToken)
        {
            return delay <= TimeSpan.Zero ? Task.CompletedTask : Task.Delay(delay, cancellationToken);
        }
    }
}
