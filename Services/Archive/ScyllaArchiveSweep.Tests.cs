using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.Core;
using Coflnet.Sky.Core.Archive;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using NUnit.Framework;

namespace Coflnet.Sky.Auctions.Archive;

public class ScyllaArchiveSweepTests
{
    private static readonly DateTime Now = new(2026, 10, 23, 12, 0, 0, DateTimeKind.Utc);
    private const int Aug = 202008;
    private const int Sep = 202009;
    private const string Hyperion = "HYPERION";
    private const string Terminator = "TERMINATOR";
    private static readonly string AugManifest = AuctionBlobKeys.ScyllaManifest(Aug);
    private static readonly string SepManifest = AuctionBlobKeys.ScyllaManifest(Sep);

    /// <summary>
    /// Sweep over in-memory MariaDB rows (read only: the sweep has no write seam), Scylla pages and Redis markers; the real
    /// paging/retry loop runs on top of <see cref="FetchPageAsync"/>.
    /// </summary>
    private class TestableScyllaArchiveSweep : ScyllaArchiveSweep
    {
        public readonly List<(string Tag, short TimeKey)> PartitionList = new();
        public int PartitionScans;
        public readonly Dictionary<(string tag, short key), List<List<ScyllaAuction>>> Pages = new();
        /// <summary>Remaining failures of a page (tag, key, page index).</summary>
        public readonly Dictionary<(string tag, short key, int page), int> PageFailures = new();
        public readonly List<(string tag, short key, int? state)> Fetches = new();
        public readonly Dictionary<string, ushort> Tags = new();
        public readonly Dictionary<(ushort tag, ushort month), ArchivedMonth> Months = new();
        public readonly Dictionary<long, (ushort tag, ushort month, byte part)> Locations = new();
        public readonly List<int> LocationChunks = new();
        public readonly List<TimeSpan> Delays = new();
        public int DatabaseFailures;
        public int DatabaseChecks;
        public ItemNameTagResolver Resolver = new();
        public int ResolverLoads;

        protected override Task<ItemNameTagResolver> LoadNameResolverAsync(CancellationToken cancellationToken)
        {
            ResolverLoads++;
            return Task.FromResult(Resolver);
        }

        public TestableScyllaArchiveSweep(InMemoryAuctionBlobStore store, InMemoryMarkers markers, IConfiguration config, ILogger<ScyllaArchiveSweep> logger)
            : base(store, store, markers, null, config, logger)
            => firstRange = ScyllaTokenRange.Split(ScyllaArchiveOptions.From(config).PartitionScanRanges)[0];

        protected override Task VerifyDatabaseAsync(CancellationToken cancellationToken)
        {
            DatabaseChecks++;
            if (DatabaseFailures-- > 0)
                throw new InvalidOperationException("Unknown column 'scylla_at'");
            return Task.CompletedTask;
        }

        /// <summary>Stand-in for Murmur3: any stable 64-bit hash works, the scan only needs every partition in exactly one range.</summary>
        public static long FakeToken((string Tag, short TimeKey) p)
        {
            var hash = 14695981039346656037UL;
            foreach (var c in p.Tag + ":" + p.TimeKey)
                hash = (hash ^ c) * 1099511628211UL;
            return unchecked((long)hash);
        }

        public int FakePartitionPageSize = 3;
        public readonly List<(ScyllaTokenRange range, int? state)> PartitionFetches = new();
        /// <summary>Ranges answered with a ReadTimeoutException.</summary>
        public Func<ScyllaTokenRange, bool> TimeoutRange;
        private readonly ScyllaTokenRange firstRange;

        protected override Task<(IReadOnlyList<(string Tag, short TimeKey)> Rows, byte[] Next)> FetchPartitionKeyPageAsync(ScyllaTokenRange range, byte[] pagingState, int pageSize, CancellationToken cancellationToken)
        {
            var index = pagingState == null ? 0 : BitConverter.ToInt32(pagingState);
            PartitionFetches.Add((range, pagingState == null ? null : index));
            if (range == firstRange && pagingState == null)
                PartitionScans++;
            if (TimeoutRange?.Invoke(range) == true)
                throw new global::Cassandra.ReadTimeoutException(global::Cassandra.ConsistencyLevel.LocalOne, 0, 1, false);
            var inRange = PartitionList.Where(p => range.Contains(FakeToken(p))).OrderBy(FakeToken).ToList();
            IReadOnlyList<(string, short)> rows = inRange.Skip(index * FakePartitionPageSize).Take(FakePartitionPageSize).ToList();
            var next = (index + 1) * FakePartitionPageSize < inRange.Count ? BitConverter.GetBytes(index + 1) : null;
            return Task.FromResult((rows, next));
        }

        protected override Task<ScyllaMonthState> GetMonthStateAsync(int month, CancellationToken cancellationToken)
        {
            var found = Months.TryGetValue((0, ArchiveMonths.ToIndex(month)), out var sentinel);
            return Task.FromResult(new ScyllaMonthState(found, sentinel?.ScyllaAt));
        }

        /// <summary>What the operator's MariaDB reset looks like to the redo wait (sentinel imported, tag rows, location rows).</summary>
        public (bool SentinelImported, int TagRows, bool HasLocations) RedoState = (true, 2, true);
        public int RedoStateReads;

        protected override Task<(bool SentinelImported, int TagRows, bool HasLocations)> GetRedoStateAsync(int month, CancellationToken cancellationToken)
        {
            RedoStateReads++;
            return Task.FromResult(RedoState);
        }

        protected override Task<ushort?> GetTagIdAsync(string tag, CancellationToken cancellationToken)
            => Task.FromResult(Tags.TryGetValue(tag, out var id) ? (ushort?)id : null);

        protected override Task<ArchivedMonth> GetTagRowAsync(string tag, int month, CancellationToken cancellationToken)
            => Task.FromResult(Row(tag, month));

        protected override Task<(IReadOnlyList<ScyllaAuction> Rows, byte[] Next)> FetchPageAsync(string tag, short timeKey, byte[] pagingState, int pageSize, CancellationToken cancellationToken)
        {
            var index = pagingState == null ? 0 : BitConverter.ToInt32(pagingState);
            Fetches.Add((tag, timeKey, pagingState == null ? null : index));
            if (PageFailures.TryGetValue((tag, timeKey, index), out var left) && left > 0)
            {
                PageFailures[(tag, timeKey, index)] = left - 1;
                throw new TimeoutException($"read timeout {tag} {timeKey} page {index}");
            }
            var pages = Pages.TryGetValue((tag, timeKey), out var p) ? p : new List<List<ScyllaAuction>>();
            IReadOnlyList<ScyllaAuction> rows = index < pages.Count ? pages[index] : new List<ScyllaAuction>();
            var next = index + 1 < pages.Count ? BitConverter.GetBytes(index + 1) : null;
            return Task.FromResult((rows, next));
        }

        protected override Task<IReadOnlyDictionary<long, (ushort TagId, ushort Month)>> LookupLocatedUidsAsync(IReadOnlyList<long> uids, CancellationToken cancellationToken)
        {
            LocationChunks.Add(uids.Count);
            IReadOnlyDictionary<long, (ushort, ushort)> found = uids.Where(Locations.ContainsKey).ToDictionary(u => u, u => (Locations[u].tag, Locations[u].month));
            return Task.FromResult(found);
        }

        protected override Task Delay(TimeSpan delay, CancellationToken cancellationToken)
        {
            Delays.Add(delay);
            return Task.CompletedTask;
        }

        public Task<int> Run() => RunOnceAsync(Now, CancellationToken.None);

        public ArchivedMonth Row(string tag, int month) => Tags.TryGetValue(tag, out var id) && Months.TryGetValue((id, ArchiveMonths.ToIndex(month)), out var r) ? r : null;
        public ArchivedMonth Sentinel(int month) => Months.TryGetValue((0, ArchiveMonths.ToIndex(month)), out var r) ? r : null;
    }

    private List<string> events;
    private InMemoryAuctionBlobStore store;
    private InMemoryMarkers markers;
    private ListLogger<ScyllaArchiveSweep> log;
    private TestableScyllaArchiveSweep sweep;
    private int rowsPerPage;

    [SetUp]
    public void Setup()
    {
        events = new List<string>();
        store = new InMemoryAuctionBlobStore(events);
        markers = new InMemoryMarkers(events);
        log = new ListLogger<ScyllaArchiveSweep>();
        rowsPerPage = 2;
        sweep = Sweep();
    }

    private TestableScyllaArchiveSweep Sweep(params (string, string)[] config)
        => new(store, markers, ArchiveTestConfig.Build(config), log);

    private static string Hex(long value) => value.ToString("x32");

    /// <summary>A Scylla row as ToCassandra would have stored it.</summary>
    private static ScyllaAuction Scylla(long uid, DateTime end, string tag = Hyperion, long bid = 0, bool? sold = null, params (long bidder, long amount)[] bids)
    {
        var row = new ScyllaAuction
        {
            Uuid = Guid.Parse(Hex(uid)),
            AuctionUid = uid,
            Tag = tag,
            TimeKey = ScyllaService.GetWeekOrDaysSinceStart(tag, end),
            End = end,
            Start = end.AddDays(-1),
            ItemCreatedAt = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            Auctioneer = Guid.Parse(Hex(1000 + uid)),
            ProfileId = Guid.Parse(Hex(2000 + uid)),
            HighestBidAmount = bid,
            IsSold = sold ?? bid > 0,
            StartingBid = 10,
            Count = 1,
            Tier = "LEGENDARY",
            Category = "WEAPON",
            ItemName = "Item " + uid,
            ItemBytes = new byte[] { 10, 0, (byte)uid, (byte)(uid >> 8) },
            Enchantments = new Dictionary<string, int> { ["sharpness"] = 5 },
            NbtLookup = new Dictionary<string, string> { ["uid"] = uid.ToString("x") },
            ItemUid = uid,
            ItemId = Guid.Parse(Hex(9000 + uid)),
            HighestBidderName = bids.Length > 0 ? "bidder" : null,
            Color = null,
        };
        row.Bids = bids.Select(b => new CassandraBid { AuctionUuid = row.Uuid, BidderUuid = Guid.Parse(Hex(b.bidder)), Amount = b.amount, Timestamp = end.AddHours(-1), ProfileId = Guid.Parse(Hex(b.bidder)) }).ToList();
        return row;
    }

    /// <summary>The MariaDB version of the same auction (what phase 1 archived).</summary>
    private static SaveAuction Maria(ScyllaAuction scylla) => ScyllaArchiveRows.Map(Clone(scylla), scylla.Tag).Row;

    private static ScyllaAuction Clone(ScyllaAuction r)
        => (ScyllaAuction)r.GetType().GetMethod("MemberwiseClone", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic).Invoke(r, null);

    /// <summary>Put rows into their partitions (pages of <see cref="rowsPerPage"/>) and the partition list.</summary>
    private void AddScylla(params ScyllaAuction[] rows)
    {
        foreach (var row in rows)
        {
            var key = (row.Tag, row.TimeKey);
            if (!sweep.Pages.TryGetValue(key, out var pages))
                sweep.Pages[key] = pages = new List<List<ScyllaAuction>>();
            if (pages.Count == 0 || pages[^1].Count >= rowsPerPage)
                pages.Add(new List<ScyllaAuction>());
            pages[^1].Add(row);
            if (!sweep.PartitionList.Contains(key))
                sweep.PartitionList.Add(key);
        }
    }

    /// <summary>Phase 1 (SkyIndexer) archived <paramref name="rows"/> of a tag-month in parts of <paramref name="perPart"/> rows.</summary>
    private void SeedPhaseOne(string tag, int month, IReadOnlyList<SaveAuction> rows, int perPart = 1000)
    {
        var hashes = new List<string>();
        var sold = 0;
        for (var p = 0; p * perPart < rows.Count; p++)
        {
            var block = ArchivedAuctionBlock.FromAuctions(tag, month, p, ArchivedAuctionBlock.SourceMariaDb, rows.Skip(p * perPart).Take(perPart));
            store.WriteBlockAsync(block).Wait();
            hashes.Add(block.Sha256());
            sold += block.IsSold.Count(s => s);
        }
        var index = store.ReadIndexAsync(tag).Result ?? new ArchivedTagIndex { Tag = tag };
        index.Upsert(new ArchivedMonthEntry { Month = month, Parts = hashes.Count, Rows = rows.Count, SoldRows = sold, PartSha256 = hashes, Source = ArchivedAuctionBlock.SourceMariaDb, VerifiedAt = Now.AddDays(-1) });
        store.WriteIndexAsync(index).Wait();
        var tagId = sweep.Tags.TryGetValue(tag, out var id) ? id : sweep.Tags[tag] = (ushort)(sweep.Tags.Count + 1);
        sweep.Months[(tagId, ArchiveMonths.ToIndex(month))] = new ArchivedMonth
        {
            TagId = tagId, Month = ArchiveMonths.ToIndex(month), Parts = (byte)hashes.Count, Rows = (uint)rows.Count, SoldRows = (uint)sold,
            Source = ArchivedAuctionBlock.SourceMariaDb, Sha256 = ArchivedAuctionBlock.CombinedSha256(hashes), ArchivedAt = Now.AddDays(-1), VerifiedAt = Now.AddDays(-1),
            IndexRows = (uint)rows.Count, IndexBidderRows = 0,
        };
        foreach (var r in rows)
            sweep.Locations[r.UId] = (tagId, ArchiveMonths.ToIndex(month), 0);
        Sentinel(month);
        events.Clear();
    }

    private void Sentinel(int month)
    {
        var key = ((ushort)0, ArchiveMonths.ToIndex(month));
        if (!sweep.Months.ContainsKey(key))
            sweep.Months[key] = new ArchivedMonth { TagId = 0, Month = key.Item2, Source = "mariadb", Sha256 = new string('0', 64), ArchivedAt = Now, VerifiedAt = Now };
    }

    /// <summary>What SkyIndexer's import does to the sentinel once it indexed the month's manifest.</summary>
    private void Imported(int month)
    {
        Sentinel(month);
        sweep.Sentinel(month).ScyllaAt = Now;
    }

    private static DateTime InSep(int day, int hour = 12) => new(2020, 9, day, hour, 0, 0, DateTimeKind.Utc);

    private List<SaveAuction> PartRows(string tag, int month, int part) => store.ReadBlockAsync(tag, month, part).Result?.ToAuctions().ToList();

    private ScyllaMonthManifest Manifest(int month) => store.ReadMetaAsync<ScyllaMonthManifest>(AuctionBlobKeys.ScyllaManifest(month)).Result;

    private static ScyllaArchiveOptions Options(params (string, string)[] config) => ScyllaArchiveOptions.From(ArchiveTestConfig.Build(config));

    private bool AnyWriteOrMarker() => events.Any(e => e.StartsWith("write:") || e.StartsWith("tag_index:") || e.StartsWith("meta:auctions") || e.StartsWith("marker:"));

    [Test]
    public async Task MonthsWaitForTheirPhaseOneSentinel_AndForTheImportOfThePreviousMonth()
    {
        AddScylla(Scylla(1, InSep(3)), Scylla(2, InSep(20)));
        Sentinel(Aug);

        Assert.That(await sweep.Run(), Is.EqualTo(1), "August (no tags) completes");
        var aug = Manifest(Aug);
        Assert.That(aug.CompletedAt, Is.Not.Null, "completed even without new rows, so SkyIndexer can set scylla_at");
        Assert.That(aug.Tags, Is.Empty);
        Assert.That(markers.Values.ContainsKey(ScyllaArchiveMarkerKeys.Month(Aug)), Is.True);

        events.Clear();
        Assert.That(await sweep.Run(), Is.Zero);
        Assert.That(AnyWriteOrMarker(), Is.False, "September waits until SkyIndexer imported August");
        Assert.That(await sweep.Run(), Is.Zero);
        Assert.That(log.Entries.Count(e => e.message.Contains("waiting for SkyIndexer to import 2020-08")), Is.EqualTo(1), "logged once");

        Imported(Aug);
        Assert.That(await sweep.Run(), Is.Zero);
        Assert.That(log.Entries.Any(e => e.message.Contains("2020-09 has no phase-1 sentinel")), Is.True);
        Assert.That(AnyWriteOrMarker(), Is.False);

        Sentinel(Sep);
        Assert.That(await sweep.Run(), Is.EqualTo(1));
        Assert.That(markers.Values[ScyllaArchiveMarkerKeys.TagMonth(Hyperion, Sep)], Does.StartWith("archived:2:1:"));
        Assert.That(PartRows(Hyperion, Sep, 0).Select(r => r.UId), Is.EqualTo(new long[] { 1, 2 }));
        Assert.That(markers.Values.ContainsKey(ScyllaArchiveMarkerKeys.SweepComplete), Is.True, "September is the newest month");
        Assert.That(log.Entries.Count(e => e.message.Contains("Scylla archive sweep complete through 2020-09")), Is.EqualTo(1));
        Assert.That(sweep.PartitionScans, Is.EqualTo(1), "the partition list is cached in R2");

        events.Clear();
        Assert.That(await sweep.Run(), Is.Zero);
        Assert.That(events, Is.Empty, "a finished sweep only reads the sweep_complete marker");
    }

    [Test]
    public async Task PipelineGate_TheNextMonthWaitsForTheImportOfThePreviousMonth()
    {
        // September is ready on the phase-1 side, but its superseded check needs August's locations, which SkyIndexer writes
        Sentinel(Aug);
        Sentinel(Sep);
        AddScylla(Scylla(1, new DateTime(2020, 8, 20, 0, 0, 0, DateTimeKind.Utc)), Scylla(2, InSep(3)));

        Assert.That(await sweep.Run(), Is.EqualTo(1));
        Assert.That(Manifest(Aug).Find(Hyperion), Is.Not.Null);
        Assert.That(Manifest(Sep), Is.Null);

        events.Clear();
        Assert.That(await sweep.Run(), Is.Zero);
        Assert.That(await sweep.Run(), Is.Zero);
        Assert.That(AnyWriteOrMarker(), Is.False, "September is not started before August is imported");
        Assert.That(sweep.Fetches.Any(f => f.key == ScyllaService.GetWeekOrDaysSinceStart(Hyperion, InSep(3))), Is.False);
        Assert.That(log.Entries.Count(e => e.message.Contains("waiting for SkyIndexer to import 2020-08")), Is.EqualTo(1));

        Imported(Aug);
        Assert.That(await sweep.Run(), Is.EqualTo(1));
        Assert.That(Manifest(Sep).Find(Hyperion).NewRows, Is.EqualTo(1));
    }

    [Test]
    public async Task GatedPassPausesImportWaitMinutes_CompleteSweepPausesIdleMinutes()
    {
        sweep = Sweep(("ARCHIVE:IMPORT_WAIT_MINUTES", "5"), ("ARCHIVE:IDLE_PAUSE_MINUTES", "60"));
        var options = Options(("ARCHIVE:IMPORT_WAIT_MINUTES", "5"), ("ARCHIVE:IDLE_PAUSE_MINUTES", "60"));
        Sentinel(Aug);
        Sentinel(Sep);

        await sweep.Run(); // finishes August, September must wait for the import
        Assert.That(sweep.PauseAfterPass(options), Is.EqualTo(TimeSpan.FromMinutes(5)));
        await sweep.Run(); // still gated
        Assert.That(sweep.PauseAfterPass(options), Is.EqualTo(TimeSpan.FromMinutes(5)));

        Imported(Aug);
        await sweep.Run(); // September, the newest month: sweep complete
        Assert.That(markers.Values.ContainsKey(ScyllaArchiveMarkerKeys.SweepComplete), Is.True);
        Assert.That(sweep.PauseAfterPass(options), Is.EqualTo(TimeSpan.FromMinutes(60)));
        await sweep.Run();
        Assert.That(sweep.PauseAfterPass(options), Is.EqualTo(TimeSpan.FromMinutes(60)));
    }

    [Test]
    public async Task TagMonth_WritesVerifiesThenTagIndexManifestMarker_InThatOrder()
    {
        sweep = Sweep(("ARCHIVE:MAX_ROWS_PER_PART", "2"));
        var overlap = new[] { Scylla(1, InSep(2), bid: 50), Scylla(2, InSep(4)) };
        SeedPhaseOne(Hyperion, Sep, overlap.Select(Maria).ToList());
        Imported(Aug);
        AddScylla(overlap);
        AddScylla(Scylla(3, InSep(5)), Scylla(4, InSep(10)), Scylla(5, InSep(28)));

        Assert.That(await sweep.Run(), Is.EqualTo(1));

        Assert.That(events, Is.EqualTo(new[]
        {
            "meta:" + ScyllaArchiveSweep.PartitionListKey,
            $"read:{Hyperion}:{Sep}:0:uncached",          // phase-1 part, never rewritten
            $"write:{Hyperion}:{Sep}:1", $"read:{Hyperion}:{Sep}:1:uncached",
            $"write:{Hyperion}:{Sep}:2", $"read:{Hyperion}:{Sep}:2:uncached",
            $"tag_index:{Hyperion}",
            "meta:" + SepManifest,
            "marker:" + ScyllaArchiveMarkerKeys.TagMonth(Hyperion, Sep),
            "meta:" + SepManifest,                           // CompletedAt
            "marker:" + ScyllaArchiveMarkerKeys.Month(Sep),
            "marker:" + ScyllaArchiveMarkerKeys.SweepComplete,
        }));
        var manifest = Manifest(Sep);
        var import = manifest.Tags.Single();
        var entry = (await store.ReadIndexAsync(Hyperion)).Find(Sep);
        Assert.That((import.Tag, import.FirstNewPart, import.NewParts, import.NewRows, import.NewSold), Is.EqualTo((Hyperion, 1, 2, 3, 0)));
        Assert.That(import.NewPartSha256, Is.EqualTo(entry.PartSha256.Skip(1)));
        Assert.That(manifest.CompletedAt, Is.Not.Null);
        Assert.That(markers.Values[ScyllaArchiveMarkerKeys.TagMonth(Hyperion, Sep)], Does.StartWith("archived:3:2:"));
        Assert.That(sweep.Row(Hyperion, Sep).Parts, Is.EqualTo((byte)1), "MariaDB is never written");
    }

    [Test]
    public async Task PartsContinueAfterPhaseOneParts_AndSourceIsMergedWhenPhaseOneExists_ScyllaOtherwise()
    {
        var phaseOne = new[] { Scylla(1, InSep(1)), Scylla(2, InSep(2)), Scylla(3, InSep(3)) };
        SeedPhaseOne(Hyperion, Sep, phaseOne.Select(Maria).ToList(), perPart: 2);
        var phaseOneHashes = (await store.ReadIndexAsync(Hyperion)).Find(Sep).PartSha256.ToList();
        Imported(Aug);
        AddScylla(phaseOne);
        AddScylla(Scylla(4, InSep(15), bid: 70, bids: new[] { (44L, 70L) }), Scylla(5, InSep(16)));
        AddScylla(Scylla(6, InSep(7), tag: Terminator), Scylla(7, InSep(8), tag: Terminator));

        await sweep.Run();

        var merged = (await store.ReadIndexAsync(Hyperion)).Find(Sep);
        Assert.That((merged.Parts, merged.Rows, merged.SoldRows, merged.Source), Is.EqualTo((3, 5, 1, ArchivedAuctionBlock.SourceMerged)));
        Assert.That(merged.PartSha256.Take(2), Is.EqualTo(phaseOneHashes));
        var part2 = await store.ReadBlockAsync(Hyperion, Sep, 2);
        Assert.That(part2.Source, Is.EqualTo(ArchivedAuctionBlock.SourceScylla));
        Assert.That(part2.AuctionUid, Is.EqualTo(new long[] { 4, 5 }));
        Assert.That(part2.ToExtras()[0].ItemUid, Is.EqualTo(4), "Scylla-only columns are kept");
        Assert.That(merged.PartSha256[2], Is.EqualTo(part2.Sha256()));

        var terminator = (await store.ReadIndexAsync(Terminator)).Find(Sep);
        Assert.That((terminator.Parts, terminator.Rows, terminator.Source), Is.EqualTo((1, 2, ArchivedAuctionBlock.SourceScylla)));
        Assert.That(PartRows(Terminator, Sep, 0).Select(r => r.UId), Is.EqualTo(new long[] { 6, 7 }));

        var manifest = Manifest(Sep);
        Assert.That(manifest.Tags.Select(t => (t.Tag, t.FirstNewPart, t.NewParts, t.NewRows, t.NewSold)),
            Is.EqualTo(new[] { (Hyperion, 2, 1, 2, 1), (Terminator, 0, 1, 2, 0) }));
    }

    [Test]
    public async Task OverlapRowsAreNeverRewritten_AndFingerprintMismatchIsCountedAndLogged()
    {
        sweep = Sweep(("ARCHIVE:CROSSCHECK_LOG_LIMIT", "1"));
        var same = Scylla(1, InSep(1), bid: 100);
        var phaseOneBid = Scylla(2, InSep(2), bid: 100);
        var phaseOneLength = Scylla(4, InSep(4));
        SeedPhaseOne(Hyperion, Sep, new[] { same, phaseOneBid, phaseOneLength }.Select(Maria).ToList());
        var phaseOneBytes = store.Objects[AuctionBlobKeys.Block(Hyperion, Sep, 0)].ToArray();
        var scyllaBid = Scylla(2, InSep(2), bid: 999);
        var scyllaLength = Scylla(4, InSep(4));
        scyllaLength.ItemBytes = new byte[] { 1 };
        AddScylla(same, scyllaBid, scyllaLength, Scylla(3, InSep(3)));

        var outcome = await sweep.ProcessTagMonthAsync(Hyperion, Sep, sweep.Row(Hyperion, Sep), new short[] { 87 }, Options(("ARCHIVE:CROSSCHECK_LOG_LIMIT", "1")), CancellationToken.None);

        Assert.That(outcome.Result, Is.EqualTo(TagMonthResult.Archived));
        Assert.That((outcome.OverlapRows, outcome.Mismatches, outcome.ScyllaOnlyRows), Is.EqualTo((3, 2, 1)));
        Assert.That(store.Objects[AuctionBlobKeys.Block(Hyperion, Sep, 0)], Is.EqualTo(phaseOneBytes), "phase-1 part is byte-identical");
        Assert.That(events.Where(e => e.StartsWith("write:")), Is.EqualTo(new[] { $"write:{Hyperion}:{Sep}:1" }));
        Assert.That(PartRows(Hyperion, Sep, 1).Select(r => r.UId), Is.EqualTo(new long[] { 3 }), "only the Scylla-only row is appended");
        Assert.That(PartRows(Hyperion, Sep, 0).Single(r => r.UId == 2).HighestBidAmount, Is.EqualTo(100), "phase-1 value wins");
        var warnings = log.Entries.Where(e => e.message.Contains("cross-check")).ToList();
        Assert.That(warnings, Has.Count.EqualTo(1), "CROSSCHECK_LOG_LIMIT");
        Assert.That(warnings[0].message, Does.Contain("uid 2").And.Contain("highest_bid_amount"));
    }

    [Test]
    public async Task CorruptReadBack_LeavesNoTagIndexChange_NoManifest_NoMarker()
    {
        var phaseOne = Scylla(1, InSep(1));
        SeedPhaseOne(Hyperion, Sep, new[] { Maria(phaseOne) });
        Imported(Aug);
        var indexBefore = store.Objects[AuctionBlobKeys.Index(Hyperion)].ToArray();
        AddScylla(phaseOne, Scylla(2, InSep(2)));
        store.CorruptReadBack = true;
        store.CorruptFromPart = 1; // the new part, not the phase-1 part 0

        await sweep.Run();

        Assert.That(events.Any(e => e.StartsWith("tag_index:") || e.StartsWith("meta:auctions") || e.StartsWith("marker:")), Is.False);
        Assert.That(store.Objects[AuctionBlobKeys.Index(Hyperion)], Is.EqualTo(indexBefore));
        Assert.That(Manifest(Sep), Is.Null);
        Assert.That(log.Entries.Any(e => e.message.Contains("read-back of part 1 differs in ItemBytes")), Is.True);
    }

    [Test]
    public async Task PageFailure_RetriesFromTheSamePagingState_ThenFailsTheTagMonthWithoutMarkers()
    {
        Sentinel(Sep);
        Imported(Aug);
        AddScylla(Scylla(1, InSep(1)), Scylla(2, InSep(2)), Scylla(3, InSep(3)), Scylla(4, InSep(4)), Scylla(5, InSep(5)));
        var key = ScyllaService.GetWeekOrDaysSinceStart(Hyperion, InSep(1));
        sweep.PageFailures[(Hyperion, key, 1)] = int.MaxValue;

        await sweep.Run();

        var secondPage = sweep.Fetches.Where(f => f.key == key && f.state == 1).ToList();
        Assert.That(secondPage, Has.Count.EqualTo(4), "first attempt + PAGE_RETRIES (3)");
        Assert.That(sweep.Fetches.Count(f => f.key == key && f.state == null), Is.EqualTo(1), "never restarted from the first page");
        Assert.That(sweep.Delays.SkipWhile(d => d != TimeSpan.FromSeconds(2)).Take(3), Is.EqualTo(new[] { TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(4), TimeSpan.FromSeconds(8) }));
        Assert.That(AnyWriteOrMarker(), Is.False);
        Assert.That(Manifest(Sep), Is.Null);
        var failure = log.Entries.Single(e => e.level == LogLevel.Error && e.message.Contains("Scylla archive of HYPERION 2020-09 failed"));
        Assert.That(failure.message, Does.Contain("InvalidOperationException: Page of HYPERION timekey"), "outer exception in the message itself");
        Assert.That(failure.message, Does.Contain("<- TimeoutException: read timeout HYPERION"), "inner exception follows");
    }

    [Test]
    public void DescribeException_ListsTypeMessageAndInnerChain()
    {
        var ex = new InvalidOperationException("outer\nline", new TimeoutException("middle", new ArgumentException("inner", new Exception("too deep"))));
        Assert.That(ScyllaArchiveSweep.DescribeException(ex), Is.EqualTo("InvalidOperationException: outer line <- TimeoutException: middle <- ArgumentException: inner"));
        Assert.That(ScyllaArchiveSweep.DescribeException(new TimeoutException("only")), Is.EqualTo("TimeoutException: only"));
    }

    [Test]
    public async Task PageFailureOnce_ResumesWithoutDuplicatesOrGaps()
    {
        Sentinel(Sep);
        Imported(Aug);
        AddScylla(Scylla(1, InSep(1)), Scylla(2, InSep(2)), Scylla(3, InSep(3)), Scylla(4, InSep(4)), Scylla(5, InSep(5)));
        var key = ScyllaService.GetWeekOrDaysSinceStart(Hyperion, InSep(1));
        sweep.PageFailures[(Hyperion, key, 1)] = 1;

        await sweep.Run();

        Assert.That(sweep.Fetches.Where(f => f.key == key).Select(f => f.state), Is.EqualTo(new int?[] { null, 1, 1, 2 }));
        Assert.That(PartRows(Hyperion, Sep, 0).Select(r => r.UId), Is.EqualTo(new long[] { 1, 2, 3, 4, 5 }));
        Assert.That(markers.Values.ContainsKey(ScyllaArchiveMarkerKeys.Month(Sep)), Is.True);
    }

    [Test]
    public async Task DuplicateAuctionAcrossPartitions_KeepsTheSoldCopy()
    {
        Sentinel(Sep);
        Imported(Aug);
        // the unsold snapshot (original end, a bid) sits in a later week than the sold row (bought earlier)
        AddScylla(Scylla(1, InSep(20), bid: 500, sold: false), Scylla(1, InSep(9), bid: 400, sold: true), Scylla(2, InSep(10)));

        await sweep.Run();

        var rows = PartRows(Hyperion, Sep, 0);
        Assert.That(rows.Select(r => (r.UId, r.End, r.HighestBidAmount)), Is.EqualTo(new[] { (1L, InSep(9), 400L), (2L, InSep(10), 0L) }));
        Assert.That(Manifest(Sep).Find(Hyperion).NewRows, Is.EqualTo(2));
    }

    [Test]
    public async Task RowsOutsideTheMonthOrWithBogusEnd_AreIgnoredAndCounted()
    {
        // week 82 runs 2020-07-28 .. 2020-08-04: a July row lives in an August partition
        var july = Scylla(1, new DateTime(2020, 7, 30, 0, 0, 0, DateTimeKind.Utc));
        var august = Scylla(2, new DateTime(2020, 8, 2, 0, 0, 0, DateTimeKind.Utc));
        var bogus = Scylla(3, new DateTime(1999, 12, 31, 0, 0, 0, DateTimeKind.Utc));
        bogus.TimeKey = 82; // garbage row filed under an in-scope key
        AddScylla(july, august, bogus);

        var outcome = await sweep.ProcessTagMonthAsync(Hyperion, Aug, null, new short[] { 82 }, Options(), CancellationToken.None);

        Assert.That((outcome.ScyllaRowsRead, outcome.ScyllaRowsInMonth, outcome.OutsideMonthRows, outcome.BogusEndRows, outcome.ScyllaOnlyRows), Is.EqualTo((3, 1, 1, 1, 1)));
        Assert.That(PartRows(Hyperion, Aug, 0).Select(r => r.UId), Is.EqualTo(new long[] { 2 }));
    }

    [Test]
    public async Task LocationLookupsAreChunkedBelowTheDiveLimit()
    {
        Sentinel(Sep);
        Imported(Aug);
        rowsPerPage = 100;
        AddScylla(Enumerable.Range(0, 250).Select(i => Scylla(100 + i, InSep(1 + i % 28))).ToArray());

        await sweep.Run();

        Assert.That(sweep.LocationChunks, Has.All.LessThanOrEqualTo(100));
        Assert.That(sweep.LocationChunks.Sum(), Is.EqualTo(250));
        Assert.That(Manifest(Sep).Find(Hyperion).NewRows, Is.EqualTo(250));
    }

    [Test]
    public async Task MonthIsCompleted_OnlyWhenEveryTagIsDoneOrEmpty()
    {
        Imported(Aug);
        Sentinel(Sep);
        AddScylla(Scylla(1, InSep(2)));
        AddScylla(Scylla(2, InSep(2), tag: Terminator));
        // a tag whose only partition overlapping September holds an August row: empty for September
        var emptyTag = Scylla(3, new DateTime(2020, 9, 1, 0, 0, 0, DateTimeKind.Utc).AddDays(-1), tag: "ASPECT_OF_THE_END");
        emptyTag.TimeKey = 87;
        AddScylla(emptyTag);
        var terminatorKey = ScyllaService.GetWeekOrDaysSinceStart(Terminator, InSep(2));
        sweep.PageFailures[(Terminator, terminatorKey, 0)] = 4;

        Assert.That(await sweep.Run(), Is.Zero);
        Assert.That(markers.Values.ContainsKey(ScyllaArchiveMarkerKeys.TagMonth(Hyperion, Sep)), Is.True);
        Assert.That(markers.Values[ScyllaArchiveMarkerKeys.TagMonth("ASPECT_OF_THE_END", Sep)], Does.StartWith("empty:"));
        Assert.That(markers.Values.ContainsKey(ScyllaArchiveMarkerKeys.TagMonth(Terminator, Sep)), Is.False);
        Assert.That(markers.Values.ContainsKey(ScyllaArchiveMarkerKeys.Month(Sep)), Is.False, "TERMINATOR failed");
        Assert.That(Manifest(Sep).CompletedAt, Is.Null);

        events.Clear();
        Assert.That(await sweep.Run(), Is.EqualTo(1));
        Assert.That(events.Any(e => e.StartsWith($"write:{Hyperion}")), Is.False, "done tags are not swept again");
        Assert.That(Manifest(Sep).Tags.Select(t => t.Tag), Is.EqualTo(new[] { Hyperion, Terminator }), "empty tag-months are not handed over");
        Assert.That(Manifest(Sep).CompletedAt, Is.Not.Null);
        Assert.That(markers.Values.ContainsKey(ScyllaArchiveMarkerKeys.Month(Sep)), Is.True);
    }

    [Test]
    public async Task MissingPhaseOnePart_FailsTheTagMonthWithoutWrites()
    {
        Imported(Aug);
        var phaseOne = new[] { Scylla(1, InSep(1)), Scylla(2, InSep(2)) };
        SeedPhaseOne(Hyperion, Sep, phaseOne.Select(Maria).ToList(), perPart: 1);
        store.Objects.Remove(AuctionBlobKeys.Block(Hyperion, Sep, 1));
        AddScylla(phaseOne);
        AddScylla(Scylla(3, InSep(3)));

        await sweep.Run();

        Assert.That(AnyWriteOrMarker(), Is.False);
        Assert.That(log.Entries.Any(e => e.level == LogLevel.Error && e.message.Contains("phase-1 part 1 is missing")), Is.True);
    }

    [Test]
    public async Task PartCountMismatchBetweenTagIndexAndArchivedMonths_FailsWithoutWrites()
    {
        Imported(Aug);
        SeedPhaseOne(Hyperion, Sep, new[] { Maria(Scylla(1, InSep(1))) });
        var index = await store.ReadIndexAsync(Hyperion);
        index.Find(Sep).Parts = 2;
        index.Find(Sep).PartSha256.Add("ff");
        await store.WriteIndexAsync(index);
        events.Clear();
        AddScylla(Scylla(3, InSep(3)));

        await sweep.Run();

        Assert.That(AnyWriteOrMarker(), Is.False);
        Assert.That(log.Entries.Any(e => e.message.Contains("tag index has 2 mariadb parts, archived_months 1")), Is.True);
    }

    [Test]
    public async Task MaxPartIsRespected()
    {
        sweep = Sweep(("ARCHIVE:MAX_ROWS_PER_PART", "1"), ("ARCHIVE:MAX_PART", "2"));
        Imported(Aug);
        SeedPhaseOne(Hyperion, Sep, new[] { Maria(Scylla(1, InSep(1))) });
        AddScylla(Scylla(2, InSep(2)), Scylla(3, InSep(3)), Scylla(4, InSep(4)));

        await sweep.Run();

        // parts 1 and 2 fit, part 3 is above MAX_PART: nothing is marked
        Assert.That(events.Where(e => e.StartsWith("write:")), Is.EqualTo(new[] { $"write:{Hyperion}:{Sep}:1", $"write:{Hyperion}:{Sep}:2" }));
        Assert.That(events.Any(e => e.StartsWith("tag_index:") || e.StartsWith("meta:auctions") || e.StartsWith("marker:")), Is.False);
        Assert.That(log.Entries.Any(e => e.message.Contains("needs part 3, above MAX_PART 2")), Is.True);
    }

    [Test]
    public async Task CrashBeforeTheMarker_IsRedone_PartsRewrittenAndManifestEntryReplaced()
    {
        Imported(Aug);
        SeedPhaseOne(Hyperion, Sep, new[] { Maria(Scylla(1, InSep(1))) });
        AddScylla(Scylla(1, InSep(1)), Scylla(2, InSep(2)), Scylla(3, InSep(3)));
        markers.FailNextSet = true; // tag index and manifest written, marker not

        Assert.That(await sweep.Run(), Is.Zero);
        Assert.That((await store.ReadIndexAsync(Hyperion)).Find(Sep).Parts, Is.EqualTo(2), "tag index already holds the new part");
        Assert.That(Manifest(Sep).Tags, Has.Count.EqualTo(1));
        Assert.That(markers.Values.ContainsKey(ScyllaArchiveMarkerKeys.TagMonth(Hyperion, Sep)), Is.False);

        events.Clear();
        Assert.That(await sweep.Run(), Is.EqualTo(1));
        Assert.That(events.Where(e => e.StartsWith("write:")), Is.EqualTo(new[] { $"write:{Hyperion}:{Sep}:1" }), "same part number rewritten, nothing appended");
        var entry = (await store.ReadIndexAsync(Hyperion)).Find(Sep);
        Assert.That((entry.Parts, entry.Rows, entry.Source), Is.EqualTo((2, 3, ArchivedAuctionBlock.SourceMerged)));
        var import = Manifest(Sep).Tags.Single();
        Assert.That((import.FirstNewPart, import.NewParts, import.NewRows), Is.EqualTo((1, 1, 2)), "entry replaced, not duplicated");
        Assert.That(await store.BlockExistsAsync(Hyperion, Sep, 2), Is.False);
    }

    [Test]
    public async Task NothingNew_OnlySetsTheOverlapOnlyMarker()
    {
        Imported(Aug);
        var rows = new[] { Scylla(1, InSep(1)), Scylla(2, InSep(2)) };
        SeedPhaseOne(Hyperion, Sep, rows.Select(Maria).ToList());
        var indexBefore = store.Objects[AuctionBlobKeys.Index(Hyperion)].ToArray();
        AddScylla(rows);

        await sweep.Run();

        Assert.That(markers.Values[ScyllaArchiveMarkerKeys.TagMonth(Hyperion, Sep)], Does.StartWith("overlap_only:"));
        Assert.That(store.Objects[AuctionBlobKeys.Index(Hyperion)], Is.EqualTo(indexBefore));
        Assert.That(events.Any(e => e.StartsWith("write:") || e.StartsWith("tag_index:")), Is.False);
        Assert.That(Manifest(Sep).Tags, Is.Empty, "nothing to import");
        Assert.That(Manifest(Sep).CompletedAt, Is.Not.Null);
    }

    [Test]
    public async Task UidLocatedInAnotherMonth_IsSupersededNotWrittenAndCounted()
    {
        // the stale unsold snapshot of an auction that was bought earlier and archived in August appears in September
        Imported(Aug);
        Sentinel(Sep);
        sweep.Tags[Hyperion] = 1;
        sweep.Locations[1] = (1, ArchiveMonths.ToIndex(Aug), 0);
        sweep.Locations[3] = (7, ArchiveMonths.ToIndex(Sep), 0); // another tag, same month
        AddScylla(Scylla(1, InSep(1)), Scylla(2, InSep(2)), Scylla(3, InSep(3)));

        var outcome = await sweep.ProcessTagMonthAsync(Hyperion, Sep, null, new short[] { 87 }, Options(), CancellationToken.None);

        Assert.That((outcome.SupersededRows, outcome.ScyllaOnlyRows, outcome.OverlapRows), Is.EqualTo((2, 1, 0)));
        Assert.That(PartRows(Hyperion, Sep, 0).Select(r => r.UId), Is.EqualTo(new long[] { 2 }));
        Assert.That(Manifest(Sep).Find(Hyperion).NewRows, Is.EqualTo(1));
        Assert.That(markers.Values[ScyllaArchiveMarkerKeys.TagMonth(Hyperion, Sep)], Does.StartWith("archived:1:1:"));
    }

    [Test]
    public async Task UidLocatedInTheSameTagMonth_IsOverlapForTheCrossCheck_AndNotWritten()
    {
        Imported(Aug);
        var phaseOne = Scylla(1, InSep(1), bid: 100);
        SeedPhaseOne(Hyperion, Sep, new[] { Maria(phaseOne) }); // also seeds its location (same tag-month)
        AddScylla(Scylla(1, InSep(1), bid: 120), Scylla(2, InSep(2)));

        var outcome = await sweep.ProcessTagMonthAsync(Hyperion, Sep, sweep.Row(Hyperion, Sep), new short[] { 87 }, Options(), CancellationToken.None);

        Assert.That((outcome.OverlapRows, outcome.Mismatches, outcome.SupersededRows, outcome.ScyllaOnlyRows), Is.EqualTo((1, 1, 0, 1)));
        Assert.That(PartRows(Hyperion, Sep, 1).Select(r => r.UId), Is.EqualTo(new long[] { 2 }));
        Assert.That(PartRows(Hyperion, Sep, 0).Single().HighestBidAmount, Is.EqualTo(100));
    }

    [Test]
    public async Task UidLocatedInTheSameTagMonthFromAPartlyImportedRun_IsWrittenAgain()
    {
        // SkyIndexer wrote the locations of the sweep's part 1 but crashed before archived_months: the rows must stay in the rewritten part
        Imported(Aug);
        SeedPhaseOne(Hyperion, Sep, new[] { Maria(Scylla(1, InSep(1))) });
        sweep.Locations[2] = (sweep.Tags[Hyperion], ArchiveMonths.ToIndex(Sep), 1);
        AddScylla(Scylla(1, InSep(1)), Scylla(2, InSep(2)));

        var outcome = await sweep.ProcessTagMonthAsync(Hyperion, Sep, sweep.Row(Hyperion, Sep), new short[] { 87 }, Options(), CancellationToken.None);

        Assert.That((outcome.SupersededRows, outcome.ScyllaOnlyRows), Is.EqualTo((0, 1)));
        Assert.That(PartRows(Hyperion, Sep, 1).Select(r => r.UId), Is.EqualTo(new long[] { 2 }));
    }

    /// <summary>SkyIndexer (another month of the same tag) updates the tag index between the sweep's read and write.</summary>
    private Func<ArchivedTagIndex, Task> ConcurrentWriter(string tag, int month, int times = 1)
    {
        return async _ =>
        {
            var other = await store.ReadIndexAsync(tag) ?? new ArchivedTagIndex { Tag = tag };
            other.Upsert(new ArchivedMonthEntry { Month = month, Parts = 1, Rows = 99, PartSha256 = new() { "aa" }, Source = ArchivedAuctionBlock.SourceMariaDb });
            await store.WriteIndexAsync(other);
            if (--times > 0)
                store.BeforeIndexWrite = ConcurrentWriter(tag, month, times);
        };
    }

    [Test]
    public async Task ConcurrentTagIndexWriter_BothMonthEntriesSurvive()
    {
        Imported(Aug);
        SeedPhaseOne(Hyperion, Sep, new[] { Maria(Scylla(1, InSep(1))) });
        AddScylla(Scylla(1, InSep(1)), Scylla(2, InSep(2)));
        store.BeforeIndexWrite = ConcurrentWriter(Hyperion, 202305);

        await sweep.Run();

        Assert.That(store.IndexConflicts, Is.EqualTo(1));
        var index = await store.ReadIndexAsync(Hyperion);
        Assert.That(index.Months.Select(m => m.Month), Is.EqualTo(new[] { Sep, 202305 }), "SkyIndexer's month is not lost");
        Assert.That((index.Find(Sep).Parts, index.Find(Sep).Source), Is.EqualTo((2, ArchivedAuctionBlock.SourceMerged)));
        Assert.That(index.Find(202305).Rows, Is.EqualTo(99));
        Assert.That(markers.Values.ContainsKey(ScyllaArchiveMarkerKeys.TagMonth(Hyperion, Sep)), Is.True);
    }

    [Test]
    public async Task TagIndexThatKeepsChanging_FailsTheTagMonthWithoutManifestOrMarker()
    {
        Imported(Aug);
        SeedPhaseOne(Hyperion, Sep, new[] { Maria(Scylla(1, InSep(1))) });
        AddScylla(Scylla(1, InSep(1)), Scylla(2, InSep(2)));
        store.BeforeIndexWrite = ConcurrentWriter(Hyperion, 202305, times: ArchiveIndexConflictException.MaxWriteAttempts);

        await sweep.Run();

        Assert.That(store.IndexConflicts, Is.EqualTo(ArchiveIndexConflictException.MaxWriteAttempts));
        Assert.That(Manifest(Sep), Is.Null);
        Assert.That(markers.Values.ContainsKey(ScyllaArchiveMarkerKeys.TagMonth(Hyperion, Sep)), Is.False);
        Assert.That((await store.ReadIndexAsync(Hyperion)).Find(Sep).Parts, Is.EqualTo(1), "phase-1 entry unchanged");
        Assert.That(log.Entries.Any(e => e.message.Contains("tag index kept changing (5 conditional writes rejected)")), Is.True);
    }

    [Test]
    public async Task ManifestWritesAreConditional_AConcurrentChangeIsReReadAndKept()
    {
        Imported(Aug);
        Sentinel(Sep);
        AddScylla(Scylla(1, InSep(1)));
        store.BeforeMetaWrite = async key =>
        {
            if (key != SepManifest)
                return;
            var (other, etag) = await store.ReadMetaVersionedAsync<ScyllaMonthManifest>(key);
            other ??= new ScyllaMonthManifest { Month = Sep };
            other.Upsert(new ScyllaTagImport { Tag = "ZZZ_OTHER", NewParts = 1 });
            await store.WriteMetaConditionalAsync(key, other, etag);
        };

        await sweep.Run();

        Assert.That(store.IndexConflicts, Is.EqualTo(1));
        Assert.That(Manifest(Sep).Tags.Select(t => t.Tag), Is.EqualTo(new[] { Hyperion, "ZZZ_OTHER" }));
    }

    /// <summary>Removes the month entry from the tag index, as a lost update before conditional writes did.</summary>
    private async Task DropIndexEntry(string tag, int month)
    {
        var index = await store.ReadIndexAsync(tag);
        index.Months.RemoveAll(m => m.Month == month);
        await store.WriteIndexAsync(index);
        events.Clear();
    }

    [Test]
    public async Task MissingTagIndexEntry_IsRebuiltFromThePhaseOneParts()
    {
        Imported(Aug);
        var phaseOne = new[] { Scylla(1, InSep(1)), Scylla(2, InSep(2)) };
        SeedPhaseOne(Hyperion, Sep, phaseOne.Select(Maria).ToList(), perPart: 1);
        var hashes = new[] { (await store.ReadBlockAsync(Hyperion, Sep, 0)).Sha256(), (await store.ReadBlockAsync(Hyperion, Sep, 1)).Sha256() };
        await DropIndexEntry(Hyperion, Sep);
        AddScylla(phaseOne);
        AddScylla(Scylla(3, InSep(3)));

        await sweep.Run();

        var entry = (await store.ReadIndexAsync(Hyperion)).Find(Sep);
        Assert.That((entry.Parts, entry.Rows, entry.Source), Is.EqualTo((3, 3, ArchivedAuctionBlock.SourceMerged)));
        Assert.That(entry.PartSha256.Take(2), Is.EqualTo(hashes), "recomputed phase-1 hashes");
        Assert.That(entry.PartSha256[2], Is.EqualTo((await store.ReadBlockAsync(Hyperion, Sep, 2)).Sha256()));
        Assert.That(Manifest(Sep).Find(Hyperion).FirstNewPart, Is.EqualTo(2));
        Assert.That(log.Entries.Any(e => e.level == LogLevel.Warning && e.message.Contains("had no entry for 2020-09")), Is.True);
    }

    [Test]
    public async Task MissingTagIndexEntry_WithNothingNew_RecreatesThePhaseOneEntry()
    {
        Imported(Aug);
        var phaseOne = new[] { Scylla(1, InSep(1)) };
        SeedPhaseOne(Hyperion, Sep, phaseOne.Select(Maria).ToList());
        await DropIndexEntry(Hyperion, Sep);
        AddScylla(phaseOne);

        await sweep.Run();

        var entry = (await store.ReadIndexAsync(Hyperion)).Find(Sep);
        Assert.That((entry.Parts, entry.Rows, entry.Source), Is.EqualTo((1, 1, ArchivedAuctionBlock.SourceMariaDb)));
        Assert.That(ArchivedAuctionBlock.CombinedSha256(entry.PartSha256), Is.EqualTo(sweep.Row(Hyperion, Sep).Sha256));
        Assert.That(markers.Values[ScyllaArchiveMarkerKeys.TagMonth(Hyperion, Sep)], Does.StartWith("overlap_only:"));
    }

    [Test]
    public async Task MissingTagIndexEntry_WithHashMismatch_FailsWithoutWrites()
    {
        Imported(Aug);
        var phaseOne = new[] { Scylla(1, InSep(1)) };
        SeedPhaseOne(Hyperion, Sep, phaseOne.Select(Maria).ToList());
        await DropIndexEntry(Hyperion, Sep);
        sweep.Row(Hyperion, Sep).Sha256 = new string('e', 64);
        AddScylla(phaseOne);
        AddScylla(Scylla(2, InSep(2)));

        await sweep.Run();

        Assert.That(AnyWriteOrMarker(), Is.False);
        Assert.That((await store.ReadIndexAsync(Hyperion)).Find(Sep), Is.Null);
        Assert.That(log.Entries.Any(e => e.level == LogLevel.Error && e.message.Contains("recomputed phase-1 part hashes do not match")), Is.True);
    }

    [Test]
    public async Task CircuitBreakerPausesAfterConsecutiveFailures()
    {
        sweep = Sweep(("ARCHIVE:CIRCUIT_BREAKER_FAILURES", "2"), ("ARCHIVE:CIRCUIT_BREAKER_PAUSE_MINUTES", "10"));
        Imported(Aug);
        Sentinel(Sep);
        foreach (var tag in new[] { "A", "B", "C" })
        {
            AddScylla(Scylla(1, InSep(2), tag: tag));
            sweep.PageFailures[(tag, ScyllaService.GetWeekOrDaysSinceStart(tag, InSep(2)), 0)] = int.MaxValue;
        }

        await sweep.Run();

        Assert.That(sweep.Delays.Count(d => d == TimeSpan.FromMinutes(10)), Is.EqualTo(1), "paused once after A and B, C alone does not trip it");
        Assert.That(log.Entries.Any(e => e.level == LogLevel.Warning && e.message.Contains("2 tag-months failed in a row")), Is.True);
    }

    /// <summary>In-scope (August/September 2020) and out-of-scope partitions of several tags.</summary>
    private List<(string Tag, short TimeKey)> ManyPartitions()
    {
        var all = new List<(string, short)>();
        foreach (var tag in new[] { Hyperion, Terminator, "ASPECT_OF_THE_END", "JUJU_SHORTBOW", "WITHER_SKULL" })
            for (short key = 78; key <= 95; key++) // weeks 78-81 and 92-95 are outside 2020-08..2020-09
                all.Add((tag, key));
        for (short key = 1150; key <= 1300; key += 7)
            all.Add(("ENCHANTED_BOOK", key));
        all.Add(("ENCHANTED_BOOK", 5)); // random-timekey garbage
        sweep.PartitionList.AddRange(all);
        return all.Where(p => ScyllaArchiveRows.OverlapsMonths(p.Item1, p.Item2, Aug, Sep)).ToList();
    }

    [Test]
    public async Task TokenRangeScan_CollectsEveryInScopePartitionExactlyOnce()
    {
        var expected = ManyPartitions();
        var ranges = ScyllaTokenRange.Split(8);
        Assert.That(sweep.PartitionList.All(p => ranges.Count(r => r.Contains(TestableScyllaArchiveSweep.FakeToken(p))) == 1), Is.True, "the ranges partition the ring");

        var found = await sweep.ScanPartitionsAsync(Options(), CancellationToken.None);

        Assert.That(found, Is.EquivalentTo(expected));
        Assert.That(found.Distinct().Count(), Is.EqualTo(found.Count));
        Assert.That(sweep.PartitionFetches.Where(f => f.state == null).Select(f => f.range), Is.EqualTo(ranges), "each range once, in order");
        Assert.That(sweep.PartitionFetches.Any(f => f.state != null), Is.True, "pages beyond the first are fetched with their paging state");
        Assert.That(markers.Values[ScyllaArchiveMarkerKeys.PartitionScanNext], Is.EqualTo("8/8/202008/202009"));
        Assert.That(markers.Sets[ScyllaArchiveMarkerKeys.PartitionScanFound], Has.Count.EqualTo(expected.Count), "only in-scope entries are kept");
        Assert.That(sweep.Delays.Count(d => d == TimeSpan.FromMilliseconds(100)), Is.EqualTo(7), "PAGE_PAUSE_MS between ranges");
    }

    [Test]
    public async Task TimingOutRange_IsRetriedThenSplit()
    {
        var expected = ManyPartitions();
        var ranges = ScyllaTokenRange.Split(8);
        sweep.TimeoutRange = r => r == ranges[3];

        var found = await sweep.ScanPartitionsAsync(Options(), CancellationToken.None);

        Assert.That(found, Is.EquivalentTo(expected));
        Assert.That(sweep.PartitionFetches.Count(f => f.range == ranges[3]), Is.EqualTo(4), "first attempt + PAGE_RETRIES (3)");
        var (low, high) = ranges[3].Halve();
        Assert.That(sweep.PartitionFetches.Any(f => f.range == low) && sweep.PartitionFetches.Any(f => f.range == high), Is.True, "both halves scanned");
        Assert.That(sweep.Delays.SkipWhile(d => d != TimeSpan.FromSeconds(2)).Take(3), Is.EqualTo(new[] { TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(4), TimeSpan.FromSeconds(8) }));
    }

    [Test]
    public async Task RangeThatKeepsTimingOut_FailsThePass_AndARestartResumesThere()
    {
        var expected = ManyPartitions();
        var ranges = ScyllaTokenRange.Split(8);
        var poison = TestableScyllaArchiveSweep.FakeToken(expected[expected.Count / 2]);
        var poisonRange = Enumerable.Range(0, 8).Single(i => ranges[i].Contains(poison));
        sweep.TimeoutRange = r => r.Contains(poison); // every half containing it times out as well

        Assert.ThrowsAsync<InvalidOperationException>(() => sweep.GetPartitionsAsync(Now, Options(), CancellationToken.None));
        Assert.That(markers.Values[ScyllaArchiveMarkerKeys.PartitionScanNext], Is.EqualTo($"{poisonRange}/8/202008/202009"));
        Assert.That(store.Objects.ContainsKey(AuctionBlobKeys.Meta(ScyllaArchiveSweep.PartitionListKey)), Is.False, "no cache before the last range");
        var halvings = sweep.PartitionFetches.Where(f => f.range.Contains(poison)).Select(f => f.range).Distinct().Count();
        Assert.That(halvings, Is.EqualTo(ScyllaArchiveSweep.MaxRangeSplitDepth + 1));

        // restart (new process): only the remaining ranges are read
        var restarted = Sweep();
        restarted.PartitionList.AddRange(sweep.PartitionList);
        var list = await restarted.GetPartitionsAsync(Now, Options(), CancellationToken.None);

        Assert.That(list, Is.EquivalentTo(expected));
        Assert.That(restarted.PartitionFetches.Where(f => f.state == null).Select(f => f.range), Is.EqualTo(ranges.Skip(poisonRange)), "finished ranges are not read again");
        Assert.That(store.Objects.ContainsKey(AuctionBlobKeys.Meta(ScyllaArchiveSweep.PartitionListKey)), Is.True, "cache written after the last range");
        Assert.That(markers.Values.ContainsKey(ScyllaArchiveMarkerKeys.PartitionScanNext) || markers.Sets.ContainsKey(ScyllaArchiveMarkerKeys.PartitionScanFound), Is.False, "progress deleted once cached");
        Assert.That(log.Entries.Any(e => e.message.Contains($"resuming the partition scan at range {poisonRange}/8")), Is.True);
    }

    [Test]
    public async Task ChangedRangeCountOrMonths_RestartTheScan()
    {
        var expected = ManyPartitions();
        markers.Values[ScyllaArchiveMarkerKeys.PartitionScanNext] = "6/16/202008/202009";
        markers.Sets[ScyllaArchiveMarkerKeys.PartitionScanFound] = new HashSet<string> { "STALE:1" };

        var found = await sweep.ScanPartitionsAsync(Options(), CancellationToken.None);

        Assert.That(found, Is.EquivalentTo(expected), "stale progress of 16 ranges is dropped");
        Assert.That(sweep.PartitionFetches.First().range, Is.EqualTo(ScyllaTokenRange.Split(8)[0]));
        Assert.That(ScyllaArchiveMarkerKeys.ResumeRange("6/8/202008/202009", 8, Aug, Sep), Is.EqualTo(6));
        Assert.That(ScyllaArchiveMarkerKeys.ResumeRange("6/8/202007/202009", 8, Aug, Sep), Is.Zero);
        Assert.That(ScyllaArchiveMarkerKeys.ResumeRange(null, 8, Aug, Sep), Is.Zero);
    }

    [Test]
    public void TokenRangesCoverTheRingContiguously()
    {
        var ranges = ScyllaTokenRange.Split(4096);
        Assert.That(ranges[0], Is.EqualTo(new ScyllaTokenRange(long.MinValue, true, ranges[0].End)));
        Assert.That(ranges[^1].End, Is.EqualTo(long.MaxValue));
        for (var i = 1; i < ranges.Count; i++)
            Assert.That((ranges[i].Start, ranges[i].StartInclusive), Is.EqualTo((ranges[i - 1].End, false)));
        Assert.That(ranges[0].Contains(long.MinValue) && ranges[^1].Contains(long.MaxValue), Is.True);
        var (low, high) = ranges[5].Halve();
        Assert.That((low.Start, low.StartInclusive, low.End == high.Start, high.StartInclusive, high.End), Is.EqualTo((ranges[5].Start, false, true, false, ranges[5].End)));
        Assert.That(ScyllaArchiveMarkerKeys.TryParsePartition(ScyllaArchiveMarkerKeys.Partition("A:B", -3), out var parsed) && parsed == ("A:B", (short)-3), Is.True);
        Assert.That(ScyllaArchiveSweep.IsTransientScyllaError(new global::Cassandra.ReadTimeoutException(global::Cassandra.ConsistencyLevel.LocalOne, 0, 1, false)), Is.True);
        Assert.That(ScyllaArchiveSweep.IsTransientScyllaError(new InvalidOperationException()), Is.False);
    }

    [Test]
    public async Task PartitionListIsFilteredToTheMonthsAndCachedUntilStale()
    {
        sweep.PartitionList.AddRange(new (string, short)[] { (Hyperion, 80), (Hyperion, 82), (Hyperion, 91), (Hyperion, 92), ("ENCHANTED_BOOK", 5), ("ENCHANTED_BOOK", 1218) });
        var options = Options();

        var first = await sweep.GetPartitionsAsync(Now, options, CancellationToken.None);
        Assert.That(first, Is.EqualTo(new (string, short)[] { ("ENCHANTED_BOOK", 1218), (Hyperion, 82), (Hyperion, 91) }));
        await sweep.GetPartitionsAsync(Now.AddDays(29), options, CancellationToken.None);
        Assert.That(sweep.PartitionScans, Is.EqualTo(1));
        await sweep.GetPartitionsAsync(Now.AddDays(31), options, CancellationToken.None);
        Assert.That(sweep.PartitionScans, Is.EqualTo(2), "PARTITION_LIST_MAX_AGE_DAYS");
        await sweep.GetPartitionsAsync(Now.AddDays(31), options with { NewestMonth = 202010 }, CancellationToken.None);
        Assert.That(sweep.PartitionScans, Is.EqualTo(3), "other months rescan");
    }

    [Test]
    public async Task UnreachableDatabaseOrRedis_LogsTheConfigKeyAndRetriesEveryTenMinutes()
    {
        sweep.DatabaseFailures = 2;
        markers.FailingGets = 1;

        await sweep.WaitForPrerequisitesAsync(CancellationToken.None);

        Assert.That(sweep.DatabaseChecks, Is.EqualTo(4));
        Assert.That(sweep.Delays, Is.EqualTo(Enumerable.Repeat(TimeSpan.FromMinutes(10), 3)));
        var errors = log.Entries.Where(e => e.level == LogLevel.Error).Select(e => e.message).ToList();
        Assert.That(errors, Has.Count.EqualTo(3));
        Assert.That(errors[0], Does.StartWith("Auction archive: MariaDB (DBConnection) unavailable, retrying in 10 min"));
        Assert.That(errors[2], Does.StartWith("Auction archive: Redis (ARCHIVE:REDIS_HOST / SETTINGS_REDIS_HOST) unavailable, retrying in 10 min"));
    }
    private static ScyllaAuction Named(long uid, DateTime end, string name, string tag = "unknown")
    {
        var row = Scylla(uid, end, tag);
        row.ItemName = name;
        return row;
    }

    private const string Unknown = "unknown";
    private const string Midas = "MIDAS_SWORD";

    private void ArrangeRetag()
    {
        sweep.Resolver = new ItemNameTagResolver(new[] { ("Hyperion", Hyperion), ("Terminator", Terminator), ("Midas Sword", Midas), ("Clay", "CLAY"), ("Clay", "CLAY_BALL") });
        Imported(Aug);
        Sentinel(Sep);
    }

    [Test]
    public async Task UnknownRows_AreRetaggedByItemName_AndOnlyTheUnresolvableRemainderStaysUnderUnknown()
    {
        ArrangeRetag();
        AddScylla(Scylla(1, InSep(3)));
        AddScylla(
            Named(10, InSep(4), "§6Hyperion ✪✪✪"),     // resolves to HYPERION (has own partitions)
            Named(11, InSep(5), "Terminator"),         // TERMINATOR has no partitions of its own
            Named(12, InSep(6), "Clay"),               // ambiguous
            Named(13, InSep(7), "Localised Sword"));   // unmatched

        Assert.That(await sweep.Run(), Is.EqualTo(1));

        Assert.That(PartRows(Hyperion, Sep, 0).Select(r => (r.UId, r.Tag)), Is.EquivalentTo(new[] { (1L, Hyperion), (10L, Hyperion) }));
        Assert.That(PartRows(Terminator, Sep, 0).Select(r => (r.UId, r.Tag)), Is.EqualTo(new[] { (11L, Terminator) }));
        Assert.That(PartRows(Unknown, Sep, 0).Select(r => r.UId), Is.EquivalentTo(new[] { 12L, 13L }));
        foreach (var tag in new[] { Hyperion, Terminator, Unknown })
            Assert.That(markers.Values.ContainsKey(ScyllaArchiveMarkerKeys.TagMonth(tag, Sep)), Is.True, tag);
        Assert.That(Manifest(Sep).Tags.Select(t => t.Tag), Is.EquivalentTo(new[] { Hyperion, Terminator, Unknown }));
        Assert.That(Manifest(Sep).Find(Terminator).FirstNewPart, Is.Zero);
        Assert.That(sweep.ResolverLoads, Is.EqualTo(1));
        Assert.That(log.Entries.Any(e => e.message.Contains("2020-09 unknown rows: 2 retagged to 2 tags, 2 kept; top unresolved names: Clay (1), Localised Sword (1)")), Is.True);
    }

    [Test]
    public async Task UnknownTagMonthWithAMarker_IsNotPreScanned()
    {
        ArrangeRetag();
        AddScylla(Scylla(1, InSep(3)), Named(10, InSep(4), "Terminator"));
        markers.Values[ScyllaArchiveMarkerKeys.TagMonth(Unknown, Sep)] = "empty:2020-10-23T00:00:00Z";

        await sweep.Run();

        Assert.That(sweep.Fetches.Any(f => f.tag == Unknown), Is.False, "no unknown page read");
        Assert.That(sweep.ResolverLoads, Is.Zero);
        Assert.That(markers.Values.ContainsKey(ScyllaArchiveMarkerKeys.TagMonth(Terminator, Sep)), Is.False);
        Assert.That(PartRows(Hyperion, Sep, 0).Select(r => r.UId), Is.EqualTo(new[] { 1L }));
    }

    [Test]
    public async Task RetaggedRowLocatedInAnotherTagMonth_IsSupersededLikeAScyllaRow()
    {
        ArrangeRetag();
        AddScylla(Named(10, InSep(4), "Terminator"), Named(11, InSep(5), "Terminator"));
        sweep.Locations[10] = (77, ArchiveMonths.ToIndex(Aug), 0);

        await sweep.Run();

        Assert.That(PartRows(Terminator, Sep, 0).Select(r => r.UId), Is.EqualTo(new[] { 11L }));
        Assert.That(log.Entries.Any(e => e.message.Contains("1 superseded")), Is.True);
    }

    [Test]
    public async Task RetaggedRowWhoseTagIsAlreadyMarked_IsDropped_AndDuplicatesKeepTheBetterCopy()
    {
        ArrangeRetag();
        markers.Values[ScyllaArchiveMarkerKeys.TagMonth(Terminator, Sep)] = "empty:2020-10-23T00:00:00Z";
        AddScylla(Named(10, InSep(4), "Terminator"), Named(20, InSep(5), "Hyperion"), Named(20, InSep(5), "Hyperion"));
        sweep.Pages[(Unknown, ScyllaService.GetWeekOrDaysSinceStart(Unknown, InSep(5)))][0][1].HighestBidAmount = 5;

        await sweep.Run();

        Assert.That(PartRows(Terminator, Sep, 0), Is.Null, "already marked, not written again");
        Assert.That(PartRows(Hyperion, Sep, 0).Select(r => (r.UId, r.HighestBidAmount)), Is.EqualTo(new[] { (20L, 5L) }));
    }

    private static DateTime InAug(int day) => new(2020, 8, day, 12, 0, 0, DateTimeKind.Utc);

    /// <summary>August fully swept and imported (two tags), September not started; operator asked for a redo of August.</summary>
    private async Task SweptAndImportedAugustWithRedoRequested()
    {
        Sentinel(Aug);
        Sentinel(Sep);
        AddScylla(Scylla(1, InAug(3)), Scylla(2, InAug(4), tag: Terminator), Scylla(3, InSep(3)));
        Assert.That(await sweep.Run(), Is.EqualTo(1));
        Imported(Aug);
        Assert.That(Manifest(Aug).Tags.Count, Is.EqualTo(2));
        Assert.That(Manifest(Aug).CompletedAt, Is.Not.Null);
        markers.Values[ScyllaArchiveMarkerKeys.Redo(Aug)] = "1";
        events.Clear();
    }

    [Test]
    public async Task Redo_ResetsManifestAndMarkers_ThenWaitsForMariaDbReset()
    {
        await SweptAndImportedAugustWithRedoRequested();
        sweep.Fetches.Clear();

        Assert.That(await sweep.Run(), Is.Zero);

        Assert.That(markers.Values.ContainsKey(ScyllaArchiveMarkerKeys.TagMonth(Hyperion, Aug)), Is.False);
        Assert.That(markers.Values.ContainsKey(ScyllaArchiveMarkerKeys.TagMonth(Terminator, Aug)), Is.False);
        Assert.That(markers.Values.ContainsKey(ScyllaArchiveMarkerKeys.Month(Aug)), Is.False);
        Assert.That(Manifest(Aug).Tags, Is.Empty);
        Assert.That(Manifest(Aug).CompletedAt, Is.Null, "the importer must not see a completed stale manifest");
        Assert.That(markers.Values.ContainsKey(ScyllaArchiveMarkerKeys.Redo(Aug)), Is.True);
        Assert.That(sweep.Fetches, Is.Empty, "no tag swept");
        Assert.That(Manifest(Sep), Is.Null, "the pass ended at August");
        Assert.That(log.Entries.Any(e => e.message.Contains("redo of 2020-08 requested; manifest and markers reset (2 tag entries removed)")), Is.True);
        Assert.That(log.Entries.Count(e => e.level == LogLevel.Warning && e.message.Contains("redo of 2020-08 waits for the MariaDB reset") && e.message.Contains("DELETE FROM auction_locations WHERE month = ")), Is.EqualTo(1));
        Assert.That(sweep.PauseAfterPass(Options()), Is.EqualTo(TimeSpan.FromMinutes(Options().ImportWaitMinutes)));

        await sweep.Run();
        Assert.That(log.Entries.Count(e => e.message.Contains("waits for the MariaDB reset")), Is.EqualTo(1), "warned once");
    }

    [Test]
    public async Task Redo_WhenMariaDbClean_DeletesKeyAndResweepsMonth()
    {
        await SweptAndImportedAugustWithRedoRequested();
        sweep.RedoState = (false, 0, false);
        sweep.Sentinel(Aug).ScyllaAt = null;

        Assert.That(await sweep.Run(), Is.EqualTo(1));

        Assert.That(markers.Values.ContainsKey(ScyllaArchiveMarkerKeys.Redo(Aug)), Is.False);
        Assert.That(log.Entries.Any(e => e.message.Contains("redo of 2020-08 starts")), Is.True);
        Assert.That(events.Any(e => e.StartsWith($"write:{Hyperion}")), Is.True, "parts written again");
        Assert.That(Manifest(Aug).Tags.Select(t => t.Tag), Is.EqualTo(new[] { Hyperion, Terminator }));
        Assert.That(Manifest(Aug).CompletedAt, Is.Not.Null);
        Assert.That(markers.Values[ScyllaArchiveMarkerKeys.TagMonth(Hyperion, Aug)], Does.StartWith("archived:"));
        Assert.That(markers.Values.ContainsKey(ScyllaArchiveMarkerKeys.Month(Aug)), Is.True);
    }

    [Test]
    public async Task Redo_IsIdempotent_OnAlreadyResetManifest()
    {
        await SweptAndImportedAugustWithRedoRequested();
        await sweep.Run();
        events.Clear();

        Assert.That(async () => await sweep.Run(), Throws.Nothing);

        Assert.That(events.Any(e => e.StartsWith("meta:")), Is.False, "the reset manifest is not written again");
        Assert.That(Manifest(Aug).Tags, Is.Empty);
        Assert.That(Manifest(Aug).CompletedAt, Is.Null);
        Assert.That(log.Entries.Count(e => e.message.Contains("manifest and markers reset")), Is.EqualTo(1));
        Assert.That(markers.Values.ContainsKey(ScyllaArchiveMarkerKeys.Redo(Aug)), Is.True);
    }

    [Test]
    public async Task Redo_OutsideConfiguredRange_IsIgnoredAndKeyDeleted()
    {
        Sentinel(Aug);
        var outside = ScyllaArchiveMarkerKeys.Redo(201912);
        markers.Values[outside] = "1";

        await sweep.Run();

        Assert.That(markers.Values.ContainsKey(outside), Is.False);
        Assert.That(log.Entries.Any(e => e.level == LogLevel.Warning && e.message.Contains("redo of 2019-12 requested but it is outside")), Is.True);
        Assert.That(sweep.RedoStateReads, Is.Zero);
    }
}
