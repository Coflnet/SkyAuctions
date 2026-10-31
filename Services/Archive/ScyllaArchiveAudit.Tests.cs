using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using Coflnet.Sky.Core;
using Coflnet.Sky.Core.Archive;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NUnit.Framework;

namespace Coflnet.Sky.Auctions.Archive;

public class ScyllaArchiveAuditTests
{
    private const int Aug = 202008;
    private const int Sep = 202009;
    private const int Oct = 202010;
    private const string Hyperion = "HYPERION";
    private const string Terminator = "TERMINATOR";
    private const ushort HyperionId = 1;
    private const ushort TerminatorId = 2;
    private static readonly DateTime Now = new(2026, 10, 31, 12, 0, 0, DateTimeKind.Utc);

    private class TestableAudit : ScyllaArchiveAudit
    {
        public readonly Dictionary<(string tag, short key), List<List<ScyllaAuditRow>>> Pages = new();
        public readonly Dictionary<long, List<ScyllaAuditRow>> ByUid = new();
        public readonly List<(string tag, short key, int page)> Fetches = new();
        public readonly List<TimeSpan> Delays = new();
        public readonly HashSet<int> Imported = new() { Aug, Sep, Oct };
        public ConsistencyLevel LastConsistency;
        public int FailPartitionsAfter = int.MaxValue;
        public readonly HashSet<(string tag, short key)> Unreadable = new();
        public bool CrashOnBackoff;
        public IReadOnlyList<ScyllaColumnInfo> Schema = DefaultSchema();
        public readonly ListLogger<ScyllaArchiveAudit> Log;

        public TestableAudit(InMemoryAuctionBlobStore store, InMemoryMarkers markers, InMemoryLocationLookup locations, IConfiguration config)
            : this(store, markers, locations, config, new ListLogger<ScyllaArchiveAudit>())
        {
        }

        private TestableAudit(InMemoryAuctionBlobStore store, InMemoryMarkers markers, InMemoryLocationLookup locations, IConfiguration config, ListLogger<ScyllaArchiveAudit> log)
            : base(store, store, markers, locations, new ArchivedAuctionReader(store, locations, NullLogger.Instance), null, config, log)
        {
            Log = log;
        }

        public static List<ScyllaColumnInfo> DefaultSchema() => ScyllaAuditSchema.KnownColumns.Select(c => new ScyllaColumnInfo(c, c switch
        {
            "tag" or "timekey" => "partition_key",
            "issold" or "end" or "auctionuid" => "clustering",
            _ => "regular",
        }, c is "nbtlookup" or "enchantments" or "coop" ? "frozen<map<text, text>>" : "text")).ToList();

        protected override Task<IReadOnlyList<ScyllaColumnInfo>> ReadSchemaAsync(CancellationToken cancellationToken) => Task.FromResult(Schema);

        protected override Task<(IReadOnlyList<ScyllaAuditRow> Rows, byte[] Next)> FetchPageAsync(string tag, short timeKey, byte[] pagingState, int pageSize, ConsistencyLevel consistency, CancellationToken cancellationToken)
        {
            var index = pagingState == null ? 0 : BitConverter.ToInt32(pagingState);
            Fetches.Add((tag, timeKey, index));
            LastConsistency = consistency;
            if (Fetches.Count > FailPartitionsAfter || Unreadable.Contains((tag, timeKey)))
                throw new InvalidOperationException("crash");
            var pages = Pages.TryGetValue((tag, timeKey), out var p) ? p : new List<List<ScyllaAuditRow>>();
            IReadOnlyList<ScyllaAuditRow> rows = index < pages.Count ? pages[index] : new List<ScyllaAuditRow>();
            return Task.FromResult((rows, index + 1 < pages.Count ? BitConverter.GetBytes(index + 1) : null));
        }

        protected override Task<IReadOnlyList<ScyllaAuditRow>> FetchRowsByUidAsync(long uid, ConsistencyLevel consistency, CancellationToken cancellationToken)
            => Task.FromResult<IReadOnlyList<ScyllaAuditRow>>(ByUid.TryGetValue(uid, out var rows) ? rows : new List<ScyllaAuditRow>());

        protected override Task<bool> IsMonthImportedAsync(int month, CancellationToken cancellationToken) => Task.FromResult(Imported.Contains(month));

        protected override int NextInt(int max) => 0;

        protected override Task Delay(TimeSpan delay, CancellationToken cancellationToken)
        {
            Delays.Add(delay);
            if (CrashOnBackoff && Fetches.Count > FailPartitionsAfter)
                throw new SimulatedCrash();
            return Task.CompletedTask;
        }

        public Task<int> Run() => RunOnceAsync(Now, CancellationToken.None);
    }

    private class SimulatedCrash : Exception
    {
    }

    private List<string> events;
    private InMemoryAuctionBlobStore store;
    private InMemoryMarkers markers;
    private InMemoryLocationLookup locations;
    private TestableAudit audit;
    private readonly List<(string Tag, short Key)> partitionList = new();

    [SetUp]
    public void SetUp()
    {
        PlayerOptOut.ResetMemory();
        partitionList.Clear();
        events = new List<string>();
        store = new InMemoryAuctionBlobStore(events);
        markers = new InMemoryMarkers(events);
        locations = new InMemoryLocationLookup();
        locations.Tags[HyperionId] = Hyperion;
        locations.Tags[TerminatorId] = Terminator;
        audit = Audit();
    }

    private TestableAudit Audit(params (string, string)[] config)
        => new(store, markers, locations, ArchiveTestConfig.Build(config.Prepend(("ARCHIVE:AUDIT_ENABLED", "true")).ToArray()));

    private static DateTime InSep(int day) => new(2020, 9, day, 12, 0, 0, DateTimeKind.Utc);
    private static DateTime InAug(int day) => new(2020, 8, day, 12, 0, 0, DateTimeKind.Utc);

    private static SaveAuction Maria(long uid, DateTime end, long bid = 0, string tag = Hyperion) => ScyllaAuditEquivalenceTests.Archived(uid, bid, end, tag);

    private static ScyllaAuditRow Row(ScyllaAuction row, long writeTime = 1_000) => new(row, writeTime);

    /// <summary>R2 holds the auctions as MariaDB (phase 1) rows of part 0 of the tag-month, located.</summary>
    private void ArchiveMaria(string tag, ushort tagId, int month, params SaveAuction[] rows)
        => Archive(tag, tagId, month, ArchivedAuctionBlock.SourceMariaDb, rows.Select(r => (r, (ScyllaAuctionExtras)null)).ToArray());

    private void Archive(string tag, ushort tagId, int month, string source, (SaveAuction Row, ScyllaAuctionExtras Extras)[] rows, int part = 0)
    {
        var block = ArchivedAuctionBlock.FromAuctions(tag, month, part, source, rows);
        store.WriteBlockAsync(block).Wait();
        IndexPart(tag, month, part, block.Sha256());
        foreach (var uid in block.AuctionUid)
            locations.Locations[uid] = new AuctionLocation { Uid = uid, TagId = tagId, Month = ArchiveMonths.ToIndex(month), Part = (byte)part };
        events.Clear();
    }

    /// <summary>List the part with this hash in the tag index (what SkyIndexer / the sweep do after verifying a block).</summary>
    private void IndexPart(string tag, int month, int part, string sha256)
    {
        var index = store.ReadIndexAsync(tag).Result ?? new ArchivedTagIndex { Tag = tag };
        var entry = index.Find(month) ?? new ArchivedMonthEntry { Month = month };
        while (entry.PartSha256.Count <= part)
            entry.PartSha256.Add("");
        entry.PartSha256[part] = sha256;
        entry.Parts = entry.PartSha256.Count;
        store.WriteIndexAsync(index.Upsert(entry)).Wait();
    }

    /// <summary>Put Scylla rows into their partitions (2 rows per page) and the partition list.</summary>
    private void AddScylla(params ScyllaAuditRow[] rows)
    {
        foreach (var item in rows)
        {
            var key = (item.Row.Tag, item.Row.TimeKey);
            if (!audit.Pages.TryGetValue(key, out var pages))
                audit.Pages[key] = pages = new List<List<ScyllaAuditRow>>();
            if (pages.Count == 0 || pages[^1].Count >= 2)
                pages.Add(new List<ScyllaAuditRow>());
            pages[^1].Add(item);
            if (!partitionList.Contains(key))
                partitionList.Add(key);
            audit.ByUid[ScyllaArchiveRows.UidOf(item.Row)] = audit.ByUid.GetValueOrDefault(ScyllaArchiveRows.UidOf(item.Row)) is { } existing ? existing.Append(item).ToList() : new List<ScyllaAuditRow> { item };
        }
        PublishPartitionList();
    }

    private void PublishPartitionList()
    {
        store.WriteMetaAsync(ScyllaArchiveSweep.PartitionListKey, new ScyllaPartitionList
        {
            CreatedAt = Now,
            OldestMonth = Aug,
            NewestMonth = 202012,
            Partitions = partitionList.OrderBy(p => p.Tag, StringComparer.Ordinal).ThenBy(p => p.Key).ToList(),
        }).Wait();
        events.Clear();
    }

    private static ScyllaAuction Raw(SaveAuction archived) => ScyllaAuditEquivalenceTests.Stored(archived);

    private ScyllaAuditMonth Result(int month) => store.ReadMetaAsync<ScyllaAuditMonth>(ScyllaArchiveAudit.ResultKey(month)).Result;

    private static long Count(ScyllaAuditMonth result, string cls) => result.ClassCounts.GetValueOrDefault(cls);

    // ---------------------------------------------------------------- classes

    [Test]
    public async Task EqualRow_MariaDbSourceWithInjectedItemBytesAndRandomFiller()
    {
        var r = Maria(1, InSep(3));
        r.AnvilUses = 2;
        r.Reforge = ItemReferences.Reforge.chomp;
        ArchiveMaria(Hyperion, HyperionId, Sep, r);
        AddScylla(Row(Raw(r)));

        await audit.Run();

        var result = Result(Sep);
        Assert.That(Count(result, AuditClass.Equal), Is.EqualTo(1));
        Assert.That(result.ClassCounts.Keys, Is.EquivalentTo(new[] { AuditClass.Equal }));
        Assert.That(result.CompletedAt, Is.Not.Null);
    }

    [Test]
    public async Task EqualRow_ScyllaSourceBlockWithExtras()
    {
        var stored = Raw(Maria(1, InSep(3), bid: 100));
        stored.ProfileName = "Mango";
        stored.HighestBidderName = "Alice";
        stored.HighestBidder = stored.Bids[0].BidderUuid;
        var (mapped, extras) = ScyllaArchiveRows.Map(Clone(stored), Hyperion);
        Archive(Hyperion, HyperionId, Sep, ArchivedAuctionBlock.SourceScylla, new[] { (mapped, extras) });
        AddScylla(Row(stored));

        await audit.Run();

        Assert.That(Count(Result(Sep), AuditClass.Equal), Is.EqualTo(1), string.Join(",", Result(Sep).ColumnDiffs));
    }

    [Test]
    public async Task R2BetterSuperset_UnsoldSnapshotOfAnAuctionR2HasSold()
    {
        var sold = Maria(1, InSep(3), bid: 500);
        ArchiveMaria(Hyperion, HyperionId, Sep, sold);
        var snapshot = Raw(Maria(1, InSep(3)));
        snapshot.End = snapshot.End.AddHours(-1);
        AddScylla(Row(Raw(sold)), Row(snapshot));

        await audit.Run();

        var result = Result(Sep);
        Assert.That((Count(result, AuditClass.Equal), Count(result, AuditClass.R2BetterSuperset)), Is.EqualTo((1L, 1L)));
        Assert.That(result.Samples[AuditClass.R2BetterSuperset].Single().Uid, Is.EqualTo(sold.UId));
    }

    [Test]
    public async Task NeedsMerge_ScyllaHasColumnsTheMariaDbRowLacks_ColumnsAreRecorded()
    {
        var r = Maria(1, InSep(3), bid: 500);
        ArchiveMaria(Hyperion, HyperionId, Sep, r);
        var stored = Raw(r);
        stored.ProfileName = "Mango";
        stored.ItemLore = "lore";
        AddScylla(Row(stored));

        await audit.Run();

        var result = Result(Sep);
        Assert.That(Count(result, AuditClass.NeedsMerge), Is.EqualTo(1));
        Assert.That(result.ColumnDiffs, Is.EquivalentTo(new Dictionary<string, long> { ["profilename"] = 1, ["itemlore"] = 1 }));
        Assert.That(result.Samples[AuditClass.NeedsMerge].Single().Columns, Is.EquivalentTo(new[] { "profilename", "itemlore" }));
        Assert.That(result.NonNullColumns["itemlore"], Is.EqualTo(1));
    }

    [Test]
    public async Task Missing_UidNotLocated_AndLocatedButAbsentFromTheBlock()
    {
        var archived = Maria(1, InSep(3));
        ArchiveMaria(Hyperion, HyperionId, Sep, archived);
        var notArchived = Raw(Maria(2, InSep(4)));
        var ghost = Maria(3, InSep(5));
        locations.Locations[ghost.UId] = new AuctionLocation { Uid = ghost.UId, TagId = HyperionId, Month = ArchiveMonths.ToIndex(Sep), Part = 0 };
        AddScylla(Row(Raw(archived)), Row(notArchived), Row(Raw(ghost)));

        await audit.Run();

        var result = Result(Sep);
        Assert.That(Count(result, AuditClass.Missing), Is.EqualTo(2));
        Assert.That(result.Samples[AuditClass.Missing].Select(s => s.Columns.Single()), Is.EquivalentTo(new[] { "no_location", "block_row_missing" }));
    }

    [Test]
    public async Task BogusEnd_IsStillLookedUp_AndRecordedAsNotInR2()
    {
        var old = Raw(Maria(1, new DateTime(2019, 3, 3, 0, 0, 0, DateTimeKind.Utc)));
        old.TimeKey = ScyllaService.GetWeekOrDaysSinceStart(Hyperion, InSep(3)); // random-timekey garbage lives in a partition of a real month
        AddScylla(Row(old));
        locations.Calls.Clear();

        await audit.Run();

        var result = Result(Sep);
        Assert.That(Count(result, AuditClass.BogusEnd), Is.EqualTo(1));
        Assert.That(locations.Calls, Is.Not.Empty, "bogus ends are looked up like every other row");
        Assert.That(result.Samples[AuditClass.BogusEnd].Single().Columns, Is.EqualTo(new[] { ScyllaArchiveAudit.BogusNotInR2 }));
        Assert.That((result.BogusEndInR2, result.Partitions.Single().BogusEndInR2), Is.EqualTo((0L, 0)));
    }

    [Test]
    public async Task BogusEnd_WithAMatchingR2Row_IsRecordedButNeverCoveredOrCompared()
    {
        ArchiveMaria(Hyperion, HyperionId, Sep, Maria(1, InSep(3)));
        var old = Raw(Maria(1, new DateTime(2019, 3, 3, 0, 0, 0, DateTimeKind.Utc)));
        old.TimeKey = ScyllaService.GetWeekOrDaysSinceStart(Hyperion, InSep(3));
        AddScylla(Row(old));

        await audit.Run();

        var result = Result(Sep);
        Assert.That(result.ClassCounts.Keys, Is.EquivalentTo(new[] { AuditClass.BogusEnd }), "not equal, not covered: the row has a bogus end");
        Assert.That(result.Samples[AuditClass.BogusEnd].Single().Columns, Is.EqualTo(new[] { ScyllaArchiveAudit.BogusInR2 }));
        Assert.That((result.BogusEndInR2, result.Partitions.Single().BogusEndInR2), Is.EqualTo((1L, 1)));
    }

    // ---------------------------------------------------------------- the awkward partitions

    [Test]
    public async Task SpanningPartition_IsAuditedForBothMonths_ReadOnceAndLocatedByUidNotByMonth()
    {
        // 2020-09-30 .. 2020-10-06 is one 7 day bucket of HYPERION: rows in two months, each located in its own tag-month
        audit = Audit(("ARCHIVE:OLDEST_MONTH", "2020-09"), ("ARCHIVE:NEWEST_MONTH", "2020-10"));
        var sepRow = Maria(1, new DateTime(2020, 9, 30, 12, 0, 0, DateTimeKind.Utc));
        var octRow = Maria(2, new DateTime(2020, 10, 2, 12, 0, 0, DateTimeKind.Utc));
        ArchiveMaria(Hyperion, HyperionId, Sep, sepRow);
        ArchiveMaria(Hyperion, HyperionId, Oct, octRow);
        var sepRaw = Raw(sepRow);
        var octRaw = Raw(octRow);
        Assert.That(sepRaw.TimeKey, Is.EqualTo(octRaw.TimeKey), "same partition");
        AddScylla(Row(sepRaw), Row(octRaw));

        Assert.That(await audit.Run(), Is.EqualTo(2));

        foreach (var month in new[] { Sep, Oct })
        {
            var result = Result(month);
            Assert.That(Count(result, AuditClass.Equal), Is.EqualTo(2), $"both rows of the partition are classified in {month}");
            Assert.That(result.Partitions.Single().SpansMonths, Is.True);
            Assert.That(result.Partitions.Single().Rows, Is.EqualTo(2));
        }
        Assert.That(audit.Fetches.Select(f => (f.tag, f.key)).Distinct().Count(), Is.EqualTo(1));
        Assert.That(audit.Fetches.Count(f => f.page == 0), Is.EqualTo(1), "the partition is read once and reused for the next month");
        Assert.That(Result(Sep).Partitions.Single().Sha256, Is.EqualTo(Result(Oct).Partitions.Single().Sha256));
    }

    [Test]
    public async Task RetaggedUnknownRow_IsFoundUnderItsRealTag()
    {
        var r = Maria(1, InSep(3), tag: Terminator);
        ArchiveMaria(Terminator, TerminatorId, Sep, r);
        var raw = Raw(r);
        raw.Tag = "unknown";
        raw.TimeKey = ScyllaService.GetWeekOrDaysSinceStart("unknown", r.End);
        AddScylla(Row(raw));

        await audit.Run();

        var result = Result(Sep);
        Assert.That(Count(result, AuditClass.Equal), Is.EqualTo(1), string.Join(",", result.ColumnDiffs));
        Assert.That(result.Partitions.Single().Tag, Is.EqualTo("unknown"));
    }

    [Test]
    public async Task AuctionLocatedInAnotherMonth_IsNotMissing()
    {
        // an unsold snapshot in September whose auction ended (and is archived) in August
        var r = Maria(1, InAug(30));
        ArchiveMaria(Hyperion, HyperionId, Aug, r);
        var snapshot = Raw(Maria(1, InAug(30)));
        snapshot.End = InSep(1);
        snapshot.TimeKey = ScyllaService.GetWeekOrDaysSinceStart(Hyperion, InSep(1));
        AddScylla(Row(snapshot));

        await audit.Run();

        Assert.That(Count(Result(Sep), AuditClass.Missing), Is.Zero);
        Assert.That(Result(Sep).ClassCounts.Values.Sum(), Is.EqualTo(1));
    }

    // ---------------------------------------------------------------- read only, resumable, throttled

    [Test]
    public async Task OnlyTheResultObjectAndTheProgressKeyAreWritten()
    {
        var r = Maria(1, InSep(3), bid: 100);
        ArchiveMaria(Hyperion, HyperionId, Sep, r);
        var stored = Raw(r);
        stored.ItemLore = "lore";
        AddScylla(Row(stored));
        var objectsBefore = store.Objects.Where(o => !o.Key.Contains("_meta/audit/")).ToDictionary(o => o.Key, o => o.Value.ToArray());

        await audit.Run();

        Assert.That(events.Where(e => !e.StartsWith("read:")), Is.All.Matches<string>(e =>
            e == "meta:" + ScyllaArchiveAudit.ResultKey(Aug) || e == "meta:" + ScyllaArchiveAudit.ResultKey(Sep)
            || e == "marker:" + ScyllaArchiveMarkerKeys.Audit(Aug) || e == "marker:" + ScyllaArchiveMarkerKeys.Audit(Sep)));
        Assert.That(events.Any(e => e.StartsWith("write:") || e.StartsWith("tag_index:")), Is.False);
        var objectsAfter = store.Objects.Where(o => !o.Key.Contains("_meta/audit/")).ToDictionary(o => o.Key, o => o.Value);
        Assert.That(objectsAfter.Keys, Is.EquivalentTo(objectsBefore.Keys), "no block, index or manifest created");
        Assert.That(objectsAfter.All(o => o.Value.SequenceEqual(objectsBefore[o.Key])), Is.True, "none changed");
        Assert.That(markers.Values.Keys.All(k => k.StartsWith(ScyllaArchiveMarkerKeys.Audit(Aug)[..^7], StringComparison.Ordinal)), Is.True, "only audit keys in Redis");
        Assert.That(audit.LastConsistency, Is.EqualTo(ConsistencyLevel.All), "ALL by default");
    }

    [Test]
    public async Task CompletedMonths_AreSkippedAndRedisHealed_ConsistencyChangeStartsOver()
    {
        var r = Maria(1, InSep(3));
        ArchiveMaria(Hyperion, HyperionId, Sep, r);
        AddScylla(Row(Raw(r)));
        Assert.That(await audit.Run(), Is.EqualTo(2));
        var fetches = audit.Fetches.Count;
        markers.Values.Clear();

        Assert.That(await audit.Run(), Is.Zero);

        Assert.That(audit.Fetches.Count, Is.EqualTo(fetches), "no Scylla read for finished months");
        Assert.That(markers.Values[ScyllaArchiveMarkerKeys.Audit(Sep)], Does.StartWith("done:"));

        var quorum = Audit(("ARCHIVE:AUDIT_CONSISTENCY", "LocalQuorum"));
        quorum.Pages.Add((Hyperion, ScyllaService.GetWeekOrDaysSinceStart(Hyperion, InSep(3))), new List<List<ScyllaAuditRow>> { new() { Row(Raw(r)) } });
        await quorum.Run();
        Assert.That(quorum.LastConsistency, Is.EqualTo(ConsistencyLevel.LocalQuorum));
        Assert.That(Result(Sep).Consistency, Is.EqualTo("LocalQuorum"));
    }

    [Test]
    public async Task Resume_ContinuesAtTheCheckpointedPartition_WithoutRereadingFinishedOnes()
    {
        var rows = Enumerable.Range(1, 5).Select(i => Maria(i, InSep(i + 2), tag: $"TAG{i}")).ToList();
        foreach (var r in rows)
        {
            locations.Tags[(ushort)(10 + r.UId)] = r.Tag;
            ArchiveMaria(r.Tag, (ushort)(10 + r.UId), Sep, r);
        }
        AddScylla(rows.Select(r => Row(Raw(r))).ToArray());
        audit = Audit(("ARCHIVE:AUDIT_CHECKPOINT_PARTITIONS", "2"));
        foreach (var kv in Rebuild(rows)) audit.Pages[kv.Key] = kv.Value;
        audit.FailPartitionsAfter = 3; // crash while reading the 4th partition
        audit.CrashOnBackoff = true;
        Assert.That(async () => await audit.Run(), Throws.TypeOf<SimulatedCrash>());
        var partial = Result(Sep);
        Assert.That(partial.CompletedAt, Is.Null);
        Assert.That(partial.NextPartition, Is.EqualTo(2), "checkpoint after 2 partitions");
        Assert.That(markers.Values[ScyllaArchiveMarkerKeys.Audit(Sep)], Does.StartWith("progress:2/5:"));

        audit = Audit(("ARCHIVE:AUDIT_CHECKPOINT_PARTITIONS", "2"));
        foreach (var kv in Rebuild(rows)) audit.Pages[kv.Key] = kv.Value;
        await audit.Run();

        var done = Result(Sep);
        Assert.That(done.CompletedAt, Is.Not.Null);
        Assert.That(done.Partitions.Count, Is.EqualTo(5));
        Assert.That(done.Partitions.Select(p => p.Tag), Is.Unique);
        Assert.That(Count(done, AuditClass.Equal), Is.EqualTo(5));
        Assert.That(audit.Fetches.Select(f => f.tag).Distinct().Count(), Is.EqualTo(3), "only the three unfinished partitions were read again");
    }

    private IEnumerable<KeyValuePair<(string, short), List<List<ScyllaAuditRow>>>> Rebuild(List<SaveAuction> rows)
        => rows.Select(r => new KeyValuePair<(string, short), List<List<ScyllaAuditRow>>>((r.Tag, ScyllaService.GetWeekOrDaysSinceStart(r.Tag, r.End)), new List<List<ScyllaAuditRow>> { new() { Row(Raw(r)) } }));

    [Test]
    public async Task PartitionsAreThrottled_ByPartitionsPerMinute()
    {
        var r = Maria(1, InSep(3));
        ArchiveMaria(Hyperion, HyperionId, Sep, r);
        AddScylla(Row(Raw(r)));
        audit = Audit(("ARCHIVE:AUDIT_PARTITIONS_PER_MINUTE", "6"));
        foreach (var kv in Rebuild(new List<SaveAuction> { r })) audit.Pages[kv.Key] = kv.Value;

        await audit.Run();

        Assert.That(audit.Delays, Does.Contain(TimeSpan.FromSeconds(10)), "6 per minute = one partition every 10 s");
    }

    [Test]
    public async Task MonthNotImportedByTheIndexer_IsSkipped_AndWithoutPartitionListNothingRuns()
    {
        var r = Maria(1, InSep(3));
        ArchiveMaria(Hyperion, HyperionId, Sep, r);
        AddScylla(Row(Raw(r)));
        audit.Imported.Remove(Sep);

        Assert.That(await audit.Run(), Is.EqualTo(1), "August (no partitions) completes, September waits");
        Assert.That(Result(Sep), Is.Null);

        store.Objects.Remove(AuctionBlobKeys.Meta(ScyllaArchiveSweep.PartitionListKey));
        Assert.That(await audit.Run(), Is.Zero);
    }

    [Test]
    public async Task PartitionHash_ChangesWhenARowOrItsWritetimeChanges()
    {
        var r = Maria(1, InSep(3));
        ArchiveMaria(Hyperion, HyperionId, Sep, r);
        AddScylla(Row(Raw(r), writeTime: 5_000));
        await audit.Run();
        var first = Result(Sep).Partitions.Single();
        Assert.That(first.MaxWriteTime, Is.EqualTo(5_000));
        Assert.That(first.Sha256, Has.Length.EqualTo(64));
        Assert.That((first.Rows, first.MinEnd, first.MaxEnd), Is.EqualTo((1, (DateTime?)r.End, (DateTime?)r.End)));

        var hashA = ScyllaArchiveAudit.HashTuples(new List<(long, long, long, long, long)> { (1, 2, 3, 4, 0), (0, 2, 3, 4, 0) });
        var hashB = ScyllaArchiveAudit.HashTuples(new List<(long, long, long, long, long)> { (0, 2, 3, 4, 0), (1, 2, 3, 4, 0) });
        var hashC = ScyllaArchiveAudit.HashTuples(new List<(long, long, long, long, long)> { (0, 2, 3, 4, 0), (1, 2, 3, 5, 0) });
        var hashD = ScyllaArchiveAudit.HashTuples(new List<(long, long, long, long, long)> { (0, 2, 3, 4, 0), (1, 2, 3, 4, 1) });
        Assert.That(hashA, Is.EqualTo(hashB), "sorted, so the page order does not matter");
        Assert.That(hashA, Is.Not.EqualTo(hashC), "writetime is part of the hash");
        Assert.That(hashA, Is.Not.EqualTo(hashD), "issold is part of the hash");
    }

    // ---------------------------------------------------------------- sample read-back

    [Test]
    public async Task SampleReadBack_FetchesThroughTheReaderAndComparesWithScylla()
    {
        var rows = new[] { Maria(1, InSep(3), bid: 100), Maria(2, InSep(4)) };
        ArchiveMaria(Hyperion, HyperionId, Sep, rows);
        AddScylla(rows.Select(r => Row(Raw(r))).ToArray());
        store.Events.Clear();

        await audit.Run();

        var readBack = Result(Sep).ReadBack;
        Assert.That((readBack.Sampled, readBack.Matched, readBack.Mismatched), Is.EqualTo((2, 2, 0)));
        Assert.That(locations.Calls.Contains("location:" + rows[0].UId), Is.True, "went through the public reader's auction_locations lookup");
    }

    [Test]
    public async Task SampleReadBack_ReportsAnAuctionThatDiffersFromScylla()
    {
        var found = Maria(1, InSep(3), bid: 100);
        var diverged = Maria(2, InSep(4), bid: 100);
        ArchiveMaria(Hyperion, HyperionId, Sep, found, diverged);
        AddScylla(Row(Raw(found)), Row(Raw(diverged)));
        // Scylla changed after the partition scan: the second auction now carries a lore the archive lacks
        audit.ByUid[diverged.UId][0].Row.ItemLore = "lore";

        await audit.Run();

        var result = Result(Sep).ReadBack;
        Assert.That((result.Matched, result.Mismatched), Is.EqualTo((1, 1)));
        Assert.That(result.Samples.Single().Uid, Is.EqualTo(diverged.UId));
        Assert.That(result.Samples.Single().Columns, Is.EqualTo(new[] { "itemlore" }));
    }

    [Test]
    public async Task SampleReadBack_ReportsAnArchivedAuctionThePublicLookupByUuidCannotFind()
    {
        var odd = Maria(7, InSep(5));
        odd.UId = 123456; // not the uid AuctionService derives from the uuid: found by the scan (uid), invisible to GET /api/auction/{uuid}
        ArchiveMaria(Hyperion, HyperionId, Sep, odd);
        AddScylla(Row(Raw(odd)));

        await audit.Run();

        var result = Result(Sep);
        Assert.That(Count(result, AuditClass.Equal), Is.EqualTo(1));
        Assert.That(result.ReadBack.Mismatched, Is.EqualTo(1));
        Assert.That(result.ReadBack.Samples.Single().Columns, Is.EqualTo(new[] { "not_found_by_reader" }));
    }

    // ---------------------------------------------------------------- options

    [Test]
    public void Options_DefaultsAndOverrides()
    {
        var d = ScyllaArchiveAuditOptions.From(ArchiveTestConfig.Build());
        Assert.That((d.Enabled, d.Consistency, d.OldestMonth, d.NewestMonth, d.PartitionsPerMinute, d.SampleReadBack), Is.EqualTo((false, ConsistencyLevel.All, 202008, 202009, 30, 200)), "months default to the sweep range");
        var o = ScyllaArchiveAuditOptions.From(ArchiveTestConfig.Build(("ARCHIVE:AUDIT_ENABLED", "true"), ("ARCHIVE:AUDIT_CONSISTENCY", "quorum"), ("ARCHIVE:AUDIT_MONTHS", "2020-09..2020-10"), ("ARCHIVE:AUDIT_PARTITIONS_PER_MINUTE", "0")));
        Assert.That((o.Enabled, o.Consistency, o.OldestMonth, o.NewestMonth, o.PartitionsPerMinute), Is.EqualTo((true, ConsistencyLevel.Quorum, 202009, 202010, 1)));
        var single = ScyllaArchiveAuditOptions.From(ArchiveTestConfig.Build(("ARCHIVE:AUDIT_MONTHS", "2020-09")));
        Assert.That((single.OldestMonth, single.NewestMonth), Is.EqualTo((202009, 202009)));
        var invalid = ScyllaArchiveAuditOptions.From(ArchiveTestConfig.Build(("ARCHIVE:AUDIT_MONTHS", "nope"), ("ARCHIVE:AUDIT_CONSISTENCY", "nope")));
        Assert.That((invalid.OldestMonth, invalid.NewestMonth, invalid.Consistency), Is.EqualTo((202008, 202009, ConsistencyLevel.All)));
    }

    [Test]
    public void ShippedAppsettings_AuditIsDisabledAndMatchesTheDefaults()
    {
        var dir = new System.IO.DirectoryInfo(AppContext.BaseDirectory);
        string path = null;
        for (; dir != null && path == null; dir = dir.Parent)
            if (System.IO.File.Exists(System.IO.Path.Combine(dir.FullName, "appsettings.json")) && System.IO.File.Exists(System.IO.Path.Combine(dir.FullName, "SkyAuctions.csproj")))
                path = System.IO.Path.Combine(dir.FullName, "appsettings.json");
        Assert.That(path, Is.Not.Null);
        var shipped = ScyllaArchiveAuditOptions.From(new ConfigurationBuilder().AddJsonFile(path).Build());
        Assert.That(shipped, Is.EqualTo(new ScyllaArchiveAuditOptions()));
        Assert.That(shipped.Enabled, Is.False);
    }

    // ---------------------------------------------------------------- review fixes: sold rule, bid timestamps

    [Test]
    public async Task IdenticalRowWithABid_IsEqualNotR2Better_EvenWhenScyllaStoredItBeforeItSold()
    {
        var r = Maria(1, InSep(3), bid: 500);
        ArchiveMaria(Hyperion, HyperionId, Sep, r);
        var stored = Raw(r);
        stored.IsSold = false; // raw issold of a row written while the auction still ran
        AddScylla(Row(stored));

        await audit.Run();

        var result = Result(Sep);
        Assert.That(result.ClassCounts.Keys, Is.EquivalentTo(new[] { AuditClass.Equal }), "same bid, same sold state under the bid > 0 rule");
        Assert.That(Fingerprint.ForAudit(stored), Is.EqualTo(Fingerprint.ForAudit(ArchivedAuctionBlock.FromAuctions(Hyperion, Sep, 0, ArchivedAuctionBlock.SourceMariaDb, new[] { (r, (ScyllaAuctionExtras)null) }), 0)));
    }

    [Test]
    public async Task SameBidAndSoldStateWithAnotherEnd_NeedsMerge_NotR2Better()
    {
        var r = Maria(1, InSep(3), bid: 500);
        ArchiveMaria(Hyperion, HyperionId, Sep, r);
        var stored = Raw(r);
        stored.IsSold = false;
        stored.End = r.End.AddSeconds(5);
        AddScylla(Row(stored));

        await audit.Run();

        var result = Result(Sep);
        Assert.That(Count(result, AuditClass.NeedsMerge), Is.EqualTo(1));
        Assert.That(result.ColumnDiffs.Keys, Does.Contain("end"));
        Assert.That(Count(result, AuditClass.R2BetterSuperset), Is.Zero);
    }

    private (SaveAuction Full, SaveAuction Snapshot) TwoBidAuction()
    {
        var full = Maria(1, InSep(3), bid: 500);
        full.Bids.Insert(0, new SaveBids { Bidder = full.Bids[0].Bidder, ProfileId = full.Bids[0].ProfileId, Amount = 100, Timestamp = full.End.AddHours(-2), AuctionId = full.Uuid });
        var snapshot = Maria(1, InSep(3), bid: 100);
        snapshot.Bids[0].Timestamp = full.Bids[0].Timestamp;
        return (full, snapshot);
    }

    [Test]
    public async Task OlderSnapshot_WithTheBidsOfTheBetterRow_IsR2BetterSuperset()
    {
        var (full, snapshot) = TwoBidAuction();
        ArchiveMaria(Hyperion, HyperionId, Sep, full);
        AddScylla(Row(Raw(snapshot)));

        await audit.Run();

        Assert.That(Result(Sep).ClassCounts.Keys, Is.EquivalentTo(new[] { AuditClass.R2BetterSuperset }), string.Join(",", Result(Sep).ColumnDiffs));
    }

    [Test]
    public async Task BidTimestampsAreComparedEvenWhenR2IsBetter()
    {
        var (full, snapshot) = TwoBidAuction();
        ArchiveMaria(Hyperion, HyperionId, Sep, full);
        var stored = Raw(snapshot);
        var shifted = stored.Bids;
        shifted[0].Timestamp = shifted[0].Timestamp.AddMinutes(1);
        stored.SerialisedBids = MessagePack.MessagePackSerializer.Serialize<IEnumerable<CassandraBid>>(shifted);
        AddScylla(Row(stored));

        await audit.Run();

        var result = Result(Sep);
        Assert.That(Count(result, AuditClass.NeedsMerge), Is.EqualTo(1));
        Assert.That(result.ColumnDiffs.Keys, Is.EquivalentTo(new[] { "serialisedbids" }));
    }

    // ---------------------------------------------------------------- review fixes: resume, schema, proof, unreadable

    private List<SaveAuction> FiveTagRows(params int[] tagNumbers)
    {
        var rows = tagNumbers.Select(i => Maria(i, InSep(i + 2), tag: $"TAG{i}")).ToList();
        foreach (var r in rows)
        {
            locations.Tags[(ushort)(10 + r.UId)] = r.Tag;
            ArchiveMaria(r.Tag, (ushort)(10 + r.UId), Sep, r);
        }
        AddScylla(rows.Select(r => Row(Raw(r))).ToArray());
        return rows;
    }

    [Test]
    public async Task Resume_StartsOverWhenThePartitionListChangedButHasTheSameCount()
    {
        var first = FiveTagRows(1, 2, 3, 4, 5);
        audit = Audit(("ARCHIVE:AUDIT_CHECKPOINT_PARTITIONS", "2"));
        foreach (var kv in Rebuild(first)) audit.Pages[kv.Key] = kv.Value;
        audit.FailPartitionsAfter = 3;
        audit.CrashOnBackoff = true;
        Assert.That(async () => await audit.Run(), Throws.TypeOf<SimulatedCrash>());
        Assert.That(Result(Sep).NextPartition, Is.EqualTo(2));

        partitionList.Clear(); // TAG5 is gone, TAG6 appeared: still 5 partitions
        var second = FiveTagRows(1, 2, 3, 4, 6);
        audit = Audit(("ARCHIVE:AUDIT_CHECKPOINT_PARTITIONS", "2"));
        foreach (var kv in Rebuild(second)) audit.Pages[kv.Key] = kv.Value;
        await audit.Run();

        var done = Result(Sep);
        Assert.That(done.Partitions.Select(p => p.Tag), Is.EquivalentTo(new[] { "TAG1", "TAG2", "TAG3", "TAG4", "TAG6" }));
        Assert.That(audit.Fetches.Select(f => f.tag).Distinct().Count(), Is.EqualTo(5), "nothing of the old checkpoint was reused");
        Assert.That(Count(done, AuditClass.Equal), Is.EqualTo(5));
    }

    [Test]
    public void PartitionListHash_IsOrderIndependent_AndChangesWithAnyPartition()
    {
        var a = ScyllaArchiveAudit.PartitionListHash(new[] { ("A", (short)1), ("B", (short)2) });
        Assert.That(ScyllaArchiveAudit.PartitionListHash(new[] { ("B", (short)2), ("A", (short)1) }), Is.EqualTo(a));
        Assert.That(ScyllaArchiveAudit.PartitionListHash(new[] { ("A", (short)1), ("B", (short)3) }), Is.Not.EqualTo(a));
        Assert.That(ScyllaArchiveAudit.PartitionListHash(new[] { ("A", (short)1), ("C", (short)2) }), Is.Not.EqualTo(a));
        Assert.That(a, Has.Length.EqualTo(64));
    }

    [Test]
    public async Task UnknownTableColumn_StopsTheAuditBeforeAnythingIsRead()
    {
        var r = Maria(1, InSep(3));
        ArchiveMaria(Hyperion, HyperionId, Sep, r);
        AddScylla(Row(Raw(r)));
        audit.Schema = TestableAudit.DefaultSchema().Append(new ScyllaColumnInfo("newcolumn", "regular", "text")).ToList();

        Assert.That(await audit.Run(), Is.Zero);

        Assert.That(Result(Sep), Is.Null);
        Assert.That(audit.Fetches, Is.Empty);
        Assert.That(audit.Log.Entries.Any(e => e.level == LogLevel.Error && e.message.Contains("newcolumn")), Is.True);
    }

    [Test]
    public void Schema_QuotesKeywordsAndSelectsTheWriteTimesOfFrozenColumnsOnly()
    {
        var columns = TestableAudit.DefaultSchema();
        columns[columns.FindIndex(c => c.Name == "nbtlookup")] = new ScyllaColumnInfo("nbtlookup", "regular", "map<text, text>");
        var schema = ScyllaAuditSchema.Build(columns);

        Assert.That(schema.IsUsable, Is.True);
        Assert.That(schema.PartitionCql, Does.Contain("\"end\"").And.Contain("\"count\"").And.Not.Contain(" end,").And.Not.Contain(" count,"));
        Assert.That(schema.WriteTimeSelected, Is.EqualTo(new[] { "serialisedbids", "itembytes", "auctioneer", "highestbidamount" }));
        Assert.That(schema.WriteTimeSkipped, Is.EqualTo(new[] { "nbtlookup" }), "a non-frozen map has no WRITETIME");
        Assert.That(schema.PartitionCql, Does.Contain("WRITETIME(serialisedbids) AS wt0").And.Contain("WRITETIME(highestbidamount) AS wt3"));
        Assert.That(schema.UidCql, Does.Contain("WHERE auctionuid = ?"));
        var all = ScyllaAuditSchema.Build(TestableAudit.DefaultSchema());
        Assert.That(all.WriteTimeSelected, Is.EqualTo(ScyllaAuditSchema.WriteTimeColumns));
        Assert.That(ScyllaAuditSchema.Build(TestableAudit.DefaultSchema().Where(c => c.Name != "tier")).Absent, Is.EqualTo(new[] { "tier" }));
    }

    [Test]
    public async Task RowWithoutAnyWriteTime_IsAudited_AndHashedAsZero()
    {
        var r = Maria(1, InSep(3));
        ArchiveMaria(Hyperion, HyperionId, Sep, r);
        AddScylla(new ScyllaAuditRow(Raw(r), null));

        await audit.Run();

        var partition = Result(Sep).Partitions.Single();
        Assert.That(partition.MaxWriteTime, Is.Null);
        Assert.That(partition.Sha256, Is.EqualTo(ScyllaArchiveAudit.HashTuples(new List<(long, long, long, long, long)> { (r.UId, r.End.Ticks, 0, 0, 0) })), "an unsold row: issold false, no bid");
    }

    [Test]
    public async Task BlocksAreReadUncached()
    {
        var r = Maria(1, InSep(3));
        ArchiveMaria(Hyperion, HyperionId, Sep, r);
        AddScylla(Row(Raw(r)));
        audit.Imported.Remove(Aug);

        await audit.Run();

        Assert.That(events.Where(e => e.StartsWith("read:")).First(), Is.EqualTo($"read:{Hyperion}:{Sep}:0:uncached"));
    }

    [Test]
    public async Task OrphanPart_NotInTheTagIndex_IsMissing()
    {
        var r = Maria(1, InSep(3));
        ArchiveMaria(Hyperion, HyperionId, Sep, r);
        var index = await store.ReadIndexAsync(Hyperion);
        index.Months.Clear();
        await store.WriteIndexAsync(index);
        AddScylla(Row(Raw(r)));

        await audit.Run();

        var result = Result(Sep);
        Assert.That(result.ClassCounts.Keys, Is.EquivalentTo(new[] { AuditClass.Missing }));
        Assert.That(result.Samples[AuditClass.Missing].Single().Columns, Is.EqualTo(new[] { ScyllaArchiveAudit.BlockNotInIndex }));
    }

    [Test]
    public async Task PartWhoseHashDiffersFromTheTagIndex_IsMissing()
    {
        var r = Maria(1, InSep(3));
        ArchiveMaria(Hyperion, HyperionId, Sep, r);
        IndexPart(Hyperion, Sep, 0, new string('0', 64));
        AddScylla(Row(Raw(r)));

        await audit.Run();

        Assert.That(Result(Sep).Samples[AuditClass.Missing].Single().Columns, Is.EqualTo(new[] { ScyllaArchiveAudit.BlockHashMismatch }));
    }

    [Test]
    public async Task UnreadablePartition_IsRecordedNotCovered_TheAuditGoesOn_AndTheNextPassRetriesIt()
    {
        var rows = FiveTagRows(1, 2);
        foreach (var kv in Rebuild(rows)) audit.Pages[kv.Key] = kv.Value;
        audit.Unreadable.Add(("TAG2", ScyllaService.GetWeekOrDaysSinceStart("TAG2", rows[1].End)));

        Assert.That(await audit.Run(), Is.EqualTo(2), "the pass does not fail");

        var result = Result(Sep);
        Assert.That(result.CompletedAt, Is.Not.Null);
        Assert.That((Count(result, AuditClass.Equal), Count(result, AuditClass.Unreadable)), Is.EqualTo((1L, 1L)));
        var failed = result.Partitions.Single(p => p.Unreadable);
        Assert.That((failed.Tag, failed.Rows, failed.Error), Is.EqualTo(("TAG2", 0, failed.Error)));
        Assert.That(failed.Error, Is.Not.Empty);
        Assert.That(markers.Values[ScyllaArchiveMarkerKeys.Audit(Sep)], Does.StartWith("unreadable:1:"));
        Assert.That(result.Samples[AuditClass.Unreadable].Single().Tag, Is.EqualTo("TAG2"));

        audit.Unreadable.Clear();
        audit.Fetches.Clear();
        Assert.That(await audit.Run(), Is.EqualTo(1), "Sep became clean");

        result = Result(Sep);
        Assert.That(result.ClassCounts.Keys, Is.EquivalentTo(new[] { AuditClass.Equal }));
        Assert.That(Count(result, AuditClass.Equal), Is.EqualTo(2));
        Assert.That(result.Partitions.Count, Is.EqualTo(2));
        Assert.That(result.Samples.ContainsKey(AuditClass.Unreadable) ? result.Samples[AuditClass.Unreadable] : new List<ScyllaAuditSample>(), Is.Empty);
        Assert.That(audit.Fetches.Select(f => f.tag).Distinct(), Is.EqualTo(new[] { "TAG2" }), "only the unreadable partition was read again");
        Assert.That(markers.Values[ScyllaArchiveMarkerKeys.Audit(Sep)], Does.StartWith("done:"));
    }

    private static ScyllaAuction Clone(ScyllaAuction r)
        => (ScyllaAuction)r.GetType().GetMethod("MemberwiseClone", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic).Invoke(r, null);
}
