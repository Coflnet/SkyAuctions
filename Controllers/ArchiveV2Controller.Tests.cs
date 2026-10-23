using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.Auctions.Archive;
using Coflnet.Sky.Core;
using Coflnet.Sky.Core.Archive;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging.Abstractions;
using NUnit.Framework;

namespace Coflnet.Sky.Auctions.Controllers;

public class ArchiveV2ControllerTests
{
    private const string Hyperion = "HYPERION";
    private const ushort HyperionId = 3;
    private const int Sep = 202009;
    private static readonly DateTime SepStart = new(2020, 9, 1, 0, 0, 0, DateTimeKind.Utc);

    private InMemoryAuctionBlobStore store;
    private FakeLookup lookup;
    private ArchiveV2Controller controller;

    private class FakeLookup : IArchiveLocationLookup
    {
        public readonly Dictionary<long, AuctionLocation> Locations = new();
        public readonly Dictionary<(string, int), ArchivedMonth> Months = new();
        public readonly List<AuctionBidder> Bidders = new();

        public Task<AuctionLocation> GetLocationAsync(long uid, CancellationToken cancellationToken = default)
            => Task.FromResult(Locations.GetValueOrDefault(uid));

        public Task<IReadOnlyDictionary<long, AuctionLocation>> GetLocationsAsync(IReadOnlyCollection<long> uids, CancellationToken cancellationToken = default)
            => Task.FromResult<IReadOnlyDictionary<long, AuctionLocation>>(uids.Where(Locations.ContainsKey).Distinct().ToDictionary(u => u, u => Locations[u]));

        public Task<string> GetTagNameAsync(ushort tagId, CancellationToken cancellationToken = default)
            => Task.FromResult(tagId == HyperionId ? Hyperion : null);

        public Task<IReadOnlyList<AuctionBidder>> GetBidderRowsAsync(uint bidderId, uint fromUnixSeconds, uint toUnixSeconds, int limit, CancellationToken cancellationToken = default)
            => Task.FromResult<IReadOnlyList<AuctionBidder>>(Bidders.Where(b => b.BidderId == bidderId && b.End >= fromUnixSeconds && b.End < toUnixSeconds)
                .OrderByDescending(b => b.End).Take(limit).ToList());

        public Task<ArchivedMonth> GetArchivedMonthAsync(string tag, int month, CancellationToken cancellationToken = default)
            => Task.FromResult(Months.GetValueOrDefault((tag, month)));
    }

    [SetUp]
    public void SetUp()
    {
        store = new InMemoryAuctionBlobStore();
        lookup = new FakeLookup();
        var reader = new ArchivedAuctionReader(store, lookup, NullLogger.Instance);
        controller = new ArchiveV2Controller(new ArchiveReadService(reader))
        {
            ControllerContext = new ControllerContext { HttpContext = new DefaultHttpContext() },
        };
        controller.HttpContext.Response.Body = new MemoryStream();
    }

    private static string Uuid(long uid) => uid.ToString("x32");

    private static SaveAuction Row(long uid, int day, long highestBid, bool bin = false) => new()
    {
        Uuid = Uuid(uid),
        UId = uid,
        Tag = Hyperion,
        ItemName = uid == 2 ? "Hyperion, \"Heroic\"" : "Hyperion",
        End = SepStart.AddDays(day),
        Start = SepStart.AddDays(day - 1),
        AuctioneerId = Uuid(1000 + uid),
        StartingBid = 100,
        HighestBidAmount = highestBid,
        Bin = bin,
        Count = 1,
        Tier = Tier.LEGENDARY,
        Category = Category.WEAPON,
        ItemCreatedAt = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc),
        NbtData = new NbtData { data = new byte[] { 1, 2, 3, (byte)uid } },
        Bids = highestBid > 0 ? new List<SaveBids> { new() { Bidder = Uuid(5000), Amount = highestBid, Timestamp = SepStart.AddDays(day - 1).AddHours(1) } } : new List<SaveBids>(),
        Enchantments = new List<Enchantment> { new(Enchantment.EnchantmentType.sharpness, 5) },
        NBTLookup = new List<NBTLookup>(),
    };

    /// <summary>Part 0 (uids 1-4) and an overlapping Scylla part 1 (uids 5-6), with tag index, locations and archived_months row.</summary>
    private void ArchiveSeptember()
    {
        var part0 = ArchivedAuctionBlock.FromAuctions(Hyperion, Sep, 0, ArchivedAuctionBlock.SourceMariaDb, new[]
        {
            Row(1, 1, 500), Row(2, 4, 0, bin: true), Row(3, 8, 900), Row(4, 20, 0),
        });
        var part1 = ArchivedAuctionBlock.FromAuctions(Hyperion, Sep, 1, ArchivedAuctionBlock.SourceScylla, new[]
        {
            (Row(5, 2, 700), new ScyllaAuctionExtras { HighestBidderName = "Alice", NbtLookupStrings = new Dictionary<string, string> { ["modifier"] = "heroic" } }),
            (Row(6, 25, 800, bin: true), new ScyllaAuctionExtras { HighestBidderName = "Bob", NbtLookupStrings = new Dictionary<string, string>() }),
        });
        var blocks = new[] { part0, part1 };
        foreach (var block in blocks)
        {
            store.WriteBlockAsync(block).Wait();
            foreach (var uid in block.AuctionUid)
                lookup.Locations[uid] = new AuctionLocation { Uid = uid, TagId = HyperionId, Month = ArchiveMonths.ToIndex(Sep), Part = (byte)block.Part };
        }
        var hashes = blocks.Select(b => b.Sha256()).ToList();
        store.WriteIndexAsync(new ArchivedTagIndex { Tag = Hyperion }.Upsert(new ArchivedMonthEntry
        {
            Month = Sep, Parts = 2, Rows = 6, SoldRows = 4, PartSha256 = hashes, Source = ArchivedAuctionBlock.SourceMerged,
            VerifiedAt = new DateTime(2026, 10, 20, 0, 0, 0, DateTimeKind.Utc),
        })).Wait();
        lookup.Months[(Hyperion, Sep)] = new ArchivedMonth { Parts = 2, Rows = 6, SoldRows = 4, Sha256 = ArchivedAuctionBlock.CombinedSha256(hashes), Source = ArchivedAuctionBlock.SourceMerged };
    }

    private static T OkValue<T>(ActionResult<T> result)
    {
        Assert.That(result.Result, Is.InstanceOf<OkObjectResult>(), () => $"was {result.Result}");
        return (T)((OkObjectResult)result.Result).Value;
    }

    private static int StatusOf<T>(ActionResult<T> result) => result.Result switch
    {
        ObjectResult o => o.StatusCode ?? 200,
        StatusCodeResult s => s.StatusCode,
        _ => -1,
    };

    private string Body()
    {
        var body = (MemoryStream)controller.HttpContext.Response.Body;
        return Encoding.UTF8.GetString(body.ToArray());
    }

    [Test]
    public async Task MonthsListsTheTagIndex()
    {
        ArchiveSeptember();
        var months = OkValue(await controller.GetMonths(Hyperion));
        Assert.That(months.Count, Is.EqualTo(1));
        Assert.That(months[0].Month, Is.EqualTo("2020-09"));
        Assert.That((months[0].Parts, months[0].Rows, months[0].SoldRows, months[0].Source), Is.EqualTo((2, 6, 4, "merged")));
        Assert.That(months[0].VerifiedAt.Kind, Is.EqualTo(DateTimeKind.Utc));
        Assert.That(StatusOf(await controller.GetMonths("UNKNOWN")), Is.EqualTo(404));
    }

    [Test]
    public async Task FilteredMonthReadWithLimit()
    {
        ArchiveSeptember();
        var sold = OkValue(await controller.GetMonth(Hyperion, "2020-09", sold: true, limit: 3));
        Assert.That(sold.Select(a => a.Uid), Is.EqualTo(new long[] { 1, 5, 3 }), "merged across parts by End, limited");
        Assert.That(sold.All(a => a.ItemBytes == null && a.Bids == null), "no item bytes or bids by default");
        Assert.That(sold.Single(a => a.Uid == 5).HighestBidderName, Is.EqualTo("Alice"));
        Assert.That(sold.Single(a => a.Uid == 5).NbtLookup, Is.EqualTo(new Dictionary<string, string> { ["modifier"] = "heroic" }));

        var newestBin = OkValue(await controller.GetMonth(Hyperion, "202009", bin: true, desc: true, includeItemBytes: true));
        Assert.That(newestBin.Select(a => a.Uid), Is.EqualTo(new long[] { 6, 2 }));
        Assert.That(Convert.FromBase64String(newestBin[0].ItemBytes), Is.EqualTo(new byte[] { 1, 2, 3, 6 }));

        var priced = OkValue(await controller.GetMonth(Hyperion, "2020-09", minPrice: 600, maxPrice: 850, endFrom: SepStart, endTo: SepStart.AddDays(30)));
        Assert.That(priced.Select(a => a.Uid), Is.EqualTo(new long[] { 5, 6 }));

        Assert.That(StatusOf(await controller.GetMonth(Hyperion, "2020-10")), Is.EqualTo(404));
        Assert.That(StatusOf(await controller.GetMonth(Hyperion, "september")), Is.EqualTo(400));
        Assert.That(ArchiveReadService.ClampLimit(100_000, ArchiveReadService.DefaultMonthLimit, ArchiveReadService.MaxMonthLimit), Is.EqualTo(5000));
        Assert.That(ArchiveReadService.ClampLimit(null, ArchiveReadService.DefaultMonthLimit, ArchiveReadService.MaxMonthLimit), Is.EqualTo(200));
    }

    [Test]
    public async Task UnknownUuidIs404()
    {
        ArchiveSeptember();
        Assert.That(StatusOf(await controller.GetAuction(Guid.NewGuid().ToString())), Is.EqualTo(404));
        Assert.That(StatusOf(await controller.GetAuction("nope")), Is.EqualTo(400));
    }

    [Test]
    public async Task AuctionByUuidIncludesBids()
    {
        var uuid = "0f1e2d3c4b5a69788796a5b4c3d2e1f0";
        var uid = AuctionService.Instance.GetId(uuid);
        var row = Row(1, 3, 1234);
        row.Uuid = uuid;
        row.UId = uid;
        store.WriteBlockAsync(ArchivedAuctionBlock.FromAuctions(Hyperion, Sep, 0, ArchivedAuctionBlock.SourceMariaDb, new[] { row })).Wait();
        lookup.Locations[uid] = new AuctionLocation { Uid = uid, TagId = HyperionId, Month = ArchiveMonths.ToIndex(Sep), Part = 0 };

        var dto = OkValue(await controller.GetAuction(Guid.ParseExact(uuid, "N").ToString("D")));

        Assert.That(dto.Uuid, Is.EqualTo(uuid));
        Assert.That(dto.Bids.Select(b => (b.Bidder, b.Amount)), Is.EqualTo(new[] { (Uuid(5000), 1234L) }));
        Assert.That(dto.ItemBytes, Is.Null);
    }

    [Test]
    public async Task CsvExportStreamsHeaderAndRows()
    {
        ArchiveSeptember();
        var result = await controller.Export(Hyperion, "2020-09", "csv", itemName: "heroic");

        Assert.That(result, Is.InstanceOf<EmptyResult>());
        var response = controller.HttpContext.Response;
        Assert.That(response.ContentType, Does.StartWith("text/csv"));
        Assert.That(response.Headers["Content-Disposition"].ToString(), Is.EqualTo("attachment; filename=HYPERION-2020-09.csv"));
        var lines = Body().Split("\r\n");
        Assert.That(lines[0], Is.EqualTo(string.Join(",", ArchiveExport.StandardColumns)));
        Assert.That(lines.Length, Is.EqualTo(3), "header, one row, trailing terminator");
        Assert.That(lines[1], Does.StartWith(Uuid(2) + ",2,HYPERION,\"Hyperion, \"\"Heroic\"\"\",LEGENDARY,WEAPON,true,false,2020-09-04T00:00:00.000Z,2020-09-05T00:00:00.000Z,"));
        Assert.That(lines[2], Is.Empty);
    }

    [Test]
    public async Task JsonLinesExportAndBadRequests()
    {
        ArchiveSeptember();
        var result = await controller.Export(Hyperion, "2020-09", "jsonl", sold: true, limit: 2);
        Assert.That(result, Is.InstanceOf<EmptyResult>());
        Assert.That(controller.HttpContext.Response.ContentType, Does.StartWith("application/x-ndjson"));
        Assert.That(controller.HttpContext.Response.Headers["Content-Disposition"].ToString(), Does.Contain("HYPERION-2020-09.jsonl"));
        var lines = Body().Split('\n', StringSplitOptions.RemoveEmptyEntries);
        Assert.That(lines.Length, Is.EqualTo(2));
        Assert.That(lines[0], Does.StartWith("{\"uuid\":\"" + Uuid(1) + "\""));

        Assert.That(await controller.Export(Hyperion, "2020-09", "xml"), Is.InstanceOf<BadRequestObjectResult>());
        Assert.That(await controller.Export(Hyperion, "2020-10", "csv"), Is.InstanceOf<NotFoundObjectResult>());
    }

    [Test]
    public async Task VerifyReportsAHashMismatchOfACorruptedStore()
    {
        ArchiveSeptember();
        var clean = OkValue(await controller.Verify(Hyperion, "2020-09"));
        Assert.That((clean.Parts, clean.Rows, clean.SoldRows, clean.HashesMatchIndex, clean.HashesMatchDb, clean.FirstDifference), Is.EqualTo((2, 6, 4, true, (bool?)true, (string)null)));

        store.CorruptReadBack = true;
        store.CorruptFromPart = 1;
        var corrupted = OkValue(await controller.Verify(Hyperion, "2020-09"));
        Assert.That(corrupted.HashesMatchIndex, Is.False);
        Assert.That(corrupted.HashesMatchDb, Is.False);
        Assert.That(corrupted.FirstDifference, Does.StartWith("part 1: tag index sha256"));
        Assert.That(store.Events.Where(e => e.StartsWith("read:")).TakeLast(2), Is.EqualTo(new[] { $"read:{Hyperion}:{Sep}:0:uncached", $"read:{Hyperion}:{Sep}:1:uncached" }));
    }

    [Test]
    public async Task PlayerBidsAreNewestFirstWithBids()
    {
        ArchiveSeptember();
        uint End(int day) => (uint)new DateTimeOffset(SepStart.AddDays(day)).ToUnixTimeSeconds();
        lookup.Bidders.Add(new AuctionBidder { BidderId = 9, Uid = 1, End = End(1), Amount = 500 });
        lookup.Bidders.Add(new AuctionBidder { BidderId = 9, Uid = 6, End = End(25), Amount = 800 });

        var bids = OkValue(await controller.GetPlayerBids(9, "2020-09", "2020-09"));
        Assert.That(bids.Select(b => b.Uid), Is.EqualTo(new long[] { 6, 1 }));
        Assert.That(bids.All(b => b.Bids != null));
        Assert.That(OkValue(await controller.GetPlayerBids(9, "2020-10", null)), Is.Empty);
        Assert.That(StatusOf(await controller.GetPlayerBids(9, "bad", null)), Is.EqualTo(400));
    }

    [Test]
    public async Task DisabledArchiveAnswers503()
    {
        var disabled = new ArchiveV2Controller();
        Assert.That(StatusOf(await disabled.GetMonths(Hyperion)), Is.EqualTo(503));
        Assert.That(await disabled.Export(Hyperion, "2020-09"), Is.InstanceOf<ObjectResult>().With.Property("StatusCode").EqualTo(503));
    }
}
