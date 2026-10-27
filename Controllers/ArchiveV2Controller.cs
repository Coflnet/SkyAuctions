using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.Auctions.Archive;
using Coflnet.Sky.Auctions.Services;
using Coflnet.Sky.Core.Archive;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Net.Http.Headers;

namespace Coflnet.Sky.Auctions.Controllers;

/// <summary>
/// Read access to the auction archive v2 (R2 blobs <c>auctions/v2/...</c> plus the MariaDB archive index tables), to verify
/// archived months in production and to read, filter and export them. Read only; answers 503 when <c>S3:ENABLED</c> is false.
/// </summary>
[ApiController]
[Route("api/archive/v2")]
public class ArchiveV2Controller : ControllerBase
{
    private readonly ArchiveReadService service;

    /// <summary>
    /// Creates a new instance of <see cref="ArchiveV2Controller"/>
    /// </summary>
    /// <param name="service">Null when the S3 archive is not enabled</param>
    public ArchiveV2Controller(ArchiveReadService service = null)
    {
        this.service = service;
    }

    /// <summary>
    /// Archived (verified) months of an item tag, from the tag's R2 index
    /// </summary>
    /// <param name="tag">Item tag, e.g. HYPERION</param>
    /// <param name="ct">Cancellation token</param>
    [HttpGet("{tag}/months")]
    [ProducesResponseType(typeof(List<ArchivedMonthSummary>), 200)]
    [ProducesResponseType(404)]
    [ProducesResponseType(503)]
    public async Task<ActionResult<List<ArchivedMonthSummary>>> GetMonths(string tag, CancellationToken ct = default)
    {
        if (service == null)
            return Disabled();
        var months = await service.GetMonthsAsync(tag, ct);
        if (months == null)
            return NotFound($"No archive for {tag}");
        return Ok(months);
    }

    /// <summary>
    /// Archived auctions of a tag that ended in a month, filtered, sorted by end (oldest first unless desc)
    /// </summary>
    /// <param name="tag">Item tag</param>
    /// <param name="month">Month as yyyy-MM (or yyyyMM)</param>
    /// <param name="endFrom">End at or after (UTC)</param>
    /// <param name="endTo">End before (UTC, exclusive)</param>
    /// <param name="sold">Only sold (highest bid above 0) or only unsold auctions</param>
    /// <param name="bin">Only BIN or only bid auctions</param>
    /// <param name="minPrice">Minimum price: highest bid when sold, starting bid otherwise</param>
    /// <param name="maxPrice">Maximum price, same rule as minPrice</param>
    /// <param name="auctioneer">Seller uuid</param>
    /// <param name="itemName">Substring of the item name (case insensitive)</param>
    /// <param name="limit">Rows to return, default 200, at most 5000</param>
    /// <param name="desc">Newest first</param>
    /// <param name="includeItemBytes">Add the raw item bytes (base64)</param>
    /// <param name="ct">Cancellation token</param>
    [HttpGet("{tag}/{month}")]
    [ProducesResponseType(typeof(List<ArchivedAuctionDto>), 200)]
    [ProducesResponseType(400)]
    [ProducesResponseType(404)]
    [ProducesResponseType(503)]
    public async Task<ActionResult<List<ArchivedAuctionDto>>> GetMonth(string tag, string month,
        DateTime? endFrom = null, DateTime? endTo = null, bool? sold = null, bool? bin = null, long? minPrice = null, long? maxPrice = null,
        string auctioneer = null, string itemName = null, int? limit = null, bool desc = false, bool includeItemBytes = false, CancellationToken ct = default)
    {
        if (service == null)
            return Disabled();
        if (!TryParseMonth(month, out var parsed))
            return BadRequest($"Invalid month {month}, expected yyyy-MM");
        var query = Query(endFrom, endTo, sold, bin, minPrice, maxPrice, auctioneer, itemName, limit, desc);
        try
        {
            var rows = await service.ReadMonthAsync(tag, parsed, query, includeItemBytes, ct);
            if (rows == null)
                return NotFound($"{tag} {ArchiveMonths.Format(parsed)} is not archived");
            return Ok(rows);
        }
        catch (InvalidDataException ex)
        {
            return StatusCode(500, ex.Message);
        }
    }

    /// <summary>
    /// Streams the filtered auctions of a tag-month as CSV (RFC 4180) or JSON lines
    /// </summary>
    /// <remarks>Same filters as the month read; limit defaults to and is capped at 100000. Columns: uuid, uid, tag, itemName, tier,
    /// category, bin, sold, start, end, startingBid, highestBidAmount, auctioneer, profileId, highestBidder, count, enchantments,
    /// nbt, coop, bids, source, part (and itemBytes with includeItemBytes).</remarks>
    /// <param name="tag">Item tag</param>
    /// <param name="month">Month as yyyy-MM</param>
    /// <param name="format">csv or jsonl</param>
    /// <param name="endFrom">End at or after (UTC)</param>
    /// <param name="endTo">End before (UTC, exclusive)</param>
    /// <param name="sold">Only sold or only unsold auctions</param>
    /// <param name="bin">Only BIN or only bid auctions</param>
    /// <param name="minPrice">Minimum price: highest bid when sold, starting bid otherwise</param>
    /// <param name="maxPrice">Maximum price</param>
    /// <param name="auctioneer">Seller uuid</param>
    /// <param name="itemName">Substring of the item name (case insensitive)</param>
    /// <param name="limit">Rows to export, at most 100000</param>
    /// <param name="desc">Newest first</param>
    /// <param name="includeItemBytes">Add an itemBytes column (base64)</param>
    /// <param name="ct">Cancellation token</param>
    [HttpGet("{tag}/{month}/export")]
    [Produces("text/csv", "application/x-ndjson")]
    [ProducesResponseType(200)]
    [ProducesResponseType(400)]
    [ProducesResponseType(404)]
    [ProducesResponseType(503)]
    public async Task<IActionResult> Export(string tag, string month, string format = "csv",
        DateTime? endFrom = null, DateTime? endTo = null, bool? sold = null, bool? bin = null, long? minPrice = null, long? maxPrice = null,
        string auctioneer = null, string itemName = null, int? limit = null, bool desc = false, bool includeItemBytes = false, CancellationToken ct = default)
    {
        if (service == null)
            return Disabled();
        if (!TryParseMonth(month, out var parsed))
            return BadRequest($"Invalid month {month}, expected yyyy-MM");
        var jsonl = string.Equals(format, "jsonl", StringComparison.OrdinalIgnoreCase) || string.Equals(format, "ndjson", StringComparison.OrdinalIgnoreCase);
        if (!jsonl && !string.Equals(format, "csv", StringComparison.OrdinalIgnoreCase))
            return BadRequest("format must be csv or jsonl");
        if (!await service.HasMonthAsync(tag, parsed, ct))
            return NotFound($"{tag} {ArchiveMonths.Format(parsed)} is not archived");

        var query = Query(endFrom, endTo, sold, bin, minPrice, maxPrice, auctioneer, itemName, limit, desc);
        var columns = includeItemBytes ? ExportColumns.ItemBytes : ExportColumns.Standard;
        var fileName = $"{SafeFileName(tag)}-{ArchiveMonths.Format(parsed)}.{(jsonl ? "jsonl" : "csv")}";
        Response.ContentType = jsonl ? "application/x-ndjson; charset=utf-8" : "text/csv; charset=utf-8";
        Response.Headers[HeaderNames.ContentDisposition] = new ContentDispositionHeaderValue("attachment") { FileName = fileName }.ToString();
        // the body is written as it is read; an error part way through truncates the download
        await using var writer = new StreamWriter(Response.Body, new UTF8Encoding(false), 64 * 1024, leaveOpen: true);
        var rows = service.ExportMonth(tag, parsed, query, ct);
        if (jsonl)
            await ArchiveExport.WriteJsonLinesAsync(rows, writer, columns, ct);
        else
            await ArchiveExport.WriteCsvAsync(rows, writer, columns, ct);
        return new EmptyResult();
    }

    /// <summary>
    /// One archived auction by uuid, including its bids (same bid shape as /api/auction/{uuid})
    /// </summary>
    /// <param name="uuid">Auction uuid (with or without dashes)</param>
    /// <param name="includeItemBytes">Add the raw item bytes (base64)</param>
    /// <param name="ct">Cancellation token</param>
    [HttpGet("auction/{uuid}")]
    [ProducesResponseType(typeof(ArchivedAuctionDto), 200)]
    [ProducesResponseType(400)]
    [ProducesResponseType(404)]
    [ProducesResponseType(503)]
    public async Task<ActionResult<ArchivedAuctionDto>> GetAuction(string uuid, bool includeItemBytes = false, CancellationToken ct = default)
    {
        if (service == null)
            return Disabled();
        if (!AuctionIdentity.TryParseGuid(uuid, out var guid))
            return BadRequest("Invalid auction uuid");
        var auction = await service.GetAuctionAsync(guid.ToString("N"), includeItemBytes, ct);
        if (auction == null)
            return NotFound();
        return Ok(auction);
    }

    /// <summary>
    /// Archived auctions a player bid on, newest first, including their bids
    /// </summary>
    /// <param name="bidderId">Players.Id of the bidder</param>
    /// <param name="fromMonth">First month (yyyy-MM, inclusive)</param>
    /// <param name="toMonth">Last month (yyyy-MM, inclusive)</param>
    /// <param name="limit">Auctions to return, default 100, at most 1000</param>
    /// <param name="before">Paging cursor <c>endUnixSeconds:uid</c>: only auctions after this position (newest first). A full page sets the response header <c>X-Next-Before</c> to the cursor of the next page</param>
    /// <param name="ct">Cancellation token</param>
    [HttpGet("player/{bidderId:int}/bids")]
    [ProducesResponseType(typeof(List<ArchivedAuctionDto>), 200)]
    [ProducesResponseType(400)]
    [ProducesResponseType(503)]
    public async Task<ActionResult<List<ArchivedAuctionDto>>> GetPlayerBids(int bidderId, string fromMonth = null, string toMonth = null, int? limit = null, string before = null, CancellationToken ct = default)
    {
        if (service == null)
            return Disabled();
        if (bidderId < 0)
            return BadRequest("Invalid bidder id");
        int? from = null, to = null;
        if (fromMonth != null)
        {
            if (!TryParseMonth(fromMonth, out var f))
                return BadRequest($"Invalid fromMonth {fromMonth}, expected yyyy-MM");
            from = f;
        }
        if (toMonth != null)
        {
            if (!TryParseMonth(toMonth, out var t))
                return BadRequest($"Invalid toMonth {toMonth}, expected yyyy-MM");
            to = t;
        }
        AuctionIndexCursor cursor = null;
        if (before != null && !AuctionIndexCursor.TryParse(before, out cursor))
            return BadRequest($"Invalid before {before}, expected endUnixSeconds:uid");
        return OkPage(await service.GetBidsAsync((uint)bidderId, from, to, limit, cursor, ct), limit);
    }

    /// <summary>
    /// Archived auctions a player sold or listed (sold or not), newest first, including their bids
    /// </summary>
    /// <param name="sellerId">Players.Id of the seller</param>
    /// <param name="fromMonth">First month (yyyy-MM, inclusive)</param>
    /// <param name="toMonth">Last month (yyyy-MM, inclusive)</param>
    /// <param name="limit">Auctions to return, default 100, at most 1000</param>
    /// <param name="before">Paging cursor <c>endUnixSeconds:uid</c>: only auctions after this position (newest first). A full page sets the response header <c>X-Next-Before</c> to the cursor of the next page</param>
    /// <param name="ct">Cancellation token</param>
    [HttpGet("player/{sellerId:int}/sells")]
    [ProducesResponseType(typeof(List<ArchivedAuctionDto>), 200)]
    [ProducesResponseType(400)]
    [ProducesResponseType(503)]
    public async Task<ActionResult<List<ArchivedAuctionDto>>> GetPlayerSells(int sellerId, string fromMonth = null, string toMonth = null, int? limit = null, string before = null, CancellationToken ct = default)
    {
        if (service == null)
            return Disabled();
        if (sellerId < 0)
            return BadRequest("Invalid seller id");
        int? from = null, to = null;
        if (fromMonth != null)
        {
            if (!TryParseMonth(fromMonth, out var f))
                return BadRequest($"Invalid fromMonth {fromMonth}, expected yyyy-MM");
            from = f;
        }
        if (toMonth != null)
        {
            if (!TryParseMonth(toMonth, out var t))
                return BadRequest($"Invalid toMonth {toMonth}, expected yyyy-MM");
            to = t;
        }
        AuctionIndexCursor cursor = null;
        if (before != null && !AuctionIndexCursor.TryParse(before, out cursor))
            return BadRequest($"Invalid before {before}, expected endUnixSeconds:uid");
        return OkPage(await service.GetSellsAsync((uint)sellerId, from, to, limit, cursor, ct), limit);
    }

    /// <summary>
    /// Diagnostic: reads every part of a tag-month uncached and compares the recomputed hashes with the tag index and archived_months
    /// </summary>
    /// <param name="tag">Item tag</param>
    /// <param name="month">Month as yyyy-MM</param>
    /// <param name="ct">Cancellation token</param>
    [HttpGet("verify/{tag}/{month}")]
    [ProducesResponseType(typeof(ArchivedMonthVerification), 200)]
    [ProducesResponseType(400)]
    [ProducesResponseType(503)]
    public async Task<ActionResult<ArchivedMonthVerification>> Verify(string tag, string month, CancellationToken ct = default)
    {
        if (service == null)
            return Disabled();
        if (!TryParseMonth(month, out var parsed))
            return BadRequest($"Invalid month {month}, expected yyyy-MM");
        return Ok(await service.VerifyAsync(tag, parsed, ct));
    }

    /// <summary>200 with the page; a full page also carries <c>X-Next-Before</c> (the cursor of its last auction).</summary>
    private ActionResult<List<ArchivedAuctionDto>> OkPage(List<ArchivedAuctionDto> page, int? limit)
    {
        if (page.Count > 0 && page.Count >= ArchiveReadService.ClampLimit(limit, ArchiveReadService.DefaultBidLimit, ArchiveReadService.MaxBidLimit))
        {
            var last = page[^1];
            Response.Headers["X-Next-Before"] = AuctionIndexCursor.After(last.End, last.Uid).ToString();
        }
        return Ok(page);
    }

    private ObjectResult Disabled() => StatusCode(503, "Auction archive (S3) not enabled");

    private static ArchivedAuctionQuery Query(DateTime? endFrom, DateTime? endTo, bool? sold, bool? bin, long? minPrice, long? maxPrice,
        string auctioneer, string itemName, int? limit, bool desc) => new()
    {
        EndFrom = endFrom,
        EndTo = endTo,
        Sold = sold,
        Bin = bin,
        MinPrice = minPrice,
        MaxPrice = maxPrice,
        Auctioneer = string.IsNullOrWhiteSpace(auctioneer) ? null : auctioneer,
        ItemNameContains = string.IsNullOrEmpty(itemName) ? null : itemName,
        Limit = limit,
        Descending = desc,
    };

    private static bool TryParseMonth(string value, out int month)
    {
        try
        {
            month = ArchiveMonths.Parse(value);
            ArchiveMonths.ToIndex(month);
            return true;
        }
        catch (Exception ex) when (ex is FormatException || ex is ArgumentOutOfRangeException)
        {
            month = 0;
            return false;
        }
    }

    private static string SafeFileName(string tag)
        => new string((tag ?? "").Select(c => char.IsAsciiLetterOrDigit(c) || c == '_' || c == '-' ? c : '_').ToArray());
}
