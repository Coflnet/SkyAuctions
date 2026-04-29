using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Linq;
using Microsoft.EntityFrameworkCore;
using Coflnet.Sky.Auctions.Services;
using Coflnet.Sky.Core;
using System.Collections.Generic;

namespace Coflnet.Sky.Auctions.Controllers;

/// <summary>
/// Main Controller handling tracking
/// </summary>
[ApiController]
[Route("/api/[controller]")]
public class AuctionController : ControllerBase
{
    private readonly ScyllaService scyllaService;
    private readonly QueryService queryService;
    private readonly UuidLookupService uuidLookup;

    /// <summary>
    /// Creates a new instance of <see cref="AuctionController"/>
    /// </summary>
    /// <param name="service"></param>
    public AuctionController(ScyllaService service, QueryService queryService, UuidLookupService uuidLookup)
    {
        this.scyllaService = service;
        this.queryService = queryService;
        this.uuidLookup = uuidLookup;
    }

    /// <summary>
    /// Tracks a flip
    /// </summary>
    /// <param name="uuid"></param>
    /// <returns></returns>
    [HttpPost]
    [Route("{uuid}")]
    public async Task<SaveAuction[]> GetAuctions(string uuid)
    {
        var guid = Guid.Parse(uuid);
        var auctions = await scyllaService.GetAuction(guid);
        if (auctions.Length > 0)
        {
            return auctions;
        }

        var resolved = await uuidLookup.ResolveAuction(guid);
        return resolved == null ? Array.Empty<SaveAuction>() : new[] { resolved };
    }
    /// <summary>
    /// Gets legacy saveauction by uuid
    /// </summary>
    /// <param name="uuid"></param>
    /// <returns></returns>
    [HttpGet]
    [Route("{uuid}")]
    public async Task<SaveAuction> GetAuction(string uuid)
    {
        var guid = Guid.Parse(uuid);
        // Try Scylla first (fast path)
        var auctions = await scyllaService.GetAuction(guid);
        if (auctions.Length > 0)
        {
            Response.Headers["X-Total-Count"] = auctions.Length.ToString();
            return scyllaService.CombineVersions(auctions);
        }

        // Fall back to tiered resolution (MariaDB → S3)
        var result = await uuidLookup.ResolveAuction(guid);
        if (result != null)
        {
            Response.Headers["X-Total-Count"] = "1";
            return result;
        }

        Response.StatusCode = 404;
        return null;
    }
    /// <summary>
    /// Recently sold auctions for a specific item
    /// </summary>
    /// <param name="id"></param>
    /// <returns></returns>
    [HttpGet]
    [Route("/api/auctions/tag/{itemTag}/recent/overview")]
    public async Task<IEnumerable<ItemPrices.AuctionPreview>> GetRecentOverview(string itemTag, [FromQuery] Dictionary<string, string> query)
    {
        return await queryService.GetRecentOverview(itemTag, query);
    }
    [HttpPost]
    [Route("/import/offset")]
    public async Task SetOffset(int id)
    {
        await SellsCollector.SetOffset(id);
    }
    [HttpPost]
    [Route("/import/migrate")]
    public async Task Migrate(int id)
    {
        using var context = new HypixelContext();
        var auction = await context.Auctions.Where(a => a.Id == id)
                .Include(a => a.Bids).Include(a => a.Enchantments).Include(a => a.NbtData).Include(a => a.NBTLookup).Include(a => a.CoopMembers)
                .FirstAsync();
        await scyllaService.InsertAuction(auction);
    }
    [HttpGet("/api/auctions/timeKey")]
    public int GetTimeKey(DateTime targetDate, string tag = "any")
    {
        return ScyllaService.GetWeekOrDaysSinceStart(tag, targetDate);
    }

    [HttpGet]
    [Route("/api/player/{uuid}/auctions")]
    public async Task<IEnumerable<SaveAuction>> GetPlayerAuctions(string uuid, DateTime end, int amount = 10)
    {
        if (end == default)
            end = DateTime.UtcNow;
        return await scyllaService.GetRecentFromPlayer(Guid.Parse(uuid), end, amount);
    }
}
