using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.Linq;
using Microsoft.EntityFrameworkCore;
using System.Collections;
using Coflnet.Sky.Auctions.Services;
using Coflnet.Sky.Core;

namespace Coflnet.Sky.Auctions.Controllers;

/// <summary>
/// Main Controller handling tracking
/// </summary>
[ApiController]
[Route("/api/[controller]")]
public class AuctionController : ControllerBase
{
    private readonly ScyllaService scyllaService;

    /// <summary>
    /// Creates a new instance of <see cref="AuctionController"/>
    /// </summary>
    /// <param name="service"></param>
    public AuctionController(ScyllaService service)
    {
        this.scyllaService = service;
    }

    /// <summary>
    /// Tracks a flip
    /// </summary>
    /// <param name="uuid"></param>
    /// <returns></returns>
    [HttpPost]
    [Route("{uuid}")]
    public async Task<SaveAuction> TrackFlip(string uuid)
    {
        var auctions = await scyllaService.GetAuction(Guid.Parse(uuid));
        Response.Headers.Add("X-Total-Count", auctions.Length.ToString());
        return auctions.First();
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
}
