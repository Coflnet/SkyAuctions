using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Coflnet.Sky.Auctions.Models;
using System;
using System.Linq;
using Microsoft.EntityFrameworkCore;
using System.Collections;
using System.Collections.Generic;
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
        return await scyllaService.GetAuction(Guid.Parse(uuid));
    }
    [HttpPost]
    [Route("/import/offset")]
    public async Task SetOffset(int id)
    {
        await SellsCollector.SetOffset(id);
    }
}
