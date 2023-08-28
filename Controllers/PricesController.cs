using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Coflnet.Sky.Auctions.Models;
using System.Collections.Generic;
using Coflnet.Sky.Core;

namespace Coflnet.Sky.Auctions.Controllers;

[ApiController]
[Route("/api/[controller]")]
public class PricesController : ControllerBase
{
    private readonly ScyllaService scyllaService;

    public PricesController(ScyllaService scyllaService)
    {
        this.scyllaService = scyllaService;
    }

    /// <summary>
    /// Aggregated sumary of item prices for the 3 last days
    /// </summary>
    /// <param name="itemTag">The item tag you want prices for</param>
    /// <param name="query">Filter query</param>
    /// <returns></returns>
    [Route("item/price/{itemTag}")]
    [HttpGet]
    [ResponseCache(Duration = 1800, Location = ResponseCacheLocation.Any, NoStore = false, VaryByQueryKeys = new string[] { "*" })]
    public Task<PriceSumary> GetSumary(string itemTag, [FromQuery] IDictionary<string, string> query)
    {
        return scyllaService.GetSumary(itemTag, new Dictionary<string, string>(query));
    }

    [Route("item/price/{itemTag}/history")]
    [HttpGet]
    [ResponseCache(Duration = 180, Location = ResponseCacheLocation.Any, NoStore = false, VaryByQueryKeys = new string[] { "*" })]
    public Task<List<SaveAuction>> GetHistory(string itemTag, [FromQuery] IDictionary<string, string> query)
    {
        return scyllaService.GetRecentBatch(itemTag);
    }
}