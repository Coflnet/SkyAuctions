using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using System;

namespace Coflnet.Sky.Auctions.Controllers;

/// <summary>
/// Handles restoring of auctions
/// </summary>
[ApiController]
[Route("/api/[controller]")]
public class RestoreController : ControllerBase
{
    private readonly RestoreService restoreService;

    public RestoreController(RestoreService restoreService)
    {
        this.restoreService = restoreService;
    }
    /// <summary>
    /// Restores a single auction
    /// </summary>
    /// <param name="uuid"></param>
    /// <returns></returns>
    [HttpPost]
    [Route("{uuid}")]
    public async Task RestoreAuction(string uuid)
    {
        await restoreService.RestoreAuction(Guid.Parse(uuid));
    }

    /// <summary>
    /// Removes an auction from the main database
    /// </summary>
    /// <param name="uuid"></param>
    [HttpDelete]
    [Route("{uuid}")]
    public async Task RemoveAuction(string uuid)
    {
        await restoreService.RemoveAuction(Guid.Parse(uuid));
    }
}
