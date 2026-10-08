using System;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.Auctions.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace Coflnet.Sky.Auctions.Controllers;

/// <summary>
/// Internal endpoints to export and erase the data of a single player (GDPR)
/// </summary>
[ApiController]
[Route("api/privacy/player/{uuid}")]
public class PrivacyController : ControllerBase
{
    private readonly PlayerPrivacyService service;

    /// <summary>
    /// Creates the controller, the S3 services are only present when S3 is enabled
    /// </summary>
    public PrivacyController(ScyllaService scyllaService, ILoggerFactory loggerFactory, S3StorageService s3 = null, S3PlayerIndexService playerIndex = null)
    {
        service = new PlayerPrivacyService(new ScyllaPlayerPrivacyStore(scyllaService, s3, playerIndex, loggerFactory.CreateLogger<ScyllaPlayerPrivacyStore>()), loggerFactory.CreateLogger<PlayerPrivacyService>());
    }

    /// <summary>
    /// Exports everything directly addressable for the player, the result is the body for <see cref="Erase"/>
    /// </summary>
    [HttpGet]
    [ProducesResponseType(typeof(PlayerExport), 200)]
    [ProducesResponseType(400)]
    public async Task<ActionResult<PlayerExport>> Export(string uuid)
    {
        if (!TryParse(uuid, out var player))
            return BadRequest("Invalid player UUID");
        return Ok(await service.Export(player, HttpContext.RequestAborted));
    }

    /// <summary>
    /// Erases the player after checking that the body is a still current export. The player has to be opted out.
    /// </summary>
    [HttpPost("erase")]
    [ProducesResponseType(typeof(PlayerEraseResult), 200)]
    [ProducesResponseType(400)]
    [ProducesResponseType(409)]
    public async Task<ActionResult<PlayerEraseResult>> Erase(string uuid, [FromBody] PlayerExport body)
    {
        if (!TryParse(uuid, out var player))
            return BadRequest("Invalid player UUID");
        try
        {
            return Ok(await service.Erase(player, body, HttpContext.RequestAborted));
        }
        catch (PrivacyException e)
        {
            return StatusCode(e.StatusCode, e.Message);
        }
    }

    private static bool TryParse(string uuid, out Guid player) => Guid.TryParse(uuid?.Replace("-", ""), out player);
}
