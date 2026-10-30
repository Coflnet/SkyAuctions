using System;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using Microsoft.AspNetCore.Mvc;

namespace Coflnet.Sky.Auctions.Controllers;

/// <summary>Checks the required CQL connection without changing the existing readiness probe.</summary>
[ApiController]
public class ConnectionController(ISession session) : ControllerBase
{
    /// <summary>Performs a bounded read of the local CQL system table.</summary>
    [HttpGet("/health/ready")]
    public async Task<IActionResult> Ready(CancellationToken cancellationToken)
    {
        try
        {
            await session.ExecuteAsync(new SimpleStatement("SELECT key FROM system.local")
                .SetReadTimeoutMillis(5000)).WaitAsync(TimeSpan.FromSeconds(5), cancellationToken);
            return Ok("ready");
        }
        catch (Exception)
        {
            return StatusCode(503, "not ready");
        }
    }
}
