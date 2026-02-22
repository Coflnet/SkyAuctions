using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Coflnet.Sky.Auctions.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace Coflnet.Sky.Auctions.Controllers;

#nullable enable

/// <summary>
/// Controller for S3 archive operations and migration management
/// </summary>
[ApiController]
[Route("api/[controller]")]
public class ArchiveController : ControllerBase
{
    private readonly S3AuctionStorage? _s3Storage;
    private readonly S3MigrationService? _migrationService;
    private readonly ScyllaService _scyllaService;
    private readonly ILogger<ArchiveController> _logger;

    /// <summary>
    /// Creates a new ArchiveController
    /// </summary>
    public ArchiveController(
        ILogger<ArchiveController> logger,
        ScyllaService scyllaService,
        S3AuctionStorage? s3Storage = null,
        S3MigrationService? migrationService = null)
    {
        _logger = logger;
        _scyllaService = scyllaService;
        _s3Storage = s3Storage;
        _migrationService = migrationService;
    }

    /// <summary>
    /// Gets the status of S3 archive storage
    /// </summary>
    [HttpGet("status")]
    public ActionResult<ArchiveStatusResponse> GetStatus()
    {
        return Ok(new ArchiveStatusResponse
        {
            S3Enabled = _s3Storage != null,
            MigrationServiceEnabled = _migrationService != null
        });
    }

    /// <summary>
    /// Gets all months that have archived data for a specific item tag
    /// </summary>
    [HttpGet("{tag}/months")]
    public async Task<ActionResult<List<MonthInfo>>> GetArchivedMonths(string tag)
    {
        if (_s3Storage == null)
            return BadRequest("S3 storage is not enabled");

        var months = await _s3Storage.GetMonthsWithData(tag);
        return Ok(months.Select(m => new MonthInfo { Year = m.year, Month = m.month }).ToList());
    }

    /// <summary>
    /// Gets archived auctions for a specific item tag and month
    /// </summary>
    [HttpGet("{tag}/{year}/{month}")]
    public async Task<ActionResult<List<CassandraAuction>>> GetArchivedAuctions(string tag, int year, int month)
    {
        if (_s3Storage == null)
            return BadRequest("S3 storage is not enabled");

        var auctions = await _s3Storage.GetAuctions(tag, year, month);
        return Ok(auctions);
    }

    /// <summary>
    /// Manually triggers migration for a specific tag and date range (dry-run by default)
    /// </summary>
    [HttpPost("migrate")]
    public async Task<ActionResult> TriggerMigration([FromBody] MigrationRequest request)
    {
        if (_migrationService == null)
            return BadRequest("Migration service is not enabled");

        if (string.IsNullOrEmpty(request.Tag))
            return BadRequest("Tag is required");

        _logger.LogInformation("Manual migration triggered for {Tag} from {Start} to {End} (DryRun: {DryRun})",
            request.Tag, request.StartDate, request.EndDate, request.DryRun);

        await _migrationService.MigrateManual(
            request.Tag,
            request.StartDate ?? DateTime.UtcNow.AddYears(-5),
            request.EndDate ?? DateTime.UtcNow.AddMonths(-3),
            request.DryRun);

        return Ok(new { message = "Migration started" });
    }

    /// <summary>
    /// Checks if a specific auction exists in the archive
    /// </summary>
    [HttpGet("auction/{uuid}")]
    public async Task<ActionResult<AuctionLookupResponse>> LookupAuction(Guid uuid)
    {
        if (_s3Storage == null)
            return BadRequest("S3 storage is not enabled");

        var (mayExist, tag, year, month) = await _s3Storage.MayContainAuction(uuid);

        if (!mayExist)
        {
            return Ok(new AuctionLookupResponse
            {
                Found = false,
                Message = "Auction not found in bloom filter index"
            });
        }

        // Try to find the actual auction
        var auction = await _s3Storage.GetAuctionByUuid(uuid);
        if (auction != null)
        {
            return Ok(new AuctionLookupResponse
            {
                Found = true,
                Tag = auction.Tag,
                Year = auction.End.Year,
                Month = auction.End.Month,
                Auction = ScyllaService.CassandraToOld(auction)
            });
        }

        return Ok(new AuctionLookupResponse
        {
            Found = false,
            Message = "Auction may exist (bloom filter positive) but not found in archive"
        });
    }
}

/// <summary>
/// Status response for archive storage
/// </summary>
public class ArchiveStatusResponse
{
    /// <summary>Whether S3 storage is enabled</summary>
    public bool S3Enabled { get; set; }
    /// <summary>Whether migration service is enabled</summary>
    public bool MigrationServiceEnabled { get; set; }
}

/// <summary>
/// Month information
/// </summary>
public class MonthInfo
{
    /// <summary>Year</summary>
    public int Year { get; set; }
    /// <summary>Month (1-12)</summary>
    public int Month { get; set; }
}

/// <summary>
/// Request to trigger a migration
/// </summary>
public class MigrationRequest
{
    /// <summary>Item tag to migrate</summary>
    public string Tag { get; set; } = string.Empty;
    /// <summary>Start date of the migration range</summary>
    public DateTime? StartDate { get; set; }
    /// <summary>End date of the migration range</summary>
    public DateTime? EndDate { get; set; }
    /// <summary>Whether to run in dry-run mode (default true)</summary>
    public bool DryRun { get; set; } = true;
}

/// <summary>
/// Response for auction lookup
/// </summary>
public class AuctionLookupResponse
{
    /// <summary>Whether the auction was found</summary>
    public bool Found { get; set; }
    /// <summary>Item tag if found</summary>
    public string? Tag { get; set; }
    /// <summary>Year if found</summary>
    public int? Year { get; set; }
    /// <summary>Month if found</summary>
    public int? Month { get; set; }
    /// <summary>Status message</summary>
    public string? Message { get; set; }
    /// <summary>The auction data if found</summary>
    public Coflnet.Sky.Core.SaveAuction? Auction { get; set; }
}
