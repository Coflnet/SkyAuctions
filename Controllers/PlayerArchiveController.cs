using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3;
using Coflnet.Sky.Auctions.Archive;
using Coflnet.Sky.Auctions.Models;
using Coflnet.Sky.Auctions.Services;
using Coflnet.Sky.Core;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Coflnet.Sky.Auctions.Controllers;

/// <summary>
/// Controller for retrieving archived player auction data from S3
/// </summary>
[ApiController]
[Route("api/player/archive")]
public class PlayerArchiveController : ControllerBase
{
    private readonly S3PlayerIndexService playerIndex;
    private readonly S3AuctionBlobService auctionBlobs;
    private readonly BloomFilterService bloomFilter;
    private readonly ScyllaService scyllaService;
    private readonly ILogger<PlayerArchiveController> logger;
    private readonly int monthsInScylla;
    private readonly ArchiveReadService archiveV2;

    /// <summary>
    /// Initializes a new instance of the PlayerArchiveController
    /// </summary>
    public PlayerArchiveController(
        ScyllaService scyllaService,
        IConfiguration config,
        ILogger<PlayerArchiveController> logger,
        S3PlayerIndexService playerIndex = null,
        S3AuctionBlobService auctionBlobs = null,
        BloomFilterService bloomFilter = null,
        ArchiveReadService archiveV2 = null)
    {
        this.archiveV2 = archiveV2;
        this.playerIndex = playerIndex;
        this.auctionBlobs = auctionBlobs;
        this.bloomFilter = bloomFilter;
        this.scyllaService = scyllaService;
        this.logger = logger;
        monthsInScylla = config.GetValue<int?>("S3_MIGRATION:MONTHS_TO_KEEP_IN_SCYLLA")
            ?? config.GetValue<int?>("S3:MonthsToKeepInScylla")
            ?? 3;
    }

    /// <summary>
    /// Gets the participation summary for a player (what they sold/bought) in a specific year
    /// </summary>
    /// <param name="playerUuid">The player UUID (with or without dashes)</param>
    /// <param name="year">The year to query (e.g., 2024)</param>
    /// <param name="ct">Cancellation token</param>
    /// <returns>List of auction participations</returns>
    [HttpGet("{playerUuid}/participation/{year:int}")]
    [ProducesResponseType(typeof(List<PlayerParticipationEntry>), 200)]
    [ProducesResponseType(404)]
    [ProducesResponseType(503)]
    public async Task<ActionResult<List<PlayerParticipationEntry>>> GetParticipation(
        string playerUuid,
        int year,
        CancellationToken ct = default)
    {
        if (playerIndex == null)
            return StatusCode(503, "S3 player index not enabled");

        if (!Guid.TryParse(playerUuid.Replace("-", ""), out var guid))
            return BadRequest("Invalid player UUID");

        try
        {
            var entries = await playerIndex.GetParticipation(guid, year, ct);
            return Ok(entries);
        }
        catch (Amazon.S3.AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return NotFound($"No participation data found for player {playerUuid} in year {year}");
        }
    }

    /// <summary>
    /// Gets the full auction details for auctions a player participated in
    /// </summary>
    /// <param name="playerUuid">The player UUID</param>
    /// <param name="year">The year to query</param>
    /// <param name="type">Filter by participation type (0=Seller, 1=Bidder, null=both)</param>
    /// <param name="tag">Filter by item tag (optional)</param>
    /// <param name="limit">Maximum auctions to return (default 100)</param>
    /// <param name="ct">Cancellation token</param>
    [HttpGet("{playerUuid}/auctions/{year:int}")]
    [ProducesResponseType(typeof(List<SaveAuction>), 200)]
    [ProducesResponseType(503)]
    public async Task<ActionResult<List<SaveAuction>>> GetPlayerAuctions(
        string playerUuid,
        int year,
        [FromQuery] ParticipationType? type = null,
        [FromQuery] string tag = null,
        [FromQuery] int limit = 100,
        CancellationToken ct = default)
    {
        if (playerIndex == null || auctionBlobs == null)
            return StatusCode(503, "S3 archive not enabled");

        if (!Guid.TryParse(playerUuid.Replace("-", ""), out var guid))
            return BadRequest("Invalid player UUID");

        try
        {
            var participations = await playerIndex.GetParticipation(guid, year, ct);

            // Apply filters
            if (type.HasValue)
                participations = participations.Where(p => p.Type == type.Value).ToList();
            if (!string.IsNullOrEmpty(tag))
                participations = participations.Where(p => p.Tag == tag).ToList();

            // Group by (tag, month) to minimize blob fetches
            var byTagMonth = participations
                .GroupBy(p => (p.Tag, Month: new DateTime(p.End.Year, p.End.Month, 1)))
                .Take(limit); // Limit groups to prevent excessive reads

            var results = new List<SaveAuction>();
            var targetUuids = participations.Take(limit).Select(p => p.AuctionUid).ToHashSet();

            foreach (var group in byTagMonth)
            {
                if (results.Count >= limit)
                    break;

                try
                {
                    var auctions = await auctionBlobs.ReadAuctions(group.Key.Tag, group.Key.Month, ct);
                    var matching = auctions.Where(a => AuctionIdentity.TryParseGuid(a.Uuid, out var auctionGuid) && targetUuids.Contains(auctionGuid));
                    results.AddRange(matching);
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "Failed to read auctions for {Tag} {Month}", group.Key.Tag, group.Key.Month);
                }
            }

            return Ok(results.Take(limit).ToList());
        }
        catch (Amazon.S3.AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return Ok(new List<SaveAuction>());
        }
    }

    /// <summary>
    /// Gets combined auction history from both ScyllaDB (recent) and S3 (archived)
    /// </summary>
    /// <param name="playerUuid">The player UUID</param>
    /// <param name="startYear">Start year (inclusive)</param>
    /// <param name="endYear">End year (inclusive, defaults to current year)</param>
    /// <param name="type">Filter by participation type</param>
    /// <param name="limit">Maximum results</param>
    /// <param name="ct">Cancellation token</param>
    [HttpGet("{playerUuid}/history")]
    [ProducesResponseType(typeof(PlayerAuctionHistory), 200)]
    public async Task<ActionResult<PlayerAuctionHistory>> GetPlayerHistory(
        string playerUuid,
        [FromQuery] int? startYear = null,
        [FromQuery] int? endYear = null,
        [FromQuery] ParticipationType? type = null,
        [FromQuery] int limit = 100,
        CancellationToken ct = default)
    {
        if (!Guid.TryParse(playerUuid.Replace("-", ""), out var guid))
            return BadRequest("Invalid player UUID");

        var currentYear = DateTime.UtcNow.Year;
        startYear ??= 2019; // SkyBlock launch
        endYear ??= currentYear;

        var result = new PlayerAuctionHistory
        {
            PlayerUuid = guid.ToString("N"),
            QueryStartYear = startYear.Value,
            QueryEndYear = endYear.Value
        };

        // 1. Get recent data from ScyllaDB (last 3 months)
        var scyllaEnd = DateTime.UtcNow;
        var scyllaCutoff = scyllaEnd.AddMonths(-monthsInScylla);
        var recentScyllaLoaded = false;
        
        try
        {
            var recentAuctions = await scyllaService.GetRecentFromPlayer(guid, scyllaEnd, limit);
            if (type.HasValue)
            {
                recentAuctions = type.Value == ParticipationType.Seller
                    ? recentAuctions.Where(a => a.AuctioneerId == guid.ToString("N") || a.AuctioneerId == guid.ToString())
                    : recentAuctions.Where(a => a.Bids?.Any(b => b.Bidder == guid.ToString("N") || b.Bidder == guid.ToString()) == true);
            }
            result.RecentAuctions = recentAuctions.ToList();
            result.ScyllaDbCount = result.RecentAuctions.Count;
            recentScyllaLoaded = true;
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to query ScyllaDB for player {Uuid}", playerUuid);
        }

        // 2. Get archived data from S3 (if enabled)
        if (playerIndex != null)
        {
            var allParticipations = new List<PlayerParticipationEntry>();
            for (int year = startYear.Value; year <= endYear.Value && !ct.IsCancellationRequested; year++)
            {
                try
                {
                    var yearParticipations = await playerIndex.GetParticipation(guid, year, ct);
                    if (type.HasValue)
                        yearParticipations = yearParticipations.Where(p => p.Type == type.Value).ToList();
                    allParticipations.AddRange(yearParticipations);
                }
                catch (Amazon.S3.AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    // No data for this year
                }
            }

            var archivedParticipations = allParticipations.AsEnumerable();
            if (recentScyllaLoaded)
            {
                archivedParticipations = archivedParticipations.Where(p => p.End < scyllaCutoff);
            }

            result.ArchivedParticipations = archivedParticipations
                .OrderByDescending(p => p.End)
                .ToList();
            result.S3ArchiveCount = result.ArchivedParticipations.Count;
        }

        // 3. R2 archive v2 (the only copy once old rows are deleted from Scylla), same entry shape, auctions the v1 index lists are not repeated
        if (archiveV2 != null)
            await AddArchiveV2Async(result, guid, type, startYear.Value, endYear.Value, limit, recentScyllaLoaded ? scyllaCutoff : null, ct);

        return Ok(result);
    }

    /// <summary>The [from, to) range of the years, clamped to 2019..next year so out-of-range input cannot overflow a DateTime.</summary>
    internal static (DateTime from, DateTime to) ArchiveYearRange(int startYear, int endYear)
    {
        var max = DateTime.UtcNow.Year + 1;
        var start = Math.Clamp(startYear, 2019, max);
        var end = Math.Clamp(endYear, 2019, max);
        return (new DateTime(start, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(end, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddYears(1));
    }

    /// <summary>Adds the archive v2 participations to <paramref name="result"/>; on timeout or failure the result stays as it is and <c>X-Archive-Warning</c> is set.</summary>
    private async Task AddArchiveV2Async(PlayerAuctionHistory result, Guid guid, ParticipationType? type, int startYear, int endYear, int limit, DateTime? scyllaCutoff, CancellationToken ct)
    {
        var (from, to) = ArchiveYearRange(startYear, endYear);
        var read = await archiveV2.ReadOrFallbackAsync(token => archiveV2.GetPlayerParticipationsAsync(guid, type, from, to, limit, token), new List<PlayerParticipationEntry>(), "player participations", ct);
        if (read.Warning != null && Response != null)
            Response.Headers["X-Archive-Warning"] = read.Warning;
        var archived = scyllaCutoff == null ? read.Value : read.Value.Where(p => p.End < scyllaCutoff).ToList();
        var known = result.ArchivedParticipations.Select(p => (p.AuctionUid, p.Type)).ToHashSet();
        result.ArchivedParticipations = result.ArchivedParticipations
            .Concat(archived.Where(p => !known.Contains((p.AuctionUid, p.Type))))
            .OrderByDescending(p => p.End)
            .ToList();
        result.S3ArchiveCount = result.ArchivedParticipations.Count;
    }

    /// <summary>
    /// Checks if a specific auction exists in the archive
    /// </summary>
    /// <param name="auctionUuid">The auction UUID</param>
    /// <param name="ct">Cancellation token</param>
    [HttpGet("auction/{auctionUuid}/exists")]
    [ProducesResponseType(typeof(AuctionExistsResponse), 200)]
    public ActionResult<AuctionExistsResponse> CheckAuctionExists(string auctionUuid, CancellationToken ct = default)
    {
        if (!Guid.TryParse(auctionUuid.Replace("-", ""), out var guid))
            return BadRequest("Invalid auction UUID");

        var result = new AuctionExistsResponse
        {
            AuctionUuid = guid.ToString("N")
        };

        if (bloomFilter != null)
        {
            result.MayExistInArchive = bloomFilter.MightExist(guid);
            result.BloomFilterAvailable = true;
        }
        else
        {
            result.BloomFilterAvailable = false;
        }

        return Ok(result);
    }
}

/// <summary>
/// Response model for player auction history
/// </summary>
public class PlayerAuctionHistory
{
    /// <summary>The player's UUID</summary>
    public string PlayerUuid { get; set; } = string.Empty;
    
    /// <summary>Start year of the query</summary>
    public int QueryStartYear { get; set; }
    
    /// <summary>End year of the query</summary>
    public int QueryEndYear { get; set; }
    
    /// <summary>Number of auctions found in ScyllaDB</summary>
    public int ScyllaDbCount { get; set; }
    
    /// <summary>Number of participation entries found in S3</summary>
    public int S3ArchiveCount { get; set; }
    
    /// <summary>Recent auctions from ScyllaDB (last 3 months)</summary>
    public List<SaveAuction> RecentAuctions { get; set; } = new();
    
    /// <summary>Archived participation records from S3</summary>
    public List<PlayerParticipationEntry> ArchivedParticipations { get; set; } = new();
}

/// <summary>
/// Response for auction existence check
/// </summary>
public class AuctionExistsResponse
{
    /// <summary>The auction UUID that was checked</summary>
    public string AuctionUuid { get; set; } = string.Empty;
    
    /// <summary>Whether the bloom filter is available</summary>
    public bool BloomFilterAvailable { get; set; }
    
    /// <summary>
    /// Whether the auction may exist in the archive (bloom filter result).
    /// False = definitely not in archive. True = possibly in archive (1% FPR).
    /// </summary>
    public bool MayExistInArchive { get; set; }
}
