using System;
using System.Linq;
using System.Threading.Tasks;
using Coflnet.Sky.Auctions.Archive;
using Coflnet.Sky.Core;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Coflnet.Sky.Auctions.Services;

/// <summary>
/// Tiered UUID resolution: MariaDB → ScyllaDB (and its legacy S3 v1 fallback) → R2 archive v2 (last tier, once the live rows are deleted)
/// </summary>
public class UuidLookupService
{
    private readonly IServiceScopeFactory scopeFactory;
    private readonly ScyllaService scyllaService;
    private readonly ILogger<UuidLookupService> logger;
    private readonly ArchiveReadService archive;

    /// <summary>
    /// Creates a new UuidLookupService
    /// </summary>
    public UuidLookupService(
        IServiceScopeFactory scopeFactory,
        ScyllaService scyllaService,
        ILogger<UuidLookupService> logger,
        ArchiveReadService archive = null)
    {
        this.scopeFactory = scopeFactory;
        this.scyllaService = scyllaService;
        this.logger = logger;
        this.archive = archive;
    }

    /// <summary>
    /// Resolves an auction by UUID, checking multiple storage tiers
    /// </summary>
    /// <param name="uuid">The auction UUID</param>
    /// <returns>The auction if found, null otherwise</returns>
    public async Task<SaveAuction> ResolveAuction(Guid uuid)
    {
        // 1. MariaDB (recent data)
        try
        {
            var dbAuction = await FindInMariaDb(uuid);
            if (dbAuction != null)
                return dbAuction;
        }
        catch (Exception e)
        {
            logger.LogWarning(e, "MariaDB lookup failed for {Uuid}", uuid);
        }

        // 2. ScyllaDB and its S3 fallback.
        try
        {
            var scyllaResult = await FindInScylla(uuid);
            if (scyllaResult != null)
                return scyllaResult;
        }
        catch (Exception e)
        {
            logger.LogWarning(e, "Scylla lookup failed for {Uuid}", uuid);
        }

        // 3. R2 archive v2 (auction_locations -> block), the only copy once old rows are deleted from MariaDB and Scylla
        if (archive != null)
        {
            try
            {
                var archived = await archive.FindAuctionAsync(uuid);
                if (archived != null)
                    return archived;
            }
            catch (Exception e)
            {
                logger.LogWarning(e, "Archive v2 lookup failed for {Uuid}", uuid);
            }
        }

        logger.LogInformation("UUID {Uuid} not found in MariaDB, ScyllaDB, or the archive", uuid);
        return null;
    }

    /// <summary>MariaDB tier (a seam for tests).</summary>
    protected virtual async Task<SaveAuction> FindInMariaDb(Guid uuid)
    {
        using var scope = scopeFactory.CreateScope();
        using var context = scope.ServiceProvider.GetRequiredService<HypixelContext>();
        var uid = AuctionService.Instance.GetId(uuid.ToString("N"));
        return await context.Auctions
            .Where(a => a.UId == uid)
            .Include(a => a.Bids)
            .Include(a => a.Enchantments)
            .Include(a => a.NbtData)
            .Include(a => a.NBTLookup)
            .Include(a => a.CoopMembers)
            .AsNoTracking()
            .FirstOrDefaultAsync();
    }

    /// <summary>Scylla tier (a seam for tests), null when Scylla has no row.</summary>
    protected virtual async Task<SaveAuction> FindInScylla(Guid uuid)
    {
        var scyllaResult = await scyllaService.GetAuction(uuid);
        return scyllaResult.Length > 0 ? scyllaService.CombineVersions(scyllaResult) : null;
    }
}
