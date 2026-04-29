using System;
using System.Linq;
using System.Threading.Tasks;
using Coflnet.Sky.Core;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Coflnet.Sky.Auctions.Services;

/// <summary>
/// Tiered UUID resolution: MariaDB → ScyllaDB → S3
/// </summary>
public class UuidLookupService
{
    private readonly IServiceScopeFactory scopeFactory;
    private readonly ScyllaService scyllaService;
    private readonly ILogger<UuidLookupService> logger;

    /// <summary>
    /// Creates a new UuidLookupService
    /// </summary>
    public UuidLookupService(
        IServiceScopeFactory scopeFactory,
        ScyllaService scyllaService,
        ILogger<UuidLookupService> logger)
    {
        this.scopeFactory = scopeFactory;
        this.scyllaService = scyllaService;
        this.logger = logger;
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
            using var scope = scopeFactory.CreateScope();
            using var context = scope.ServiceProvider.GetRequiredService<HypixelContext>();
            var uid = AuctionService.Instance.GetId(uuid.ToString("N"));
            var dbAuction = await context.Auctions
                .Where(a => a.UId == uid)
                .Include(a => a.Bids)
                .Include(a => a.Enchantments)
                .Include(a => a.NbtData)
                .Include(a => a.NBTLookup)
                .Include(a => a.CoopMembers)
                .AsNoTracking()
                .FirstOrDefaultAsync();
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
            var scyllaResult = await scyllaService.GetAuction(uuid);
            if (scyllaResult.Length > 0)
                return scyllaService.CombineVersions(scyllaResult);
        }
        catch (Exception e)
        {
            logger.LogWarning(e, "Scylla lookup failed for {Uuid}", uuid);
        }

        logger.LogInformation("UUID {Uuid} not found in MariaDB, ScyllaDB, or S3", uuid);
        return null;
    }
}
