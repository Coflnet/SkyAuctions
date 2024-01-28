using System;
using System.Linq;
using System.Threading.Tasks;
using Cassandra.Data.Linq;
using Coflnet.Sky.Core;
using Microsoft.EntityFrameworkCore;
using Newtonsoft.Json;

namespace Coflnet.Sky.Auctions;

public class RestoreService
{
    private readonly ScyllaService scyllaService;

    private readonly HypixelContext context;

    public RestoreService(ScyllaService scyllaService, HypixelContext context)
    {
        this.scyllaService = scyllaService;
        this.context = context;
    }

    /// <summary>
    /// Restores an auction from the scylla database
    /// </summary>
    /// <param name="auctionid"></param>
    /// <returns></returns>
    public async Task<SaveAuction> RestoreAuction(Guid auctionid)
    {
        var auction = await scyllaService.GetCombinedAuction(auctionid);
        await context.Auctions.AddAsync(auction);
        await context.SaveChangesAsync();
        return auction;
    }

    public async Task RemoveAuction(Guid auctionid)
    {
        var uid = AuctionService.Instance.GetId(auctionid.ToString("N"));
        var archivedVersionTask = scyllaService.GetCombinedAuction(auctionid);
        var auction = await context.Auctions.Where(a => a.UId == uid)
                        .Include(a => a.Enchantments)
                        .Include(a => a.NbtData)
                        .Include(a => a.NBTLookup)
                        .Include(a => a.CoopMembers)
                        .Include(a => a.Bids)
                        .FirstAsync();
        if (auction == null)
            return; // already deleted
        var archivedVersion = JsonConvert.SerializeObject(await archivedVersionTask);
        var toBeDeleted = JsonConvert.SerializeObject(auction);
        if (archivedVersion != toBeDeleted)
        {
            throw new CoflnetException("no_match", "Archived version does not match to be deleted version");
        }

        context.Auctions.Remove(auction);
        await context.SaveChangesAsync();
    }

    /// <summary>
    /// Restores all auctions from a player between two dates in batches of 500
    /// </summary>
    /// <param name="playerUuid"></param>
    /// <param name="end"></param>
    /// <returns></returns>
    public async Task RestoreAuctionsFromPlayer(Guid playerUuid, DateTime end)
    {
        var auctions = await scyllaService.GetRecentFromPlayer(playerUuid, end, 500);
        foreach (var item in auctions.GroupBy(a => a.Uuid).Select(g => scyllaService.CombineVersions(g.ToArray())))
        {
            context.Auctions.Add(item);
        }
        await context.SaveChangesAsync();
    }
}