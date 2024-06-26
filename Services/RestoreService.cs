using System;
using System.Linq;
using System.Threading.Tasks;
using Cassandra.Data.Linq;
using Coflnet.Sky.Core;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace Coflnet.Sky.Auctions;

public class RestoreService
{
    private readonly ScyllaService scyllaService;
    private readonly ILogger<RestoreService> logger;
    private readonly HypixelContext context;

    public RestoreService(ScyllaService scyllaService, HypixelContext context, ILogger<RestoreService> logger)
    {
        this.scyllaService = scyllaService;
        this.context = context;
        this.logger = logger;
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
        SaveAuction archivedObj = null;
        try
        {
            archivedObj = await archivedVersionTask;
        }
        catch (Exception)
        {
            await scyllaService.InsertAuction(auction);
            throw;
        }
        archivedObj.FlatenedNBT = null;
        var compareAuction = ScyllaService.CassandraToOld(ScyllaService.ToCassandra(auction));
        if(archivedObj.AuctioneerId == archivedObj.Uuid)
        {
            archivedObj.AuctioneerId = compareAuction.AuctioneerId;
            await scyllaService.InsertAuction(archivedObj);
            logger.LogInformation("Fixed auctioneer id for auction {0}", auctionid);
        }
        AdjustForOptimizations(archivedObj);
        AdjustForOptimizations(compareAuction);
        var settings = new JsonSerializerSettings()
        {
            DateTimeZoneHandling = DateTimeZoneHandling.Utc
        };
        var archivedVersion = JsonConvert.SerializeObject(archivedObj);
        var toBeDeleted = JsonConvert.SerializeObject(compareAuction);
        if (toBeDeleted != archivedVersion)
        {
            Console.WriteLine($"Archived version: {archivedVersion}");
            Console.WriteLine($"To be deleted   : {toBeDeleted}");
            await scyllaService.InsertAuction(auction);
            throw new CoflnetException("no_match", "Archived version does not match to be deleted version");
        }

        context.Remove(auction);
        context.RemoveRange(auction.Enchantments);
        if (auction.NbtData != null)
            context.RemoveRange(auction.NbtData);
        context.RemoveRange(auction.NBTLookup);
        context.RemoveRange(auction.CoopMembers);
        context.RemoveRange(auction.Bids);
        await context.SaveChangesAsync();

        static void AdjustForOptimizations(SaveAuction archivedObj)
        {
            if (archivedObj.ProfileId == archivedObj.AuctioneerId)
                archivedObj.ProfileId = null;
            foreach (var bid in archivedObj.Bids)
            {
                if (bid.ProfileId == bid.Bidder)
                    bid.ProfileId = null;
                // set to be utc don't change timezone 
                if (bid.Timestamp.Kind != DateTimeKind.Utc)
                {
                    // get offset of current locale and revert it
                    bid.Timestamp = bid.Timestamp.AddHours(-TimeZoneInfo.Local.GetUtcOffset(bid.Timestamp).Hours);
                    bid.Timestamp = DateTime.SpecifyKind(bid.Timestamp, DateTimeKind.Utc);
                }
            }
            archivedObj.Bids = archivedObj.Bids.OrderBy(b => b.Amount).ToList();
            archivedObj.Enchantments = archivedObj.Enchantments.OrderBy(e => e.Type).ToList();
            archivedObj.FlatenedNBT = archivedObj.FlatenedNBT.OrderBy(n => n.Key).ToDictionary(n => n.Key, n => n.Value);
        }
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