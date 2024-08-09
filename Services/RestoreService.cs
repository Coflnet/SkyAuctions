using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cassandra.Data.Linq;
using Coflnet.Kafka;
using Coflnet.Sky.Core;
using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace Coflnet.Sky.Auctions;

public class RestoreService
{
    private readonly ScyllaService scyllaService;
    private readonly ILogger<RestoreService> logger;
    private readonly HypixelContext context;
    private readonly IProducer<string, long> deleteProducer;

    public RestoreService(ScyllaService scyllaService, HypixelContext context, ILogger<RestoreService> logger, KafkaCreator kafkaCreator)
    {
        this.scyllaService = scyllaService;
        this.context = context;
        this.logger = logger;
        deleteProducer = kafkaCreator.BuildProducer<string, long>();
        _ = kafkaCreator.CreateTopicIfNotExist("sky-delete-auction", 1);
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

    public async Task RemoveAuction(params Guid[] auctionids)
    {
        var uids = auctionids.Select(id => AuctionService.Instance.GetId(id.ToString("N")));
        var auctions = await context.Auctions.Where(a => uids.Contains(a.UId))
                        .Include(a => a.Enchantments)
                        .Include(a => a.NbtData)
                        .Include(a => a.NBTLookup)
                        .Include(a => a.CoopMembers)
                        .Include(a => a.Bids)
                        .ToListAsync();

        var archivedVersionTask = auctionids.Select(async id =>
        {
            try
            {
                return await scyllaService.GetCombinedAuction(id);
            }
            catch (System.Exception)
            {
                var fromDb = auctions.Where(a => Guid.Parse(a.Uuid) == id).ToList();
                if (fromDb.Count == 0)
                {
                    logger.LogWarning("Auction {0} not found in database", id);
                    return null;
                }
                if (fromDb.Count > 1)
                {
                    logger.LogWarning("Multiple auctions with id {0} found in database {count}", id, fromDb.Count);
                }
                var auction = fromDb.First();
                if (auction.Tag == null && auction.AuctioneerId == null && auction.ProfileId == null && auction.HighestBidAmount == 0 && auction.ItemCreatedAt < new DateTime(2010, 1, 1))
                {
                    // not sure where they are from but they are baically uesless
                    ProduceDelete(auction);
                    return null;
                }
                logger.LogInformation("Auction {0} not found in scylla, inserting {full}", id, JsonConvert.SerializeObject(fromDb));
                await scyllaService.InsertAuction(auction);
                return await scyllaService.GetCombinedAuction(id);
            }
        });
        try
        {
            var archivedAuctions = (await Task.WhenAll(archivedVersionTask)).ToDictionary(a => a.Uuid);
            foreach (var auction in auctions)
            {
                await NewMethod(archivedAuctions, auction);
            }
            //await context.SaveChangesAsync();
        }
        catch (Exception e)
        {
            if (auctionids.Length > 1)
            {
                await RemoveAuction(auctionids.Take(auctionids.Length / 2).ToArray());
                await RemoveAuction(auctionids.Skip(auctionids.Length / 2).ToArray());
            }
            else
            {
                logger.LogError(e, "Error while saving trying to delete {0}", auctions.First().Uuid);
                throw;
            }
        }
    }

    private async Task NewMethod(Dictionary<string, SaveAuction> archivedAuctions, SaveAuction auction)
    {
        SaveAuction archivedObj = null;
        try
        {
            archivedObj = archivedAuctions[auction.Uuid];
        }
        catch (Exception)
        {
            await scyllaService.InsertAuction(auction);
            throw;
        }
        if (auction == null)
            return; // already deleted
        archivedObj.FlatenedNBT = null;
        var compareAuction = ScyllaService.CassandraToOld(ScyllaService.ToCassandra(auction));
        if (archivedObj.AuctioneerId == archivedObj.Uuid)
        {
            archivedObj.AuctioneerId = compareAuction.AuctioneerId;
        }
        AdjustForOptimizations(archivedObj);
        AdjustForOptimizations(compareAuction);
        var settings = new JsonSerializerSettings()
        {
            DateTimeZoneHandling = DateTimeZoneHandling.Utc
        };
        var archivedVersion = JsonConvert.SerializeObject(archivedObj);
        var toBeDeleted = JsonConvert.SerializeObject(compareAuction);
        if (toBeDeleted != archivedVersion && archivedObj.HighestBidAmount <= compareAuction.HighestBidAmount)
        {
            Console.WriteLine($"Archived version: {archivedVersion}");
            Console.WriteLine($"To be deleted   : {toBeDeleted}");
            await scyllaService.InsertAuction(auction);
            if (archivedObj.End == compareAuction.End && archivedObj.Start == compareAuction.Start && archivedObj.ItemCreatedAt == compareAuction.ItemCreatedAt || toBeDeleted.Length != archivedVersion.Length)
                throw new CoflnetException("no_match", $"Archived version does not match to be deleted version {JsonConvert.SerializeObject(auction)}");
        }
        /*
                context.Remove(auction);
                context.RemoveRange(auction.Enchantments);
                if (auction.NbtData != null)
                    context.RemoveRange(auction.NbtData);
                context.RemoveRange(auction.NBTLookup);
                context.RemoveRange(auction.CoopMembers);
                context.RemoveRange(auction.Bids);
                */
        ProduceDelete(auction);

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
            archivedObj.Bids = archivedObj.Bids.OrderBy(b => b.Amount).ThenBy(b => b.Timestamp).ToList();
            archivedObj.Enchantments = archivedObj.Enchantments.OrderBy(e => e.Type).ToList();
            archivedObj.FlatenedNBT = archivedObj.FlatenedNBT.OrderBy(n => n.Key).ToDictionary(n => n.Key, n => n.Value);
        }
    }

    private void ProduceDelete(SaveAuction auction)
    {
        deleteProducer.Produce("sky-delete-auction", new Message<string, long> { Key = auction.Uuid, Value = auction.HighestBidAmount });
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