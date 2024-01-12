using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Data.Linq;
using Cassandra.Mapping;
using Coflnet.Sky.Auctions.Models;
using Coflnet.Sky.Core;
using Coflnet.Sky.Filter;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using static Coflnet.Sky.Core.Enchantment;

namespace Coflnet.Sky.Auctions;
public class ScyllaService
{
    public Cassandra.ISession Session { get; set; }
    private Table<CassandraAuction> _auctionsTable;
    public Table<CassandraAuction> AuctionsTable
    {
        get
        {
            if (_auctionsTable == null)
            {
                _auctionsTable = GetAuctionsTable();
            }
            return _auctionsTable;
        }
        set
        {
            _auctionsTable = value;
        }
    }
    /// <summary>
    /// Table of past queries for quick access
    /// </summary>
    public Table<QueryArchive> QueryArchiveTable { get; set; }
    private Table<CassandraBid> BidsTable { get; set; }
    private ILogger<ScyllaService> Logger { get; set; }
    public ScyllaService(Cassandra.ISession session, ILogger<ScyllaService> logger)
    {
        Session = session;
        Logger = logger;
    }

    public async Task Create()
    {
        Logger.LogInformation("Creating tables");
        // drop tables
        //Session.Execute("DROP TABLE IF EXISTS auctions");
        var auctionsTable = GetAuctionsTable();
        var bidsTable = GetBidsTable();
        var queryTable = GetQueryArchiveTable();
        await auctionsTable.CreateIfNotExistsAsync();
        await bidsTable.CreateIfNotExistsAsync();
        await queryTable.CreateIfNotExistsAsync();

        AuctionsTable = auctionsTable;
        BidsTable = bidsTable;
        QueryArchiveTable = queryTable;
        Logger.LogInformation("Created tables");
        await Task.Delay(1000);
    }

    public async Task InsertAuction(SaveAuction auction)
    {
        if (auction.AuctioneerId == null && auction.Tag == null && auction.HighestBidAmount == 0)
            return;
        CassandraAuction converted = ToCassandra(auction);
        // check if exists
        var existing = (await AuctionsTable.Where(a => a.Tag == converted.Tag && a.IsSold == converted.IsSold && a.End == converted.End && a.Uuid == converted.Uuid).Select(a => a.Auctioneer).ExecuteAsync()).FirstOrDefault();
        if (existing != null && converted.Auctioneer == existing)
        {
            if (Random.Shared.NextDouble() < 0.01)
                Console.WriteLine("Already exists");
            return;
        }
        BatchStatement batch = new();
        var statement = AuctionsTable.Insert(converted).SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
        foreach (var item in converted.Bids)
        {
            batch = batch.Add(BidsTable.Insert(item));
        }
        batch = batch.Add(statement);
        batch.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
        batch.SetTimestamp(converted.End);
        batch.SetBatchType(BatchType.Unlogged);
        batch.SetRetryPolicy(new DefaultRetryPolicy());

        batch.SetRoutingKey(statement.RoutingKey);

        await Session.ExecuteAsync(batch).ConfigureAwait(false);
    }

    private static CassandraAuction ToCassandra(SaveAuction auction)
    {
        var auctionUuid = Guid.Parse(auction.Uuid);
        auction = new SaveAuction(auction);
        var root = auction.NbtData?.Root() ?? new fNbt.NbtCompound("i");
        if (auction.AnvilUses > 0)
        {
            root.Add(new fNbt.NbtInt("anvil_uses", auction.AnvilUses));
        }
        if (auction.Reforge != ItemReferences.Reforge.None)
        {
            root.Add(new fNbt.NbtString("modifier", auction.Reforge.ToString()));
        }
        auction.NbtData = new() { data = NBT.Bytes(root) };
        auction.SetFlattenedNbt(NBT.FlattenNbtData(auction.NbtData.Data));
        var colorString = auction.FlatenedNBT.GetValueOrDefault("color");
        int? color = null;
        if (colorString != null)
            color = (int)NBT.GetColor(colorString);
        var bids = auction.Bids?.Select(b => ToCassandra(b, Guid.Parse(auction.Uuid))).ToList();
        var enchants = auction.Enchantments.ToDictionary(e => e.Type == EnchantmentType.unknown ? ("unknown" + Random.Shared.Next(1, 20)) : e.Type.ToString(), e => (int)e.Level);
        var highestBidder = auction.Bids.Count == 0 ? Guid.Empty : Guid.Parse(auction.Bids.OrderByDescending(b => b.Amount).First().Bidder);
        var itemUid = long.Parse(auction.FlatenedNBT.GetValueOrDefault("uid", "0"), System.Globalization.NumberStyles.HexNumber);
        var itemUuid = Guid.Parse(auction.FlatenedNBT.GetValueOrDefault("uuid") ?? "00000000-0000-0000-0000-" + auction.FlatenedNBT.GetValueOrDefault("uid", "000000000000"));
        var isSold = auction.HighestBidAmount > 0 && auction.End < DateTime.UtcNow;
        var sellerUuid = Guid.Parse(auction.AuctioneerId ?? Guid.Empty.ToString());
        var converted = new CassandraAuction()
        {
            Uuid = auctionUuid,
            Auctioneer = sellerUuid,
            Bin = auction.Bin,
            Category = auction.Category.ToString(),
            Coop = auction.Coop,
            Color = color,
            Enchantments = enchants,
            End = auction.End,
            HighestBidAmount = auction.HighestBidAmount,
            HighestBidder = highestBidder,
            ItemName = auction.ItemName,
            Tag = auction.Tag ?? "unknown",
            Tier = auction.Tier.ToString(),
            StartingBid = auction.StartingBid,
            ItemUid = itemUid,
            ItemId = itemUuid,
            Start = auction.Start,
            ItemBytes = auction.NbtData?.data?.ToArray(),
            IsSold = isSold,
            ItemCreatedAt = auction.ItemCreatedAt,
            ProfileId = Guid.Parse(auction.ProfileId ?? auction.AuctioneerId),
            NbtLookup = auction.FlatenedNBT,
            Count = auction.Count,
            Bids = bids
        };
        return converted;
    }

    private Table<CassandraAuction> GetAuctionsTable()
    {
        var mapping = new MappingConfiguration()
            .Define(new Map<CassandraAuction>()
            .PartitionKey(t => t.Tag)
            // issold for selecting not sold auctions and checking if they were sold at a different time, auction uuid to allow storing auctions to be sold in the same second
            .ClusteringKey(new Tuple<string, SortOrder>("issold", SortOrder.Ascending), new("end", SortOrder.Descending), new("uuid", SortOrder.Descending))
            // secondary index
            .Column(t => t.Uuid, cm => cm.WithSecondaryIndex())
            .Column(t => t.Auctioneer, cm => cm.WithSecondaryIndex())
            .Column(t => t.HighestBidder, cm => cm.WithSecondaryIndex())
            .Column(t => t.ItemId, cm => cm.WithSecondaryIndex())
            .Column(t => t.Bids, cm => cm.Ignore())
        );
        return new Table<CassandraAuction>(Session, mapping, "auctions");
    }

    private Table<QueryArchive> GetQueryArchiveTable()
    {
        var mapping = new MappingConfiguration()
            .Define(new Map<QueryArchive>()
            .PartitionKey(t => t.Tag, t => t.FilterKey)
            .ClusteringKey(t => t.End)
        );
        return new Table<QueryArchive>(Session, mapping, "query_archive");
    }

    public async Task InsertBid(SaveBids bid, Guid guid)
    {
        await BidsTable.Insert(ToCassandra(bid, guid)).ExecuteAsync();
    }

    private static CassandraBid ToCassandra(SaveBids bid, Guid auctionUuid)
    {
        return new CassandraBid()
        {
            Amount = bid.Amount,
            AuctionUuid = auctionUuid,
            BidderUuid = Guid.Parse(bid.Bidder),
            Timestamp = bid.Timestamp,
            ProfileId = bid.ProfileId == "unknown" ? Guid.Parse("00000000-0000-0000-0000-000000000001") : Guid.Parse(bid.ProfileId ?? bid.Bidder)
        };
    }

    public async Task<SaveAuction[]> GetAuction(Guid uuid)
    {
        var result = await AuctionsTable.Where(a => a.Uuid == uuid).ExecuteAsync();
        var auctions = result.ToArray();
        return auctions.Select(CassandraToOld).ToArray();
    }

    public static SaveAuction CassandraToOld(CassandraAuction auction)
    {
        return new SaveAuction()
        {
            AuctioneerId = auction.Auctioneer.ToString("N"),
            Bin = auction.Bin,
            Category = (Category)Enum.Parse(typeof(Category), auction.Category),
            Coop = auction.Coop,
            End = auction.End,
            HighestBidAmount = auction.HighestBidAmount,
            ItemName = auction.ItemName,
            Tag = auction.Tag,
            Tier = (Tier)Enum.Parse(typeof(Tier), auction.Tier),
            StartingBid = auction.StartingBid,
            FlatenedNBT = auction.NbtLookup,
            ItemCreatedAt = auction.ItemCreatedAt,
            ProfileId = auction.ProfileId.ToString(),
            Uuid = auction.Uuid.ToString("N"),
            Count = auction.Count,

            AnvilUses = (short)int.Parse(auction.NbtLookup.GetValueOrDefault("anvil_uses", "0")),
            Reforge = auction.NbtLookup.GetValueOrDefault("modifier") == null ? ItemReferences.Reforge.None : (ItemReferences.Reforge)Enum.Parse(typeof(ItemReferences.Reforge), auction.NbtLookup.GetValueOrDefault("modifier")),
            Bids = auction.Bids.Select(b => new SaveBids()
            {
                Amount = b.Amount,
                AuctionId = b.AuctionUuid.ToString(),
                Bidder = b.BidderUuid.ToString(),
                Timestamp = b.Timestamp,
                ProfileId = b.ProfileId.ToString(),
            }).ToList(),
            NbtData = new NbtData()
            {
                data = auction.ItemBytes
            },
            Enchantments = auction.Enchantments.Select(e => new Enchantment()
            {
                Level = (byte)e.Value,
                Type = (EnchantmentType)Enum.Parse(typeof(EnchantmentType), e.Key),
            }).ToList(),
            UId = auction.ItemUid,
            Start = auction.Start,
        };
    }

    public async Task<IEnumerable<SaveAuction>> GetRecentFromPlayer(Guid playerUuid, DateTime before, int amount)
    {
        var statement = AuctionsTable.Where(a => a.Auctioneer == playerUuid && a.End < before && a.End > before - TimeSpan.FromDays(5)).AllowFiltering().Take(amount);
        statement.EnableTracing();
        var result = await statement.ExecuteAsync();
        Console.WriteLine("\n" + statement.QueryTrace + "\n" + statement.QueryTrace.Events.Count);
        // log the query trace formatted
        Console.WriteLine(JsonConvert.SerializeObject(statement.QueryTrace.Events.Select(s => s.ToString()), Formatting.Indented));
        Console.WriteLine(JsonConvert.SerializeObject(statement.QueryTrace.Parameters));
        Console.WriteLine(JsonConvert.SerializeObject(statement.QueryTrace.RequestType));
        return ToOldFormat(result);
    }

    public async Task<IEnumerable<AveragePrice>> GetSoldAuctionsBetween(string itemTag, DateTime start, DateTime end)
    {
        var result = await AuctionsTable.Where(a => a.Tag == itemTag && a.End > start && a.End < end).Select(a => new { a.Tag, a.End, a.HighestBidAmount }).ExecuteAsync();
        return result.GroupBy(a => (a.Tag, a.End.Hour)).Select(g => new AveragePrice()
        {
            Avg = g.Average(a => a.HighestBidAmount),
            Min = g.Min(a => a.HighestBidAmount),
            Max = g.Max(a => a.HighestBidAmount),
            Volume = g.Count(),
            Date = g.First().End,
        });
    }

    private static IEnumerable<SaveAuction> ToOldFormat(IEnumerable<CassandraAuction> result)
    {
        return result.Select(CassandraToOld);
    }

    private Table<CassandraBid> GetBidsTable()
    {
        var mapping = new MappingConfiguration()
            .Define(new Map<CassandraBid>()
            .PartitionKey(t => t.BidderUuid).ClusteringKey(new Tuple<string, SortOrder>("timestamp", SortOrder.Descending))
            // secondary index
            .Column(t => t.AuctionUuid, cm => cm.WithSecondaryIndex())
        );
        return new Table<CassandraBid>(Session, mapping, "bids");
    }

    internal Task InsertAuctions(IEnumerable<SaveAuction> auctions)
    {
        if (auctions == null)
            return Task.CompletedTask;
        return Parallel.ForEachAsync(auctions, async (a, t) =>
        {
            try
            {
                await InsertAuction(a);
            }
            catch (Exception e)
            {
                Logger.LogError(e, "Failed to insert auction\n" + JsonConvert.SerializeObject(a));
            }
        });
    }

    internal async Task InsertAuctionsOfTag(IEnumerable<SaveAuction> auctions)
    {
        var tag = auctions.First().Tag;
        if (!auctions.All(a => a.Tag == tag))
            throw new ArgumentException("All auctions must have the same tag");
        var batch = new BatchStatement();
        Statement statement = null;
        try
        {
            await AssignExistingData(auctions);
        }
        catch (System.Exception e)
        {
            Logger.LogError(e, "Failed to assign existing data");
        }
        // insert
        foreach (var a in auctions)
        {
            statement = AuctionsTable.Insert(ToCassandra(a));
            var time = a.Bids?.Select(b => b.Timestamp).DefaultIfEmpty(a.Start).Max() ?? a.Start;
            statement.SetTimestamp(time);
            batch.Add(statement);
        }
        batch.SetRoutingKey(statement.RoutingKey);
        await Session.ExecuteAsync(batch);
    }

    private async Task AssignExistingData(IEnumerable<SaveAuction> auctions)
    {
        // auctions without start are from the sells endpoint and are missing some info that might was available when the auction was created
        var lookup = auctions
            .Where(a => a.Start == default && a.End > DateTime.UtcNow - TimeSpan.FromDays(14))
            .ToLookup(a => Guid.Parse(a.Uuid));
        if (!lookup.Any())
            return;
        // find and delete not sold auctions
        var ids = lookup.Select(a => a.Key).ToList();
        var minEnd = lookup.Select(a => a.Min(a => a.End)).Min();
        var maxEnd = lookup.Select(a => a.Max(a => a.End)).Max() + TimeSpan.FromDays(14);
        var tag = lookup.First().First().Tag;
        var result = await AuctionsTable.Where(a => ids.Contains(a.Uuid) && !a.IsSold && a.End < maxEnd && a.End > minEnd && a.Tag == tag).AllowFiltering().ExecuteAsync();
        Logger.LogInformation($"Found {result.Count()} auctions to retrofit");
        foreach (var a in result)
        {
            if (!lookup.Contains(a.Uuid))
            {
                Logger.LogError($"Auction {a.Uuid} not found in lookup, first match {ids.FirstOrDefault()}");
                continue;
            }
            var match = lookup[a.Uuid].First();
            match.Start = a.Start;
            match.Count = a.Count;
            match.ItemCreatedAt = a.ItemCreatedAt;
            match.ItemName = a.ItemName;
            match.ProfileId = a.ProfileId.ToString();
            match.Bin = a.Bin;
            match.StartingBid = a.StartingBid;
            Console.WriteLine($"retrofitted {match.Uuid} {match.ItemName} {match.Start} {match.Count} {match.ItemCreatedAt} {match.ProfileId} {match.Bin} {match.StartingBid}");

            //statement = AuctionsTable.Delete(a);
            //batch.Add(statement);
        }
    }

    internal async Task InsertBids(IEnumerable<SaveBids> bids)
    {
        if (bids == null)
            return;
        var batch = new BatchStatement();
        Statement statement = null;
        foreach (var b in bids)
        {
            statement = BidsTable.Insert(ToCassandra(b, Guid.Parse(b.AuctionId)));
            batch.Add(statement);
        }
        batch.SetRoutingKey(statement.RoutingKey);
        await Session.ExecuteAsync(batch).ConfigureAwait(false);
    }

    internal async Task<PriceSumary> GetSumary(string itemTag, Dictionary<string, string> dictionary)
    {
        var days = 2d;
        if (dictionary.Remove("days", out var d))
        {
            if (!double.TryParse(d, out days))
            {
                days = 2d;
            }
            // max 2 days, positive
            days = Math.Min(2, Math.Max(0, days));
        }
        var batch = await AuctionsTable.Where(a => a.Tag == itemTag && a.End > DateTime.UtcNow - TimeSpan.FromDays(days) && a.End < DateTime.UtcNow && a.IsSold).ExecuteAsync();

        var result = new FilterEngine().Filter(batch.Select(CassandraToOld), dictionary).ToList();
        if (result.Count == 0)
            return new PriceSumary();
        if (result.GroupBy(a => a.Uuid).Any(g => g.Count() > 1))
            throw new Exception("Duplicate auctions");
        return new PriceSumary()
        {
            Mean = result.Average(a => a.HighestBidAmount),
            Min = result.Min(a => a.HighestBidAmount),
            Max = result.Max(a => a.HighestBidAmount),
            Volume = result.Count(),
            Med = result.OrderBy(a => a.HighestBidAmount).Skip(result.Count() / 2).First().HighestBidAmount,
            Mode = result.GroupBy(a => a.HighestBidAmount).OrderByDescending(g => g.Count()).First().Key,
        };
    }
    public async Task<List<SaveAuction>> GetRecentBatch(string itemTag)
    {
        var batch = await AuctionsTable.Where(a => a.Tag == itemTag && a.End > DateTime.UtcNow - TimeSpan.FromDays(2) && a.IsSold).OrderByDescending(a => a.End).Take(1000).ExecuteAsync();
        return batch.Select(CassandraToOld).ToList();
    }
}