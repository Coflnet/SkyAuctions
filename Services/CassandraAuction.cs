using System;
using System.Collections.Generic;
using Coflnet.Sky.Core;
using Newtonsoft.Json;

namespace Coflnet.Sky.Auctions;
/// <summary>
/// Auctions stored in cassandra
/// </summary>
public class CassandraAuction : ICassandraItem
{
    [Cassandra.Mapping.Attributes.SecondaryIndex()]
    public Guid Uuid { get; set; }
    [Cassandra.Mapping.Attributes.PartitionKey]
    public string Tag { get; set; }
    public string ItemName { get; set; }
    public string ItemLore { get; set; }
    public string Extra { get; set; }
    public string Category { get; set; }
    public string Tier { get; set; }
    public long StartingBid { get; set; }
    public long HighestBidAmount { get; set; }
    public bool Bin { get; set; }
    public bool IsSold { get; set; }
    [Cassandra.Mapping.Attributes.ClusteringKey]
    public DateTime End { get; set; }
    public DateTime Start { get; set; }
    public DateTime ItemCreatedAt { get; set; }
    public Guid Auctioneer { get; set; }
    public Guid ProfileId { get; set; }
    public string ProfileName { get; set; }
    public List<string> Coop { get; set; }
    public string CoopName { get; set; }
    public Guid HighestBidder { get; set; }
    public string HighestBidderName { get; set; }
    public string ExtraAttributesJson { get; set; }
    public Dictionary<string, string> NbtLookup { get; set; }
    public byte[] ItemBytes { get; set; }
    public long ItemUid { get; set; }
    public long? Id
    {
        get
        {
            return ItemUid;
        }
        set
        {
            ItemUid = value.Value;
        }
    }
    public Guid ItemId { get; set; }
    public Dictionary<string, int> Enchantments { get; set; }
    [MessagePack.IgnoreMember]
    [JsonIgnore]
    [System.Text.Json.Serialization.JsonIgnore]
    public byte[] SerialisedBids { get; set; }
    [Cassandra.Mapping.Attributes.Ignore]
    [JsonProperty("bids")]
    public List<CassandraBid> Bids
    {
        get
        {
            return MessagePack.MessagePackSerializer.Deserialize<List<CassandraBid>>(SerialisedBids);
        }
        set
        {
            if (SerialisedBids == null && value != null)
                SerialisedBids = MessagePack.MessagePackSerializer.Serialize<IEnumerable<CassandraBid>>(value);
        }
    }
    public int Count { get; set; }
    public int? Color { get; set; }
}
