using System;

namespace Coflnet.Sky.Auctions;

[MessagePack.MessagePackObject]
public class CassandraBid
{
    [MessagePack.Key(0)]
    public Guid AuctionUuid { get; set; }
    [MessagePack.Key(1)]
    public Guid BidderUuid { get; set; }
    [MessagePack.Key(2)]
    public long Amount { get; set; }
    [MessagePack.Key(3)]
    public DateTime Timestamp { get; set; }
    [MessagePack.Key(4)]
    public string BidderName { get; set; }
    [MessagePack.Key(5)]
    public Guid ProfileId { get; set; }

}
