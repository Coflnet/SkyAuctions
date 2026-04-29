using System;
using MessagePack;

namespace Coflnet.Sky.Auctions.Models;

public enum ParticipationType : byte
{
    Seller = 0,
    Bidder = 1
}

[MessagePackObject]
public class PlayerParticipationEntry
{
    [Key(0)]
    public Guid AuctionUid { get; set; }
    [Key(1)]
    public DateTime End { get; set; }
    [Key(2)]
    public string Tag { get; set; }
    [Key(3)]
    public ParticipationType Type { get; set; }
}
