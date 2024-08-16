using System.Collections.Generic;

namespace Coflnet.Sky.Auctions.Services;

public class BuyerLookup
{
    public string Name { get; set; }
    public string Uuid { get; set; }
    public Dictionary<string, string> ItemsInInventory { get; set; } = new();
    public Dictionary<string,string> SocialLinks { get; set; } = new();
    public long LastLogin { get; set; }
    public bool ProfileNotFound { get; set; }
}
