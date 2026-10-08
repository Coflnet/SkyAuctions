using System.Collections.Generic;
using System.Linq;
using Coflnet.Sky.Core;

namespace Coflnet.Sky.Auctions.Services;

/// <summary>
/// Detection helpers around <see cref="PlayerOptOut"/> so unchanged data can be left untouched
/// </summary>
public static class OptOutScrubber
{
    /// <summary>True if the auction still references an opted out player anywhere (seller, profile, bids, coop)</summary>
    public static bool ContainsOptedOut(SaveAuction auction)
    {
        if (auction == null)
            return false;
        return PlayerOptOut.IsOptedOut(auction.AuctioneerId)
            || PlayerOptOut.IsOptedOut(auction.ProfileId)
            || (auction.Bids?.Any(b => PlayerOptOut.IsOptedOut(b.Bidder) || PlayerOptOut.IsOptedOut(b.ProfileId)) ?? false)
            || (auction.CoopMembers?.Any(m => PlayerOptOut.IsOptedOut(m.value)) ?? false)
            || (auction.ClaimedBids?.Any(m => PlayerOptOut.IsOptedOut(m.value)) ?? false);
    }

    /// <summary>Masks every auction that references an opted out player. Returns true if anything changed.</summary>
    public static bool MaskAll(IEnumerable<SaveAuction> auctions)
    {
        var changed = false;
        foreach (var auction in auctions)
        {
            if (!ContainsOptedOut(auction))
                continue;
            PlayerOptOut.Mask(auction);
            changed = true;
        }
        return changed;
    }
}
