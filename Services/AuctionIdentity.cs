using System;
using System.Collections.Generic;
using System.Globalization;
using Coflnet.Sky.Core;

namespace Coflnet.Sky.Auctions.Services;

internal static class AuctionIdentity
{
    public static bool TryParseGuid(string value, out Guid guid)
    {
        guid = Guid.Empty;
        if (string.IsNullOrWhiteSpace(value) || string.Equals(value, "unknown", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var trimmed = value.Trim();
        return Guid.TryParse(trimmed, out guid)
            || Guid.TryParseExact(trimmed.Replace("-", string.Empty, StringComparison.Ordinal), "N", out guid);
    }

    public static Guid ParseGuidOrEmpty(string value)
    {
        return TryParseGuid(value, out var guid) ? guid : Guid.Empty;
    }

    public static string NormalizeUuid(string value)
    {
        return TryParseGuid(value, out var guid)
            ? guid.ToString("N")
            : string.Empty;
    }

    public static bool Matches(Guid guid, string value)
    {
        return TryParseGuid(value, out var parsed) && parsed == guid;
    }

    public static long GetAuctionUid(SaveAuction auction)
    {
        if (auction == null)
        {
            return 0;
        }

        if (auction.UId != 0)
        {
            return auction.UId;
        }

        return TryParseGuid(auction.Uuid, out var guid)
            ? AuctionService.Instance.GetId(guid.ToString("N"))
            : 0;
    }

    public static long ParseItemUid(IDictionary<string, string> flattenedNbt)
    {
        if (flattenedNbt?.TryGetValue("uid", out var uid) == true
            && long.TryParse(uid, NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var parsed))
        {
            return parsed;
        }

        return 0;
    }

    public static Guid ParseItemGuid(IDictionary<string, string> flattenedNbt)
    {
        if (flattenedNbt == null)
        {
            return Guid.Empty;
        }

        if (flattenedNbt.TryGetValue("uuid", out var itemUuid) && TryParseGuid(itemUuid, out var parsedUuid))
        {
            return parsedUuid;
        }

        if (!flattenedNbt.TryGetValue("uid", out var itemUid) || string.IsNullOrWhiteSpace(itemUid))
        {
            return Guid.Empty;
        }

        var suffix = itemUid.Trim();
        if (suffix.Length > 12)
        {
            suffix = suffix[^12..];
        }

        suffix = suffix.PadLeft(12, '0');
        return TryParseGuid($"00000000-0000-0000-0000-{suffix}", out var generated)
            ? generated
            : Guid.Empty;
    }
}