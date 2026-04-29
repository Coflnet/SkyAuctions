using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using Coflnet.Sky.Core;
using Newtonsoft.Json;

namespace Coflnet.Sky.Auctions.Services;

public class AuctionBlobSerializer
{
    private static readonly JsonSerializer Serializer = JsonSerializer.Create(new JsonSerializerSettings
    {
        DateTimeZoneHandling = DateTimeZoneHandling.Utc,
        NullValueHandling = NullValueHandling.Ignore
    });

    public byte[] Serialize(IEnumerable<SaveAuction> auctions)
    {
        using var ms = new MemoryStream();
        using (var gz = new GZipStream(ms, CompressionLevel.Optimal, leaveOpen: true))
        using (var sw = new StreamWriter(gz))
        using (var jw = new JsonTextWriter(sw))
        {
            Serializer.Serialize(jw, auctions);
        }
        return ms.ToArray();
    }

    public List<SaveAuction> Deserialize(byte[] data)
    {
        using var ms = new MemoryStream(data);
        using var gz = new GZipStream(ms, CompressionMode.Decompress);
        using var sr = new StreamReader(gz);
        using var jr = new JsonTextReader(sr);
        return Serializer.Deserialize<List<SaveAuction>>(jr) ?? new List<SaveAuction>();
    }
}
