using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.Auctions.Models;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace Coflnet.Sky.Auctions.Services;

public class S3PlayerIndexService
{
    private readonly S3StorageService s3;
    private readonly ILogger<S3PlayerIndexService> logger;
    private readonly ConcurrentDictionary<string, List<PlayerParticipationEntry>> buffer = new();
    private readonly ConcurrentDictionary<string, SemaphoreSlim> blobLocks = new(StringComparer.Ordinal);
    private readonly SemaphoreSlim flushLock = new(1, 1);
    private const int FlushParallelism = 16;

    private static readonly JsonSerializer Serializer = JsonSerializer.Create(new JsonSerializerSettings
    {
        DateTimeZoneHandling = DateTimeZoneHandling.Utc,
        NullValueHandling = NullValueHandling.Ignore
    });

    public S3PlayerIndexService(S3StorageService s3, ILogger<S3PlayerIndexService> logger)
    {
        this.s3 = s3;
        this.logger = logger;
    }

    public static string BlobKey(Guid player, int year) =>
        $"players/{player.ToString("N")[..2]}/{player:N}/{year}.json.gz";

    public void AddParticipations(Guid player, IEnumerable<PlayerParticipationEntry> entries)
    {
        foreach (var entry in entries)
        {
            var key = $"{player:N}_{entry.End.Year}";
            buffer.AddOrUpdate(key,
                _ => new List<PlayerParticipationEntry> { entry },
                (_, list) => { lock (list) { list.Add(entry); } return list; });
        }
    }

    public async Task FlushAll(CancellationToken ct = default)
    {
        if (!await flushLock.WaitAsync(0, ct))
            return; // another flush in progress

        try
        {
            var toFlush = new List<(Guid Player, int Year, List<PlayerParticipationEntry> Entries)>();
            var keys = buffer.Keys.ToList();
            foreach (var key in keys)
            {
                if (!buffer.TryRemove(key, out var entries))
                    continue;

                var parts = key.Split('_');
                var player = Guid.Parse(parts[0]);
                var year = int.Parse(parts[1]);
                toFlush.Add((player, year, entries));
            }

            var options = new ParallelOptions
            {
                CancellationToken = ct,
                MaxDegreeOfParallelism = FlushParallelism
            };
            await Parallel.ForEachAsync(toFlush, options, async (item, token) =>
            {
                await MergeAndUpload(item.Player, item.Year, item.Entries, token);
            });
        }
        finally
        {
            flushLock.Release();
        }
    }

    private async Task MergeAndUpload(Guid player, int year, List<PlayerParticipationEntry> newEntries, CancellationToken ct)
    {
        var blobKey = BlobKey(player, year);
        var blobLock = blobLocks.GetOrAdd(blobKey, _ => new SemaphoreSlim(1, 1));
        await blobLock.WaitAsync(ct);
        try
        {
            var existing = new List<PlayerParticipationEntry>();
            if (await s3.HeadBlob(blobKey, ct))
            {
                try
                {
                    var data = await s3.GetBlob(blobKey, ct);
                    existing = DeserializeEntries(data);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Failed to read player index blob {blobKey}", e);
                }
            }

            var set = existing.ToDictionary(e => (e.AuctionUid, e.Type));
            foreach (var entry in newEntries)
            {
                set[(entry.AuctionUid, entry.Type)] = entry;
            }

            var merged = set.Values
                .OrderByDescending(e => e.End)
                .ThenBy(e => e.AuctionUid)
                .ToList();
            var bytes = SerializeEntries(merged);
            await s3.PutBlob(blobKey, bytes, "application/gzip", ct);
        }
        finally
        {
            blobLock.Release();
        }
    }

    public async Task<List<PlayerParticipationEntry>> GetParticipation(Guid player, int year, CancellationToken ct = default)
    {
        var blobKey = BlobKey(player, year);
        var data = await s3.GetBlob(blobKey, ct);
        return DeserializeEntries(data);
    }

    private static byte[] SerializeEntries(List<PlayerParticipationEntry> entries)
    {
        using var ms = new MemoryStream();
        using (var gz = new GZipStream(ms, CompressionLevel.Optimal, leaveOpen: true))
        using (var sw = new StreamWriter(gz))
        using (var jw = new JsonTextWriter(sw))
        {
            Serializer.Serialize(jw, entries);
        }
        return ms.ToArray();
    }

    private static List<PlayerParticipationEntry> DeserializeEntries(byte[] data)
    {
        using var ms = new MemoryStream(data);
        using var gz = new GZipStream(ms, CompressionMode.Decompress);
        using var sr = new StreamReader(gz);
        using var jr = new JsonTextReader(sr);
        return Serializer.Deserialize<List<PlayerParticipationEntry>>(jr) ?? new List<PlayerParticipationEntry>();
    }
}
