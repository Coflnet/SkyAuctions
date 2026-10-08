using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.Auctions.Models;
using Coflnet.Sky.Core;
using Microsoft.Extensions.Logging;

namespace Coflnet.Sky.Auctions.Services;

/// <summary>Primary key of a row in weekly_auctions_2</summary>
public readonly record struct AuctionRowKey(string Tag, short TimeKey, bool IsSold, DateTime End, long AuctionUid)
{
    /// <summary>Normalizes End to utc milliseconds (cassandra timestamp precision) so json round trips compare equal</summary>
    public static AuctionRowKey From(string tag, short timeKey, bool isSold, DateTime end, long auctionUid)
    {
        var utc = end.Kind == DateTimeKind.Unspecified ? DateTime.SpecifyKind(end, DateTimeKind.Utc) : end.ToUniversalTime();
        return new AuctionRowKey(tag, timeKey, isSold, new DateTime(utc.Ticks - utc.Ticks % TimeSpan.TicksPerMillisecond, DateTimeKind.Utc), auctionUid);
    }

    public static AuctionRowKey From(ScyllaAuction row) => From(row.Tag, row.TimeKey, row.IsSold, row.End, row.AuctionUid);
}

/// <summary>One weekly_auctions_2 row that involves the exported player, with its full primary key</summary>
public class ExportedAuction
{
    public string Tag { get; set; }
    public short TimeKey { get; set; }
    public bool IsSold { get; set; }
    public DateTime End { get; set; }
    public long AuctionUid { get; set; }
    public Guid Uuid { get; set; }
    public Guid Auctioneer { get; set; }
    public Guid HighestBidder { get; set; }
    public Guid ProfileId { get; set; }
    public List<string> Coop { get; set; }
    public long StartingBid { get; set; }
    public long HighestBidAmount { get; set; }
    public bool Bin { get; set; }
    public string ItemName { get; set; }
    public DateTime Start { get; set; }
    public List<CassandraBid> Bids { get; set; }

    [Newtonsoft.Json.JsonIgnore]
    public AuctionRowKey Key => AuctionRowKey.From(Tag, TimeKey, IsSold, End, AuctionUid);
}

/// <summary>Result of the export, also the body expected by the erase call</summary>
public class PlayerExport
{
    public string PlayerUuid { get; set; }
    public DateTime CreatedAtUtc { get; set; }
    public List<CassandraBid> Bids { get; set; } = new();
    public List<ExportedAuction> Auctions { get; set; } = new();
    public List<int> S3ParticipationYears { get; set; } = new();
    public Dictionary<int, List<PlayerParticipationEntry>> S3Participation { get; set; } = new();
    public string NotCovered { get; set; }
}

public class PlayerEraseResult
{
    public int BidsDeleted { get; set; }
    public int AuctionsRewritten { get; set; }
    public int S3PlayerIndexObjectsDeleted { get; set; }
}

/// <summary>Validation failure carrying the http status to answer with</summary>
public class PrivacyException : Exception
{
    public int StatusCode { get; }
    public PrivacyException(int statusCode, string message) : base(message) { StatusCode = statusCode; }
}

/// <summary>Thin data access so the logic can be tested without scylla/s3</summary>
public interface IPlayerPrivacyStore
{
    Task<List<CassandraBid>> GetBids(Guid player, CancellationToken ct);
    Task<List<ScyllaAuction>> GetAuctionsBySeller(Guid player, CancellationToken ct);
    Task<List<ScyllaAuction>> GetAuctionsByHighestBidder(Guid player, CancellationToken ct);
    Task<List<ScyllaAuction>> GetAuctionsByAuctionUuid(Guid auctionUuid, CancellationToken ct);
    Task<ScyllaAuction> GetAuction(AuctionRowKey key, CancellationToken ct);
    /// <summary>Rewrites only the identity columns of the row</summary>
    Task UpdateAuctionIdentity(ScyllaAuction row, CancellationToken ct);
    Task DeleteBids(Guid player, CancellationToken ct);
    /// <summary>Participation entries per year, empty when S3 is not enabled</summary>
    Task<Dictionary<int, List<PlayerParticipationEntry>>> GetS3Participation(Guid player, CancellationToken ct);
    /// <summary>Deletes all player index objects, returns how many</summary>
    Task<int> DeleteS3PlayerIndex(Guid player, CancellationToken ct);
}

/// <summary>
/// Export and erasure of a single player. Only data that is directly addressable is touched:
/// the bids partition, auctions found via the seller/highest bidder/auction uid indexes and the S3 player index.
/// </summary>
public class PlayerPrivacyService
{
    public const string NotCoveredNote = "Auctions where the player is only a coop member are not included because finding them would need a full table scan; "
        + "coop-only auctions are scrubbed lazily at read time. S3 auction archive blobs are scrubbed lazily the next time they are read or written.";

    /// <summary>Maximum number of per auction queries in flight at once</summary>
    public const int MaxConcurrency = 8;

    private readonly IPlayerPrivacyStore store;
    private readonly ILogger<PlayerPrivacyService> logger;

    public PlayerPrivacyService(IPlayerPrivacyStore store, ILogger<PlayerPrivacyService> logger)
    {
        this.store = store;
        this.logger = logger;
    }

    public async Task<PlayerExport> Export(Guid player, CancellationToken ct = default)
    {
        var total = Stopwatch.StartNew();
        var sw = Stopwatch.StartNew();
        var bids = await store.GetBids(player, ct);
        logger.LogInformation("Privacy export {Player}: {Count} bids loaded in {Elapsed}ms", player, bids.Count, sw.ElapsedMilliseconds);
        var rows = await FindAuctionRows(player, bids, ct);
        sw.Restart();
        var s3 = await store.GetS3Participation(player, ct);
        logger.LogInformation("Privacy export {Player}: s3 participation {Years} years in {Elapsed}ms", player, s3.Count, sw.ElapsedMilliseconds);
        logger.LogInformation("Privacy export {Player} done: {Auctions} auctions in {Elapsed}ms", player, rows.Count, total.ElapsedMilliseconds);
        return new PlayerExport
        {
            PlayerUuid = player.ToString("N"),
            CreatedAtUtc = DateTime.UtcNow,
            Bids = bids,
            Auctions = rows.Values.Select(ToExported).ToList(),
            S3ParticipationYears = s3.Keys.OrderBy(y => y).ToList(),
            S3Participation = s3,
            NotCovered = NotCoveredNote
        };
    }

    public async Task<PlayerEraseResult> Erase(Guid player, PlayerExport body, CancellationToken ct = default)
    {
        if (body == null)
            throw new PrivacyException(400, "Missing export body");
        if (!Guid.TryParse(body.PlayerUuid, out var bodyPlayer) || bodyPlayer != player)
            throw new PrivacyException(400, "playerUuid in the body does not match the route");
        var bodyBids = body.Bids ?? new();
        var bodyAuctions = body.Auctions ?? new();
        if (bodyBids.Any(b => b.BidderUuid != player))
            throw new PrivacyException(400, "The body contains bids of another player");
        if (!PlayerOptOut.IsOptedOut(player))
            throw new PrivacyException(409, "The player has not opted out");

        var total = Stopwatch.StartNew();
        var sw = Stopwatch.StartNew();
        // every listed auction has to involve the player in the live row
        var lives = await MapBounded(bodyAuctions, (auction, token) => store.GetAuction(auction.Key, token), ct);
        for (var i = 0; i < bodyAuctions.Count; i++)
        {
            if (lives[i] == null)
                throw new PrivacyException(409, "An exported auction no longer exists, create a fresh export");
            if (!Involves(lives[i], player))
                throw new PrivacyException(400, $"Auction {bodyAuctions[i].Uuid} does not involve the player");
        }
        logger.LogInformation("Privacy erase {Player}: {Count} exported auctions validated in {Elapsed}ms", player, bodyAuctions.Count, sw.ElapsedMilliseconds);

        // scope check, the lookups are repeated and have to match the export
        sw.Restart();
        var liveBids = await store.GetBids(player, ct);
        logger.LogInformation("Privacy erase {Player}: {Count} bids reloaded in {Elapsed}ms", player, liveBids.Count, sw.ElapsedMilliseconds);
        var liveRows = await FindAuctionRows(player, liveBids, ct);
        var liveBidKeys = liveBids.Select(BidKey).ToHashSet();
        var bodyBidKeys = bodyBids.Select(BidKey).ToHashSet();
        if (!liveBidKeys.SetEquals(bodyBidKeys) || !liveRows.Keys.ToHashSet().SetEquals(bodyAuctions.Select(a => a.Key)))
            throw new PrivacyException(409, "The data changed since the export, create a fresh export");

        sw.Restart();
        var rewritten = 0;
        await MapBounded(liveRows.Values.ToList(), async (row, token) =>
        {
            if (AnonymizeRow(row, player, NewAnonymousGuid()))
            {
                await store.UpdateAuctionIdentity(row, token);
                Interlocked.Increment(ref rewritten);
            }
            return true;
        }, ct);
        logger.LogInformation("Privacy erase {Player}: {Count} auction rows rewritten in {Elapsed}ms", player, rewritten, sw.ElapsedMilliseconds);
        // bids last: the auctions above are found through them
        sw.Restart();
        var s3Deleted = await store.DeleteS3PlayerIndex(player, ct);
        logger.LogInformation("Privacy erase {Player}: {Count} s3 objects deleted in {Elapsed}ms", player, s3Deleted, sw.ElapsedMilliseconds);
        sw.Restart();
        if (liveBids.Count > 0)
            await store.DeleteBids(player, ct);
        logger.LogInformation("Privacy erase {Player}: {Count} bids deleted in {Elapsed}ms", player, liveBids.Count, sw.ElapsedMilliseconds);
        logger.LogInformation("Erased player {Player}: {Bids} bids, {Auctions} auctions, {S3} s3 objects in {Elapsed}ms", player, liveBids.Count, rewritten, s3Deleted, total.ElapsedMilliseconds);
        return new PlayerEraseResult { BidsDeleted = liveBids.Count, AuctionsRewritten = rewritten, S3PlayerIndexObjectsDeleted = s3Deleted };
    }

    /// <summary>
    /// Runs <paramref name="work"/> for every item with at most <see cref="MaxConcurrency"/> in flight, results keep the input order.
    /// The first failure or cancellation stops queued items from starting.
    /// </summary>
    private static async Task<TResult[]> MapBounded<TIn, TResult>(IReadOnlyList<TIn> items, Func<TIn, CancellationToken, Task<TResult>> work, CancellationToken ct)
    {
        using var gate = new SemaphoreSlim(MaxConcurrency);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var token = cts.Token;
        var tasks = items.Select(async item =>
        {
            await gate.WaitAsync(token);
            try
            {
                token.ThrowIfCancellationRequested();
                return await work(item, token);
            }
            catch
            {
                cts.Cancel();
                throw;
            }
            finally
            {
                gate.Release();
            }
        }).ToList();
        try
        {
            return await Task.WhenAll(tasks);
        }
        catch
        {
            // surface the original failure instead of a follow-up cancellation
            var first = tasks.FirstOrDefault(t => t.IsFaulted);
            if (first != null)
                await first;
            throw;
        }
    }

    private static (Guid, DateTime) BidKey(CassandraBid bid) => (bid.AuctionUuid, new DateTime(bid.Timestamp.ToUniversalTime().Ticks / TimeSpan.TicksPerMillisecond * TimeSpan.TicksPerMillisecond, DateTimeKind.Utc));

    private async Task<Dictionary<AuctionRowKey, ScyllaAuction>> FindAuctionRows(Guid player, List<CassandraBid> bids, CancellationToken ct)
    {
        var rows = new Dictionary<AuctionRowKey, ScyllaAuction>();
        var sw = Stopwatch.StartNew();
        var seller = await store.GetAuctionsBySeller(player, ct);
        foreach (var row in seller)
            rows[AuctionRowKey.From(row)] = row;
        logger.LogInformation("Privacy lookup {Player}: {Count} seller auctions in {Elapsed}ms", player, seller.Count, sw.ElapsedMilliseconds);

        sw.Restart();
        var highest = await store.GetAuctionsByHighestBidder(player, ct);
        foreach (var row in highest)
            rows[AuctionRowKey.From(row)] = row;
        logger.LogInformation("Privacy lookup {Player}: {Count} highest bidder auctions in {Elapsed}ms", player, highest.Count, sw.ElapsedMilliseconds);

        sw.Restart();
        var auctionUuids = bids.Select(b => b.AuctionUuid).Distinct().ToList();
        var viaBids = await MapBounded(auctionUuids, (uuid, token) => store.GetAuctionsByAuctionUuid(uuid, token), ct);
        foreach (var found in viaBids)
            foreach (var row in found)
                rows[AuctionRowKey.From(row)] = row;
        logger.LogInformation("Privacy lookup {Player}: {Count} distinct auctions from {Bids} bids in {Elapsed}ms", player, auctionUuids.Count, bids.Count, sw.ElapsedMilliseconds);
        return rows;
    }

    /// <summary>Does the live row reference the player as seller, highest bidder, bidder or coop member</summary>
    public static bool Involves(ScyllaAuction row, Guid player)
    {
        if (row.Auctioneer == player || row.HighestBidder == player)
            return true;
        if (row.Coop?.Any(c => IsPlayer(c, player)) ?? false)
            return true;
        return ReadBids(row).Any(b => b.BidderUuid == player);
    }

    private static bool IsPlayer(string uuid, Guid player) => Guid.TryParse(uuid?.Replace("-", ""), out var parsed) && parsed == player;

    private static List<CassandraBid> ReadBids(CassandraAuction row) =>
        row.SerialisedBids == null || row.SerialisedBids.Length == 0
            ? new List<CassandraBid>()
            : MessagePack.MessagePackSerializer.Deserialize<List<CassandraBid>>(row.SerialisedBids);

    private static ExportedAuction ToExported(ScyllaAuction row) => new()
    {
        Tag = row.Tag,
        TimeKey = row.TimeKey,
        IsSold = row.IsSold,
        End = row.End,
        AuctionUid = row.AuctionUid,
        Uuid = row.Uuid,
        Auctioneer = row.Auctioneer,
        HighestBidder = row.HighestBidder,
        ProfileId = row.ProfileId,
        Coop = row.Coop,
        StartingBid = row.StartingBid,
        HighestBidAmount = row.HighestBidAmount,
        Bin = row.Bin,
        ItemName = row.ItemName,
        Start = row.Start,
        Bids = ReadBids(row)
    };

    /// <summary>Same scheme as <see cref="PlayerOptOut"/> (prefix + random byte)</summary>
    public static Guid NewAnonymousGuid() => Guid.Parse(PlayerOptOut.AnonymousUuidPrefix + Random.Shared.Next(1, 254).ToString("X2"));

    /// <summary>
    /// Replaces the identity of <paramref name="player"/> in the row (in place). Other players and all economic/item data stay untouched.
    /// Profile ids equal to the player's uuid are cleared everywhere, also on other bidders' bids.
    /// </summary>
    /// <returns>true if anything changed</returns>
    public static bool AnonymizeRow(ScyllaAuction row, Guid player, Guid anonymous) =>
        AnonymizeRowCore(row, g => g == player, () => anonymous);

    /// <summary>
    /// Same as <see cref="AnonymizeRow"/> for all currently opted out players (read time erasure, e.g. coop members).
    /// Each erased identity gets its own anonymous uuid.
    /// </summary>
    /// <returns>true if anything changed</returns>
    public static bool AnonymizeRowForOptedOut(ScyllaAuction row) =>
        AnonymizeRowCore(row, PlayerOptOut.IsOptedOut, NewAnonymousGuid);

    /// <summary>
    /// Cheap check on the row columns only (no bid deserialization), false negatives are possible for bids.
    /// </summary>
    public static bool RowColumnsInvolveOptedOut(CassandraAuction row) =>
        PlayerOptOut.IsOptedOut(row.Auctioneer) || PlayerOptOut.IsOptedOut(row.HighestBidder) || PlayerOptOut.IsOptedOut(row.ProfileId)
        || (row.Coop?.Any(PlayerOptOut.IsOptedOut) ?? false);

    private static bool AnonymizeRowCore(ScyllaAuction row, Func<Guid, bool> isTarget, Func<Guid> newAnonymous)
    {
        var changed = false;
        if (isTarget(row.Auctioneer))
        {
            row.Auctioneer = newAnonymous();
            row.ProfileId = Guid.Empty;
            row.ProfileName = null;
            row.CoopName = null;
            changed = true;
        }
        if (isTarget(row.HighestBidder))
        {
            row.HighestBidder = newAnonymous();
            row.HighestBidderName = null;
            changed = true;
        }
        if (row.ProfileId != Guid.Empty && isTarget(row.ProfileId))
        {
            row.ProfileId = Guid.Empty;
            row.ProfileName = null;
            changed = true;
        }
        if (row.Coop != null && row.Coop.RemoveAll(c => Guid.TryParse(c?.Replace("-", ""), out var parsed) && isTarget(parsed)) > 0)
            changed = true;
        if (row.SerialisedBids is { Length: > 0 })
        {
            var bids = ReadBids(row);
            var bidsChanged = false;
            foreach (var bid in bids)
            {
                if (isTarget(bid.BidderUuid))
                {
                    bid.BidderUuid = newAnonymous();
                    bid.ProfileId = Guid.Empty;
                    bid.BidderName = null;
                    bidsChanged = true;
                }
                else if (bid.ProfileId != Guid.Empty && isTarget(bid.ProfileId))
                {
                    bid.ProfileId = Guid.Empty;
                    bidsChanged = true;
                }
            }
            if (bidsChanged)
            {
                row.SerialisedBids = MessagePack.MessagePackSerializer.Serialize<IEnumerable<CassandraBid>>(bids);
                changed = true;
            }
        }
        return changed;
    }
}
