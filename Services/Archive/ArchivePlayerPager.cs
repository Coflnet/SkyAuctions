using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.Core.Archive;
using Microsoft.EntityFrameworkCore;

namespace Coflnet.Sky.Auctions.Archive;

/// <summary>One page of the player endpoints: the auctions and the cursor of the next page (null when the index has no more rows).</summary>
/// <param name="Auctions">Auctions in index order, newest End first.</param>
/// <param name="Next">Cursor taken from the last <b>index row</b> consumed by the page (End seconds, uid), never from the returned auctions.</param>
public sealed record ArchivedPlayerPage(List<ArchivedAuctionDto> Auctions, AuctionIndexCursor Next);

/// <summary>Every month (<c>yyyyMM</c>) whose <c>archived_months</c> sentinel (tag_id 0) says MariaDB rows were deleted (<c>deleted_at</c> set or <c>deleted_rows</c> &gt; 0).</summary>
public delegate Task<HashSet<int>> DeletedMonthsLookup(CancellationToken cancellationToken);

/// <summary>
/// Paging over a player's index rows (<c>auction_sellers</c> / <c>auction_bidders</c>) for the player endpoints. A page runs the index query once
/// and reads the auctions of exactly those rows (<see cref="ArchivedAuctionReader.FindByUidsAsync"/>), so the cursor and the content always come from
/// the same rows: a row inserted between two queries cannot be skipped, and a page that is shorter than the limit because some index rows have no location
/// or block (the reader skips them) still continues. With <c>deletedOnly</c> the index query is restricted to the End ranges of the months the
/// sentinel rows of <c>archived_months</c> mark as deleted (cached for <see cref="DefaultDeletedMonthsTtl"/>); nothing deleted means an empty page without a cursor.
/// </summary>
public class ArchivePlayerPager
{
    /// <summary>How long the set of deleted months is reused.</summary>
    public static readonly TimeSpan DefaultDeletedMonthsTtl = TimeSpan.FromSeconds(60);

    private sealed record CachedMonths(HashSet<int> Months, DateTime ExpiresAt);

    private readonly ArchivedAuctionReader reader;
    private readonly IArchiveLocationLookup lookup;
    private readonly DeletedMonthsLookup deletedMonths;
    private readonly TimeSpan deletedMonthsTtl;
    private readonly Func<DateTime> utcNow;
    private CachedMonths cached;

    /// <summary>Create; <paramref name="deletedMonths"/> defaults to the read-only <see cref="ArchiveContext"/> query, its result is reused for <paramref name="deletedMonthsTtl"/> (default 60 s).</summary>
    public ArchivePlayerPager(ArchivedAuctionReader reader, IArchiveLocationLookup lookup, DeletedMonthsLookup deletedMonths = null, TimeSpan? deletedMonthsTtl = null, Func<DateTime> utcNow = null)
    {
        this.reader = reader;
        this.lookup = lookup;
        this.deletedMonths = deletedMonths ?? ReadDeletedMonthsAsync;
        this.deletedMonthsTtl = deletedMonthsTtl ?? DefaultDeletedMonthsTtl;
        this.utcNow = utcNow ?? (() => DateTime.UtcNow);
    }

    /// <summary>Cursor that continues after an index row.</summary>
    public static AuctionIndexCursor CursorOf(IndexRow row) => new(row.End, row.Uid);

    /// <summary>An index row of either role.</summary>
    public readonly record struct IndexRow(uint End, long Uid);

    /// <summary>One page of a player's auctions as seller (<paramref name="sells"/>) or bidder.</summary>
    public async Task<ArchivedPlayerPage> GetPageAsync(uint playerId, bool sells, int? fromMonth, int? toMonth, int take, AuctionIndexCursor before, bool deletedOnly, CancellationToken cancellationToken)
    {
        var (from, to) = SecondsRange(fromMonth, toMonth);
        var ranges = deletedOnly ? await DeletedRangesAsync(from, to, before, cancellationToken).ConfigureAwait(false) : new List<(uint From, uint To)> { (from, to) };
        if (ranges.Count == 0)
            return new ArchivedPlayerPage(new List<ArchivedAuctionDto>(), null);
        // one row more than the page shows tells whether there is a next page
        var rows = await IndexRowsAsync(playerId, sells, ranges, take + 1, before, cancellationToken).ConfigureAwait(false);
        var hasMore = rows.Count > take;
        var page = rows.Take(take).ToList();
        var found = await reader.FindByUidsAsync(page.Select(r => r.Uid).ToList(), cancellationToken).ConfigureAwait(false);
        var next = hasMore ? CursorOf(page[^1]) : null;
        return new ArchivedPlayerPage(found.Select(a => ArchivedAuctionDto.From(a, includeBids: true)).ToList(), next);
    }

    /// <summary>The End ranges (newest first) of the deleted months inside <c>[from, to)</c> that can still hold rows after <paramref name="before"/>.</summary>
    private async Task<List<(uint From, uint To)>> DeletedRangesAsync(uint from, uint to, AuctionIndexCursor before, CancellationToken cancellationToken)
    {
        var months = await DeletedMonthsAsync(cancellationToken).ConfigureAwait(false);
        var ranges = new List<(uint From, uint To)>();
        foreach (var month in months.OrderByDescending(m => m))
        {
            var (start, end) = ArchiveMonths.MonthRange(month);
            var range = (From: Math.Max(from, ToSeconds(start)), To: Math.Min(to, ToSeconds(end)));
            if (range.From < range.To && (before == null || range.From <= before.End))
                ranges.Add(range);
        }
        return ranges;
    }

    private async Task<HashSet<int>> DeletedMonthsAsync(CancellationToken cancellationToken)
    {
        var snapshot = cached;
        if (snapshot != null && utcNow() < snapshot.ExpiresAt)
            return snapshot.Months;
        var months = await deletedMonths(cancellationToken).ConfigureAwait(false);
        cached = new CachedMonths(months, utcNow() + deletedMonthsTtl);
        return months;
    }

    /// <summary>Up to <paramref name="limit"/> index rows over the ranges (newest range first); the later ranges continue after the last row of the earlier ones.</summary>
    private async Task<List<IndexRow>> IndexRowsAsync(uint playerId, bool sells, List<(uint From, uint To)> ranges, int limit, AuctionIndexCursor before, CancellationToken cancellationToken)
    {
        var rows = new List<IndexRow>();
        foreach (var (from, to) in ranges)
        {
            var cursor = rows.Count > 0 ? CursorOf(rows[^1]) : before;
            rows.AddRange(await IndexRowsAsync(playerId, sells, from, to, limit - rows.Count, cursor, cancellationToken).ConfigureAwait(false));
            if (rows.Count >= limit)
                break;
        }
        return rows;
    }

    private async Task<IReadOnlyList<IndexRow>> IndexRowsAsync(uint playerId, bool sells, uint from, uint to, int limit, AuctionIndexCursor before, CancellationToken cancellationToken)
    {
        if (sells)
            return (await lookup.GetSellerRowsAsync(playerId, from, to, limit, before, cancellationToken).ConfigureAwait(false)).Select(r => new IndexRow(r.End, r.Uid)).ToList();
        return (await lookup.GetBidderRowsAsync(playerId, from, to, limit, before, cancellationToken).ConfigureAwait(false)).Select(r => new IndexRow(r.End, r.Uid)).ToList();
    }

    /// <summary>The month of an index row's End.</summary>
    public static int MonthOf(uint endUnixSeconds) => ArchiveMonths.MonthOf(DateTimeOffset.FromUnixTimeSeconds(endUnixSeconds).UtcDateTime);

    private static (uint From, uint To) SecondsRange(int? fromMonth, int? toMonth)
        => (fromMonth == null ? 0u : ToSeconds(ArchiveMonths.MonthRange(fromMonth.Value).start),
            toMonth == null ? uint.MaxValue : ToSeconds(ArchiveMonths.MonthRange(toMonth.Value).end));

    private static uint ToSeconds(DateTime utc)
    {
        var seconds = new DateTimeOffset(DateTime.SpecifyKind(utc, DateTimeKind.Utc)).ToUnixTimeSeconds();
        return seconds <= 0 ? 0 : seconds >= uint.MaxValue ? uint.MaxValue : (uint)seconds;
    }

    private static async Task<HashSet<int>> ReadDeletedMonthsAsync(CancellationToken cancellationToken)
    {
        using var db = new ArchiveContext();
        var sentinels = await db.ArchivedMonths.AsNoTracking().Where(m => m.TagId == 0).ToListAsync(cancellationToken).ConfigureAwait(false);
        return sentinels.Where(ScyllaArchiveRows.IsDeleted).Select(m => ArchiveMonths.FromIndex(m.Month)).ToHashSet();
    }
}
