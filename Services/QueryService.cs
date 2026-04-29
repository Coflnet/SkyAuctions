using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Data.Linq;
using Coflnet.Sky.Auctions.Models;
using Coflnet.Sky.Auctions.Services;
using Coflnet.Sky.Core;
using Coflnet.Sky.Filter;
using Coflnet.Sky.PlayerName.Client.Api;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Coflnet.Sky.Auctions;

public class QueryService
{
    private readonly ScyllaService scyllaService;
    private readonly ILogger<QueryService> logger;
    private readonly FilterEngine filterService;
    private readonly IPlayerNameApi playerNameApi;
    private readonly S3AuctionStorage s3Storage;
    private readonly ShadowReadService shadowRead;
    private readonly int monthsInScylla;

    public QueryService(ScyllaService scyllaService, ILogger<QueryService> logger, FilterEngine filterService, IPlayerNameApi playerNameApi, IConfiguration config, S3AuctionStorage s3Storage = null, ShadowReadService shadowRead = null)
    {
        this.scyllaService = scyllaService;
        this.logger = logger;
        this.filterService = filterService;
        this.playerNameApi = playerNameApi;
        this.s3Storage = s3Storage;
        this.shadowRead = shadowRead;
        monthsInScylla = config.GetValue<int?>("S3_MIGRATION:MONTHS_TO_KEEP_IN_SCYLLA")
            ?? config.GetValue<int?>("S3:MonthsToKeepInScylla")
            ?? 3;
    }

    public async Task<IEnumerable<QueryArchive>> GetPriceSumary(string itemTag, Dictionary<string, string> query)
    {
        var historyTable = scyllaService.QueryArchiveTable;
        var special = new[] { "EndAfter", "EndBefore" };
        var key = string.Concat(query.OrderBy(kv => kv.Key).Where(q => !special.Contains(q.Key)).Select(kv => kv.Key + kv.Value));

        var end = DateTime.UtcNow;
        if (query.ContainsKey("EndBefore"))
        {
            end = DateTime.Parse(query["EndBefore"]);
        }
        end = end.RoundDown(TimeSpan.FromDays(1));
        var start = end.AddDays(-7);
        if (query.ContainsKey("EndAfter"))
        {
            start = DateTime.Parse(query["EndAfter"]).RoundDown(TimeSpan.FromDays(1));
        }
        var history = (await historyTable.Where(h => h.FilterKey == key && h.Tag == itemTag && h.End > start && h.End <= end).ExecuteAsync()).ToList();
        var expected = (int)(end - start).TotalDays;
        if (history.Count < expected)
        {
            logger.LogInformation($"Found {history.Count} days of history for {itemTag} in the last {expected} days");
            var lookup = history.ToLookup(h => h.End);
            var expectedEnds = Enumerable.Range(0, expected).Select(i => DateTime.UtcNow.RoundDown(TimeSpan.FromDays(1)).AddDays(-i)).Where(d => !lookup.Contains(d)).ToList();
            foreach (var missing in expectedEnds)
            {
                var sumary = await AggregateDay(itemTag, query, key, missing);
                historyTable.Insert(sumary).SetConsistencyLevel(ConsistencyLevel.LocalQuorum).Execute();
                history.Add(sumary);
            }
        }
        return history;
    }

    internal async IAsyncEnumerable<SaveAuction> GetFiltered(string itemTag, Dictionary<string, string> filters, DateTime start, DateTime end, int amount = 1000)
    {
        var table = scyllaService.AuctionsTable;
        var endKey = ScyllaService.GetWeekOrDaysSinceStart(itemTag, end);
        var startKey = ScyllaService.GetWeekOrDaysSinceStart(itemTag, start);
        var returnCount = 0;
        var scyllaCutoff = DateTime.UtcNow.AddMonths(-monthsInScylla);
        var s3MonthCache = new Dictionary<DateTime, IReadOnlyList<CassandraAuction>>();
        var servedS3Months = new HashSet<DateTime>();

        // reverse from end to start
        foreach (var key in Enumerable.Range(startKey, endKey - startKey + 1).Reverse())
        {
            // Determine if this time range is in ScyllaDB or S3
            var keyDate = GetDateFromTimeKey(itemTag, key);

            IEnumerable<CassandraAuction> baseData;
            if (keyDate >= scyllaCutoff)
            {
                // Data is in ScyllaDB
                var scyllaList = (await table.Where(a => a.End > start && a.End <= end && a.IsSold && a.Tag == itemTag && a.TimeKey == key).ExecuteAsync()).ToList();
                baseData = scyllaList;
                // Shadow compare against S3 (passive) - only useful when archive has been backfilled for that month
                if (shadowRead != null && shadowRead.IsEnabled)
                {
                    shadowRead.CompareTagMonth("filter", itemTag, keyDate, scyllaList.Select(ScyllaService.CassandraToOld).ToList());
                }
            }
            else if (s3Storage != null)
            {
                // Data should be in S3
                var month = new DateTime(keyDate.Year, keyDate.Month, 1, 0, 0, 0, DateTimeKind.Utc);
                if (!servedS3Months.Add(month))
                {
                    continue;
                }

                if (!s3MonthCache.TryGetValue(month, out var cachedMonth))
                {
                    cachedMonth = (await GetAuctionsFromS3(itemTag, start, end, keyDate)).ToList();
                    s3MonthCache[month] = cachedMonth;
                }

                baseData = cachedMonth;
            }
            else
            {
                // No S3 storage configured, try ScyllaDB anyway
                baseData = await table.Where(a => a.End > start && a.End <= end && a.IsSold && a.Tag == itemTag && a.TimeKey == key).ExecuteAsync();
            }

            var result = AddFilter(filters, baseData).ToList();
            foreach (var item in result)
            {
                yield return item;
                returnCount++;
                if (returnCount >= amount)
                {
                    yield break;
                }
            }
        }
    }

    /// <summary>
    /// Gets the approximate date for a time key
    /// </summary>
    private static DateTime GetDateFromTimeKey(string tag, int timeKey)
    {
        var startDate = new DateTime(2019, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var splitSize = 7d;
        if (tag == "ENCHANTED_BOOK" || tag == "unknown" || tag == null)
        {
            splitSize = 0.5;
        }
        return startDate.AddDays(timeKey * splitSize);
    }

    /// <summary>
    /// Retrieves auctions from S3 storage for a specific date range
    /// </summary>
    private async Task<IEnumerable<CassandraAuction>> GetAuctionsFromS3(string itemTag, DateTime start, DateTime end, DateTime keyDate)
    {
        if (s3Storage == null)
            return Enumerable.Empty<CassandraAuction>();

        try
        {
            var year = keyDate.Year;
            var month = keyDate.Month;
            var auctions = await s3Storage.GetAuctions(itemTag, year, month);
            
            // Filter by date range
            return auctions.Where(a => a.IsSold && a.End > start && a.End <= end);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to retrieve auctions from S3 for {Tag} {Date}", itemTag, keyDate);
            return Enumerable.Empty<CassandraAuction>();
        }
    }

    internal async Task<IEnumerable<ItemPrices.AuctionPreview>> GetRecentOverview(string itemTag, Dictionary<string, string> query)
    {
        var end = DateTime.UtcNow;
        var start = end.AddHours(-1);
        var result = await LoadAuctions(itemTag, query, start, end);
        if (result.Count < 12)
        {
            start = start.AddDays(-13); // two weeks
            result = await LoadAuctions(itemTag, query, start, end);
            logger.LogInformation($"Found {result.Count} auctions for {itemTag} in the last 2 weeks");
        }
        var playerIds = await playerNameApi.PlayerNameNamesBatchPostAsync(result.Select(a => a.AuctioneerId).Distinct().ToList());
        return result.Select(a => new ItemPrices.AuctionPreview()
        {
            End = a.End,
            Price = a.HighestBidAmount,
            Seller = a.AuctioneerId,
            Uuid = a.Uuid,
            PlayerName = playerIds?.GetValueOrDefault(a.AuctioneerId, a.AuctioneerId)
        });
    }

    private async Task<List<SaveAuction>> LoadAuctions(string itemTag, Dictionary<string, string> query, DateTime start, DateTime end)
    {
        var result = new List<SaveAuction>();
        await foreach (var auction in GetFiltered(itemTag, query, start, end, 12))
        {
            result.Add(auction);
        }

        return result;
    }

    private IEnumerable<SaveAuction> AddFilter(Dictionary<string, string> query, IEnumerable<CassandraAuction> baseData)
    {
        var filter = filterService.GetMatchExpression(query, false).Compile();
        var withFilter = baseData.Select(ScyllaService.CassandraToOld).Where(a => filter(a));
        return withFilter;
    }

    private async Task<QueryArchive> AggregateDay(string tag, Dictionary<string, string> query, string key, DateTime end)
    {
        var start = end.AddDays(-1);
        var result = new List<SaveAuction>();
        await foreach (var auction in GetFiltered(tag, query, start, end, int.MaxValue))
        {
            result.Add(auction);
        }
        var prices = result.Select(a => a.HighestBidAmount).ToList();
        var sumary = new QueryArchive
        {
            End = end,
            Start = start,
            FilterKey = key,
            Filters = query,
            Tag = tag,
            Max = prices.DefaultIfEmpty(0).Max(),
            Min = prices.DefaultIfEmpty(0).Min(),
            Med = prices.DefaultIfEmpty(0).OrderBy(p => p).ElementAt(prices.Count / 2),
            Mean = prices.DefaultIfEmpty(0).Average(),
            Mode = prices.Count() == 0 ? 0 : prices.GroupBy(p => p).OrderByDescending(g => g.Count()).First().Key,
            Volume = prices.Count()
        };
        return sumary;
    }
}
