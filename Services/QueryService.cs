using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Data.Linq;
using Coflnet.Sky.Auctions.Models;
using Coflnet.Sky.Core;
using Coflnet.Sky.Filter;
using Coflnet.Sky.PlayerName.Client.Api;
using Microsoft.Extensions.Logging;

namespace Coflnet.Sky.Auctions;

public class QueryService
{
    private readonly ScyllaService scyllaService;
    private readonly ILogger<QueryService> logger;
    private readonly FilterEngine filterService;
    private readonly IPlayerNameApi playerNameApi;

    public QueryService(ScyllaService scyllaService, ILogger<QueryService> logger, FilterEngine filterService, IPlayerNameApi playerNameApi)
    {
        this.scyllaService = scyllaService;
        this.logger = logger;
        this.filterService = filterService;
        this.playerNameApi = playerNameApi;
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

    internal async Task<IEnumerable<ItemPrices.AuctionPreview>> GetRecentOverview(string itemTag, Dictionary<string, string> query)
    {
        var end = DateTime.UtcNow;
        var start = end.AddHours(-1);
        // if the auctions are rewritten to be sorted by end desc this order by could be avoided
        List<SaveAuction> result = await GetAuctions(itemTag, query, end, start);
        if (result.Count < 12)
        {
            start = start.AddDays(-13); // two weeks
            result.AddRange(await GetAuctions(itemTag, query, end, start));
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

        async Task<List<SaveAuction>> GetAuctions(string itemTag, Dictionary<string, string> query, DateTime end, DateTime start)
        {
            var baseData = await scyllaService.AuctionsTable.Where(a => a.End > start && a.End <= end && a.IsSold == true && a.Tag == itemTag).OrderByDescending(a => a.End).ExecuteAsync();
            IEnumerable<SaveAuction> withFilter = AddFilter(query, baseData);
            var result = withFilter.Take(12).ToList();
            return result;
        }
    }

    private IEnumerable<SaveAuction> AddFilter(Dictionary<string, string> query, IEnumerable<CassandraAuction> baseData)
    {
        var filter = filterService.GetMatchExpression(query, false).Compile();
        var withFilter = baseData.Select(ScyllaService.CassandraToOld).Where(a => filter(a));
        return withFilter;
    }

    private async Task<QueryArchive> AggregateDay(string tag, Dictionary<string, string> query, string key, DateTime end)
    {
        var table = scyllaService.AuctionsTable;
        var start = end.AddDays(-1);
        var monthId = ScyllaService.GetWeeksSinceStart(end);
        var baseData = await table.Where(a => a.End > start && a.End <= end && a.IsSold == true && a.Tag == tag && a.TimeKey == monthId).ExecuteAsync();
        var result = AddFilter(query, baseData).ToList();
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
