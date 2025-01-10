using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using System;
using Cassandra.Data.Linq;
using System.Collections.Generic;
using Cassandra;
using Cassandra.Mapping;
using System.Linq;
using Coflnet.Sky.Core;
using System.Text;
using RestSharp;
using Newtonsoft.Json;
using Microsoft.Extensions.Logging;

namespace Coflnet.Sky.Auctions.Services;

public class ExportService : BackgroundService
{
    Table<ExportRequest> exportRequests;
    QueryService queryService;
    ProfileClient profileClient;
    ILogger<ExportService> logger;
    public ExportService(ISession session, QueryService queryService, ProfileClient profileClient, ILogger<ExportService> logger)
    {
        var mapping = new MappingConfiguration().Define(
            new Map<ExportRequest>()
                .TableName("export_requests")
                .PartitionKey(e => e.ByEmail)
                .ClusteringKey(e => e.RequestedAt)
                .Column(e => e.Flags, cm => cm.WithDbType<int>())
                .Column(e => e.Status, cm => cm.WithDbType<int>())
        );
        exportRequests = new Table<ExportRequest>(session, mapping);
        exportRequests.CreateIfNotExists();
        // set table TTL
        session.Execute($"ALTER TABLE export_requests WITH default_time_to_live = 86400");
        this.queryService = queryService;
        this.profileClient = profileClient;
        this.logger = logger;
    }

    Queue<ExportRequest> pendingRequests = new Queue<ExportRequest>();

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // load from db
        var requests = (await exportRequests.ExecuteAsync()).ToList();
        foreach (var request in requests.OrderBy(r => r.RequestedAt))
        {
            if (request.Status != ExportStatus.Done)
                pendingRequests.Enqueue(request);
            else
            {
                logger.LogInformation($"Skipping {request.ByEmail} export for {request.ItemTag} request with status {request.Status}");
            }
        }
        logger.LogInformation("Export service started {0} {total}", pendingRequests.Count, requests);
        while (!stoppingToken.IsCancellationRequested)
        {
            if (pendingRequests.Count > 0)
            {
                var request = pendingRequests.Dequeue();
                request.Status = ExportStatus.InProgress;
                exportRequests.Insert(request).Execute();
                try
                {
                    await RunExport(request);
                }
                catch (System.Exception e)
                {
                    logger.LogError(e, $"Failed to export {request.ItemTag} for {request.ByEmail}");
                    request.Status = ExportStatus.Failed;
                    exportRequests.Insert(request).Execute();
                    pendingRequests.Enqueue(request);
                    await Task.Delay(10000, stoppingToken);
                    continue;
                }
                request.Status = ExportStatus.Done;
                exportRequests.Insert(request).Execute();
            }
            await Task.Delay(10000, stoppingToken);
        }
    }

    private async Task RunExport(ExportRequest request)
    {
        logger.LogInformation($"Exporting {request.ItemTag} for {request.ByEmail} {JsonConvert.SerializeObject(request)}");
        var end = GetTimeKey(request, "EndBefore");
        var start = GetTimeKey(request, "EndAfter");

        var auctions = queryService.GetFiltered(request.ItemTag, request.Filters, start, end, request.ByEmail.Contains("thomaswilcox") ? 20_000 : 1000).ToBlockingEnumerable();
        if (request.Flags.HasFlag(ExportFlags.UniqueItems))
        {
            auctions = auctions.GroupBy(a => a.FlatenedNBT.GetValueOrDefault("uid", Random.Shared.Next().ToString())).Select(g => g.OrderByDescending(g => g.End).First());
        }
        var resultBuilder = new StringBuilder();
        var buyerUuids = auctions.Select(a => a.Bids.OrderByDescending(b => b.Amount).FirstOrDefault()).Where(b => b != null).Select(b => (b.Bidder, b.ProfileId)).Distinct().ToList();
        var buyerLookup = new Dictionary<(string, string), BuyerLookup>();
        logger.LogInformation($"Found {buyerUuids.Count} unique buyers, loading their details");
        if (request.Flags.HasFlag(ExportFlags.InventoryCheck) || request.Flags.HasFlag(ExportFlags.IncludeSocial))
            foreach (var item in buyerUuids)
            {
                var lookup = await profileClient.GetLookup(item.Bidder, item.ProfileId);
                buyerLookup[item] = lookup;
            }
        var columnMapping = new Dictionary<string, Func<SaveAuction, string>>() { {
            "uuid", a => a.Uuid.ToString() },
            {"itemName", a=>a.ItemName},
            {"highestBidAmount", a=>a.HighestBidAmount.ToString()},
            {"bin", a=>a.Bin.ToString()},
            {"endedAt", a=>a.End.ToString()},
            {"buyerUuid", a=> GetBuyerProfile(a).Item1},
            {"buyerProfile", a=> GetBuyerProfile(a).Item2},
            {"itemUid", a=>a.FlatenedNBT.GetValueOrDefault("uid")?.ToString()},
            {"inInventory", a => buyerLookup.GetValueOrDefault(GetBuyerProfile(a))?.ItemsInInventory?.GetValueOrDefault(a.FlatenedNBT.GetValueOrDefault("uid", a.Tag), "no")?.ToString() ?? "failed"},
            {"itemsFoundInInventory", a => buyerLookup.GetValueOrDefault(GetBuyerProfile(a))?.ItemsInInventory.Count.ToString()},
            {"profileFound", a => buyerLookup.GetValueOrDefault(GetBuyerProfile(a))?.ProfileNotFound.ToString()},
            {"buyerName", a => buyerLookup.GetValueOrDefault(GetBuyerProfile(a))?.Name},
            {"buyerLastLogin", a => buyerLookup.GetValueOrDefault(GetBuyerProfile(a))?.LastLogin.ToString()},
             };
        if (request.Flags.HasFlag(ExportFlags.IncludeSocial))
        {
            var socialAvailable = buyerLookup.Values.SelectMany(b => b.SocialLinks.Keys).GroupBy(s => s).Select(g => g.Key).ToList();
            Console.WriteLine($"Social available {string.Join(", ", socialAvailable)}");
            foreach (var social in socialAvailable)
            {
                columnMapping[$"buyerSocial_{social}"] = a => buyerLookup.GetValueOrDefault(GetBuyerProfile(a))?.SocialLinks.GetValueOrDefault(social, "none");
            }
        }
        if (request.Columns == null || request.Columns.Count < 2)
        {
            request.Columns = columnMapping.Keys.ToList();
        }
        foreach (var column in request.Columns)
        {
            if (!columnMapping.ContainsKey(column))
            {
                throw new CoflnetException("invalid_column", $"Invalid column {column}");
            }
        }
        resultBuilder.AppendLine(string.Join(',', request.Columns));
        foreach (var auction in auctions)
        {
            foreach (var column in request.Columns)
            {
                resultBuilder.Append(columnMapping[column](auction));
                resultBuilder.Append(',');
            }
            // remove last ,
            resultBuilder.Length--;
            resultBuilder.AppendLine();
        }
        logger.LogInformation($"Sending {auctions.Count()} auctions for {request.ItemTag} from {start} to {end}");
        var client = new RestClient();
        var webhookRequest = new RestRequest(request.DiscordWebhookUrl, Method.Post);
        var title = $"Auction export for {request.ItemTag} from {start} to {end}";
        // attach file to discord webhook
        webhookRequest.AddFile("file", Encoding.UTF8.GetBytes(resultBuilder.ToString()), "auctions.csv", "text/csv");
        webhookRequest.AddHeader("Content-Type", "multipart/form-data");
        var payloadjson = JsonConvert.SerializeObject(new { content = title });
        webhookRequest.AddParameter("payload_json", payloadjson, ParameterType.RequestBody);

        var response = await client.ExecuteAsync(webhookRequest);
        if (!response.IsSuccessful)
        {
            request.Status = ExportStatus.Failed;
            exportRequests.Insert(request).Execute();
        }


        static DateTime GetTimeKey(ExportRequest request, string key)
        {
            return DateTimeOffset.FromUnixTimeSeconds(long.Parse(request.Filters.GetValueOrDefault(key)
                ?? throw new CoflnetException($"missing_{key.ToLower()}", $"Missing {key} filter"))).DateTime;
        }

        static string GetBuyerUuid(SaveAuction a)
        {
            return a.Bids.OrderByDescending(b => b.Amount).FirstOrDefault()?.Bidder;
        }
        static (string, string) GetBuyerProfile(SaveAuction a)
        {
            var bid = a.Bids.OrderByDescending(b => b.Amount).FirstOrDefault();
            return (bid.Bidder ?? "none", bid.ProfileId ?? "none");
        }
    }



    public async Task<ExportRequest> RequestExport(ExportRequest request)
    {
        var waitingCount = pendingRequests.Where(r => r.ByEmail == request.ByEmail).Count();
        if (waitingCount >= 4)
        {
            logger.LogError($"Too many requests from {request.ByEmail} already in progress, please wait for one of them to finish");
            throw new CoflnetException("to_many_requests", "Too many requests from you already in progress, please wait for one of them to finish");
        }
        await exportRequests.Insert(request).ExecuteAsync();
        pendingRequests.Enqueue(request);

        await SendExportQueueNoteToDiscord(request);
        return request;
    }

    private async Task SendExportQueueNoteToDiscord(ExportRequest request)
    {
        var client = new RestClient();
        var webhookRequest = new RestRequest(request.DiscordWebhookUrl, Method.Post);
        var title = $"Export request for {request.ItemTag} queued";
        var payloadjson = JsonConvert.SerializeObject(new { content = title });
        webhookRequest.AddParameter("payload_json", payloadjson, ParameterType.RequestBody);
        var response = await client.ExecuteAsync(webhookRequest);
        if (!response.IsSuccessful)
        {
            logger.LogError($"Failed to send webhook for {request.ItemTag} export request");
        }
    }

    public async Task CancelExport(string email)
    {
        var request = pendingRequests.FirstOrDefault(r => r.ByEmail == email);
        if (request != null)
        {
            pendingRequests = new Queue<ExportRequest>(pendingRequests.Where(r => r.ByEmail != email));
            request.Status = ExportStatus.Cancelled;
            await exportRequests.Insert(request).ExecuteAsync();
        }
    }

    internal async Task<IEnumerable<ExportRequest>> GetExports(string email)
    {
        return await exportRequests.Where(r => r.ByEmail == email).ExecuteAsync();
    }

    public class ExportRequest
    {
        public string ByEmail { get; set; }
        public string DiscordWebhookUrl { get; set; }
        public DateTime RequestedAt { get; set; }
        public string ItemTag { get; set; }
        public Dictionary<string, string> Filters { get; set; }
        public ExportFlags Flags { get; set; }
        public List<string> Columns { get; set; }
        public string S3Url { get; set; }
        public ExportStatus Status { get; set; }
    }

    public enum ExportStatus
    {
        Pending,
        InProgress,
        Done,
        Failed,
        Cancelled
    }

    [Flags]
    public enum ExportFlags
    {
        None,
        IncludeSocial,
        InventoryCheck = 2,
        UniqueItems = 4
    }
}
