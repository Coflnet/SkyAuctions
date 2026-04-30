using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Data.Linq;
using Cassandra.Mapping;
using Coflnet.Sky.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using RestSharp;

namespace Coflnet.Sky.Auctions.Services;

public class ExportService : BackgroundService
{
    private readonly Table<ExportRequest> exportJobs;
    private readonly QueryService queryService;
    private readonly S3StorageService s3;
    private readonly ILogger<ExportService> logger;
    private readonly ConcurrentQueue<Guid> pendingJobs = new();
    private readonly ConcurrentDictionary<Guid, ExportRequest> knownJobs = new();
    private readonly TimeSpan signedUrlDuration;

    public ExportService(
        ISession session,
        QueryService queryService,
        ILogger<ExportService> logger,
        IConfiguration config,
        S3StorageService s3 = null)
    {
        var mapping = new MappingConfiguration().Define(
            new Map<ExportRequest>()
                .TableName("export_jobs_v2")
                .PartitionKey(e => e.ByEmail)
                .ClusteringKey(
                    new Tuple<string, SortOrder>("requestedat", SortOrder.Descending),
                    new Tuple<string, SortOrder>("jobid", SortOrder.Ascending))
                .Column(e => e.Flags, cm => cm.WithDbType<int>())
                .Column(e => e.Status, cm => cm.WithDbType<int>())
        );

        exportJobs = new Table<ExportRequest>(session, mapping);
        exportJobs.CreateIfNotExists();
        this.queryService = queryService;
        this.s3 = s3;
        this.logger = logger;

        var signedMinutes = int.TryParse(config["S3:ExportSignedUrlMinutes"], out var minutes) ? minutes : 60 * 24;
        signedUrlDuration = TimeSpan.FromMinutes(Math.Max(5, signedMinutes));
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Export service started");
        while (!stoppingToken.IsCancellationRequested)
        {
            if (!pendingJobs.TryDequeue(out var jobId))
            {
                await Task.Delay(2000, stoppingToken);
                continue;
            }

            if (!knownJobs.TryGetValue(jobId, out var request))
            {
                continue;
            }

            if (request.AbortRequested || request.Status != ExportStatus.Pending)
            {
                request.Status = ExportStatus.Aborted;
                await Save(request);
                continue;
            }

            request.Status = ExportStatus.InProgress;
            await Save(request);

            try
            {
                await RunExport(request, stoppingToken);
            }
            catch (OperationCanceledException) when (request.AbortRequested)
            {
                request.Status = ExportStatus.Aborted;
                request.CompletedAt = DateTime.UtcNow;
                await Save(request);
            }
            catch (Exception e)
            {
                logger.LogError(e, "Failed export job {JobId} for {Email}", request.JobId, request.ByEmail);
                request.Status = ExportStatus.Failed;
                request.Error = e.Message.Truncate(1024);
                request.CompletedAt = DateTime.UtcNow;
                await Save(request);
                try
                {
                    await SendExportFailureToDiscord(request, e);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Failed to send export failure webhook");
                }
            }
        }
    }

    private async Task RunExport(ExportRequest request, CancellationToken ct)
    {
        if (s3 == null)
        {
            throw new CoflnetException("export_storage_disabled", "S3 export storage is not enabled");
        }

        logger.LogInformation(
            "Exporting job {JobId} for {Email}: {Tag} {Start}..{End}",
            request.JobId,
            request.ByEmail,
            request.ItemTag,
            request.Start,
            request.End);

        if (request.End <= request.Start)
        {
            throw new CoflnetException("invalid_time_range", "End has to be after start");
        }

        var maxAgeStart = DateTime.UtcNow.AddDays(-14);
        if (request.Start < maxAgeStart)
        {
            throw new CoflnetException("start_too_old", "Exports currently support at most the last 14 days");
        }

        if (request.End > DateTime.UtcNow.AddMinutes(5))
        {
            throw new CoflnetException("end_in_future", "End time may not be in the future");
        }

        var filters = (request.Filters ?? new Dictionary<string, string>())
            .Where(kv => kv.Key != "EndAfter" && kv.Key != "EndBefore")
            .ToDictionary(k => k.Key, v => v.Value);

        var users = (request.Users ?? new List<string>())
            .Where(u => !string.IsNullOrWhiteSpace(u))
            .Select(u => u.Trim().ToLowerInvariant())
            .ToHashSet();

        var columns = BuildColumns(request.Columns);
        var builder = new StringBuilder();
        builder.AppendLine(string.Join(',', columns.Select(c => c.Name)));

        var rowCount = 0;
        await foreach (var auction in queryService.GetFiltered(request.ItemTag, filters, request.Start, request.End, int.MaxValue))
        {
            if (request.AbortRequested)
            {
                throw new OperationCanceledException("Export aborted by user");
            }

            if (users.Count > 0 && !MatchesUserFilter(auction, users))
            {
                continue;
            }

            builder.AppendLine(string.Join(',', columns.Select(c => EscapeCsv(c.Value(auction)))));
            rowCount++;
        }

        var key = $"exports/{request.ByEmail}/{request.RequestedAt:yyyy-MM-dd}/{request.JobId:N}.csv";
        var bytes = Encoding.UTF8.GetBytes(builder.ToString());
        await s3.PutBlob(key, bytes, "text/csv", ct);

        request.S3Key = key;
        request.SignedUrl = s3.GetSignedDownloadUrl(key, signedUrlDuration);
        request.SignedUrlExpiresAt = DateTime.UtcNow.Add(signedUrlDuration);
        request.Status = ExportStatus.Done;
        request.RowCount = rowCount;
        request.Error = null;
        request.CompletedAt = DateTime.UtcNow;
        await Save(request);

        logger.LogInformation("Completed export job {JobId}: {Rows} rows", request.JobId, rowCount);

        try
        {
            await SendExportSuccessToDiscord(request);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to send export success webhook");
        }
    }

    public async Task<ExportRequest> RequestExport(ExportRequest request)
    {
        request.ByEmail = (request.ByEmail ?? string.Empty).Trim().ToLowerInvariant();
        if (string.IsNullOrWhiteSpace(request.ByEmail))
        {
            throw new CoflnetException("missing_email", "ByEmail is required");
        }

        request.ItemTag = request.ItemTag?.Trim();
        if (string.IsNullOrWhiteSpace(request.ItemTag))
        {
            throw new CoflnetException("missing_item", "ItemTag is required");
        }

        request.JobId = request.JobId == Guid.Empty ? Guid.NewGuid() : request.JobId;
        request.RequestedAt = request.RequestedAt == default ? DateTime.UtcNow : request.RequestedAt;

        if (request.Start == default && request.Filters?.TryGetValue("EndAfter", out var startUnix) == true)
        {
            request.Start = DateTimeOffset.FromUnixTimeSeconds(long.Parse(startUnix)).UtcDateTime;
        }
        if (request.End == default && request.Filters?.TryGetValue("EndBefore", out var endUnix) == true)
        {
            request.End = DateTimeOffset.FromUnixTimeSeconds(long.Parse(endUnix)).UtcDateTime;
        }

        if (request.Start == default || request.End == default)
        {
            throw new CoflnetException("missing_time_range", "Start and End are required");
        }

        request.Status = ExportStatus.Pending;
        request.AbortRequested = false;

        await Save(request);
        knownJobs[request.JobId] = request;
        pendingJobs.Enqueue(request.JobId);

        try
        {
            await SendExportQueuedToDiscord(request);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to send export queued webhook");
        }

        return request;
    }

    public async Task<IEnumerable<ExportRequest>> GetJobs(string email)
    {
        var normalized = NormalizeEmail(email);
        var jobs = await exportJobs.Where(r => r.ByEmail == normalized).ExecuteAsync();
        return jobs.OrderByDescending(j => j.RequestedAt);
    }

    public async Task<IEnumerable<ExportRequest>> GetScheduled(string email)
    {
        var jobs = await GetJobs(email);
        return jobs.Where(j => j.Status == ExportStatus.Pending || j.Status == ExportStatus.InProgress);
    }

    public async Task<IEnumerable<ExportRequest>> GetExports(string email)
    {
        var jobs = await GetJobs(email);
        return jobs.Where(j =>
            j.Status == ExportStatus.Done ||
            j.Status == ExportStatus.Failed ||
            j.Status == ExportStatus.Deleted);
    }

    public async Task AbortScheduled(string email, Guid jobId)
    {
        var job = await GetJob(email, jobId);
        if (job == null)
        {
            throw new CoflnetException("not_found", "Export job not found");
        }

        if (job.Status == ExportStatus.Done || job.Status == ExportStatus.Deleted)
        {
            throw new CoflnetException("not_abortable", "Completed exports cannot be aborted");
        }

        job.AbortRequested = true;
        if (job.Status == ExportStatus.Pending)
        {
            job.Status = ExportStatus.Aborted;
            job.CompletedAt = DateTime.UtcNow;
        }

        await Save(job);
        knownJobs[job.JobId] = job;
    }

    public async Task DeleteExport(string email, Guid jobId)
    {
        if (s3 == null)
        {
            throw new CoflnetException("export_storage_disabled", "S3 export storage is not enabled");
        }

        var job = await GetJob(email, jobId);
        if (job == null)
        {
            throw new CoflnetException("not_found", "Export job not found");
        }

        if (job.Status == ExportStatus.InProgress || job.Status == ExportStatus.Pending)
        {
            throw new CoflnetException("job_active", "Abort scheduled export before deleting");
        }

        if (!string.IsNullOrWhiteSpace(job.S3Key))
        {
            await s3.DeleteBlob(job.S3Key);
        }

        job.SignedUrl = null;
        job.SignedUrlExpiresAt = null;
        job.Status = ExportStatus.Deleted;
        job.CompletedAt = DateTime.UtcNow;

        await Save(job);
        knownJobs[job.JobId] = job;
    }

    private async Task<ExportRequest> GetJob(string email, Guid jobId)
    {
        var jobs = await exportJobs.Where(r => r.ByEmail == NormalizeEmail(email)).ExecuteAsync();
        return jobs.FirstOrDefault(j => j.JobId == jobId);
    }

    private Task Save(ExportRequest request)
    {
        return exportJobs.Insert(request).ExecuteAsync();
    }

    private static string NormalizeEmail(string email) => (email ?? string.Empty).Trim().ToLowerInvariant();

    private async Task SendDiscordWebhook(string webhookUrl, string content, string context)
    {
        if (string.IsNullOrWhiteSpace(webhookUrl))
            return;
        var client = new RestClient();
        var webhookRequest = new RestRequest(webhookUrl, Method.Post);
        var payloadJson = JsonConvert.SerializeObject(new { content });
        webhookRequest.AddParameter("payload_json", payloadJson, ParameterType.RequestBody);
        var response = await client.ExecuteAsync(webhookRequest);
        if (!response.IsSuccessful)
        {
            logger.LogError(
                "Failed to send {Context} webhook: {Status} {Error}",
                context,
                response.StatusCode,
                response.ErrorMessage);
        }
    }

    private Task SendExportQueuedToDiscord(ExportRequest request)
    {
        var content = $"Export request for {request.ItemTag} queued\nUser: {request.ByEmail}\nJob: {request.JobId}";
        return SendDiscordWebhook(request.DiscordWebhookUrl, content, "queued");
    }

    private Task SendExportSuccessToDiscord(ExportRequest request)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"Export finished for {request.ItemTag}");
        sb.AppendLine($"User: {request.ByEmail}");
        sb.AppendLine($"Status: {request.Status}");
        sb.AppendLine($"Rows: {request.RowCount}");
        if (!string.IsNullOrEmpty(request.SignedUrl))
            sb.AppendLine($"Download: {request.SignedUrl}");
        return SendDiscordWebhook(request.DiscordWebhookUrl, sb.ToString(), "success");
    }

    private Task SendExportFailureToDiscord(ExportRequest request, Exception e)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"Export failed for {request.ItemTag}");
        sb.AppendLine($"User: {request.ByEmail}");
        sb.AppendLine($"Status: {request.Status}");
        sb.AppendLine($"Error: {e.Message}");
        if (!string.IsNullOrEmpty(e.StackTrace))
        {
            var firstLine = e.StackTrace.Split('\n').FirstOrDefault()?.Trim();
            if (!string.IsNullOrEmpty(firstLine))
                sb.AppendLine($"Trace: {firstLine}");
        }
        return SendDiscordWebhook(request.DiscordWebhookUrl, sb.ToString(), "failure");
    }

    private static bool MatchesUserFilter(SaveAuction auction, HashSet<string> users)
    {
        if (!string.IsNullOrWhiteSpace(auction.AuctioneerId) && users.Contains(auction.AuctioneerId.ToLowerInvariant()))
        {
            return true;
        }

        foreach (var bid in auction.Bids ?? new List<SaveBids>())
        {
            if (!string.IsNullOrWhiteSpace(bid.Bidder) && users.Contains(bid.Bidder.ToLowerInvariant()))
            {
                return true;
            }
        }

        return false;
    }

    private static string EscapeCsv(string value)
    {
        value ??= string.Empty;
        var escaped = value.Replace("\"", "\"\"");
        if (escaped.Contains(',') || escaped.Contains('"') || escaped.Contains('\n') || escaped.Contains('\r'))
        {
            return $"\"{escaped}\"";
        }

        return escaped;
    }

    private static List<(string Name, Func<SaveAuction, string> Value)> BuildColumns(List<string> columns)
    {
        var mapping = new Dictionary<string, Func<SaveAuction, string>>(StringComparer.OrdinalIgnoreCase)
        {
            { "uuid", a => a.Uuid.ToString() },
            { "itemTag", a => a.Tag ?? string.Empty },
            { "itemName", a => a.ItemName ?? string.Empty },
            { "highestBidAmount", a => a.HighestBidAmount.ToString() },
            { "bin", a => a.Bin.ToString() },
            { "endedAt", a => a.End.ToString("O") },
            { "startedAt", a => a.Start.ToString("O") },
            { "sellerUuid", a => a.AuctioneerId ?? string.Empty },
            { "highestBidderUuid", a => a.Bids?.OrderByDescending(b => b.Amount).FirstOrDefault()?.Bidder ?? string.Empty },
            { "itemUid", a => a.FlatenedNBT.GetValueOrDefault("uid")?.ToString() ?? string.Empty },
        };

        var requested = columns == null || columns.Count == 0
            ? new List<string> { "uuid", "itemTag", "itemName", "highestBidAmount", "bin", "startedAt", "endedAt", "sellerUuid", "highestBidderUuid", "itemUid" }
            : columns;

        var result = new List<(string Name, Func<SaveAuction, string> Value)>();
        foreach (var column in requested)
        {
            if (!mapping.TryGetValue(column, out var selector))
            {
                throw new CoflnetException("invalid_column", $"Invalid column {column}");
            }
            result.Add((column, selector));
        }

        return result;
    }

    public class ExportRequest
    {
        public Guid JobId { get; set; }
        public string ByEmail { get; set; }
        public DateTime RequestedAt { get; set; }
        public string ItemTag { get; set; }
        public DateTime Start { get; set; }
        public DateTime End { get; set; }
        public Dictionary<string, string> Filters { get; set; }
        public List<string> Users { get; set; }
        public ExportFlags Flags { get; set; }
        public List<string> Columns { get; set; }
        public string S3Key { get; set; }
        public string SignedUrl { get; set; }
        public DateTime? SignedUrlExpiresAt { get; set; }
        public int RowCount { get; set; }
        public ExportStatus Status { get; set; }
        public bool AbortRequested { get; set; }
        public DateTime? CompletedAt { get; set; }
        public string Error { get; set; }
        public string DiscordWebhookUrl { get; set; }
    }

    public enum ExportStatus
    {
        Pending,
        InProgress,
        Done,
        Failed,
        Aborted,
        Deleted
    }

    [Flags]
    public enum ExportFlags
    {
        None,
        IncludeSocial = 1,
        InventoryCheck = 2,
        UniqueItems = 4
    }
}
