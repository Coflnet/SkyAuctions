using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3;
using Coflnet.Sky.Auctions.Models;
using Coflnet.Sky.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Prometheus;

namespace Coflnet.Sky.Auctions.Services;

/// <summary>
/// Passively reads from S3 in parallel to a Scylla read and reports differences via metrics + logs.
/// Used to validate archive coverage before flipping the migration to active mode.
/// Never throws back to the caller and never alters the response.
/// </summary>
public class ShadowReadService
{
    private readonly S3AuctionBlobService auctionBlobs;
    private readonly S3PlayerIndexService playerIndex;
    private readonly ILogger<ShadowReadService> logger;
    private readonly bool enabled;
    private readonly int sampleEveryN;
    private int counter;

    private static readonly Counter ScyllaOnly =
        Metrics.CreateCounter("sky_auctions_shadow_scylla_only_total", "Auctions present in Scylla but missing in S3 archive", "scope");
    private static readonly Counter S3Only =
        Metrics.CreateCounter("sky_auctions_shadow_s3_only_total", "Auctions present in S3 archive but missing in Scylla read window", "scope");
    private static readonly Counter Matches =
        Metrics.CreateCounter("sky_auctions_shadow_match_total", "Auctions that match in both Scylla and S3", "scope");
    private static readonly Counter Errors =
        Metrics.CreateCounter("sky_auctions_shadow_error_total", "Shadow read failures", "scope");
    private static readonly Counter Skipped =
        Metrics.CreateCounter("sky_auctions_shadow_not_yet_archived_total", "Shadow reads skipped because the S3 blob does not exist yet", "scope");

    /// <summary>
    /// Creates a new <see cref="ShadowReadService"/>. Disabled unless <c>S3:ShadowMode</c> is true.
    /// </summary>
    public ShadowReadService(
        IConfiguration config,
        ILogger<ShadowReadService> logger,
        S3AuctionBlobService auctionBlobs = null,
        S3PlayerIndexService playerIndex = null)
    {
        this.auctionBlobs = auctionBlobs;
        this.playerIndex = playerIndex;
        this.logger = logger;
        enabled = config.GetValue<bool>("S3:ShadowMode")
            || config.GetValue<bool>("S3_MIGRATION:SHADOW_MODE");
        sampleEveryN = config.GetValue<int?>("S3:ShadowSampleEveryN") ?? 1;
        if (sampleEveryN < 1) sampleEveryN = 1;
    }

    /// <summary>
    /// True when shadow reads should be issued.
    /// </summary>
    public bool IsEnabled => enabled && auctionBlobs != null;

    private bool ShouldSample()
    {
        if (sampleEveryN <= 1) return true;
        return Interlocked.Increment(ref counter) % sampleEveryN == 0;
    }

    /// <summary>
    /// Compare a Scylla result for (tag, month) against the S3 archive blob in the background.
    /// </summary>
    public void CompareTagMonth(string scope, string tag, DateTime month, IReadOnlyCollection<SaveAuction> scyllaAuctions)
    {
        if (!IsEnabled || string.IsNullOrEmpty(tag) || !ShouldSample()) return;
        _ = Task.Run(() => CompareTagMonthInternal(scope, tag, month, scyllaAuctions));
    }

    private async Task CompareTagMonthInternal(string scope, string tag, DateTime month, IReadOnlyCollection<SaveAuction> scyllaAuctions)
    {
        try
        {
            var monthStart = new DateTime(month.Year, month.Month, 1, 0, 0, 0, DateTimeKind.Utc);
            List<SaveAuction> archived;
            try
            {
                archived = await auctionBlobs.ReadAuctions(tag, monthStart);
            }
            catch (AmazonS3Exception e) when (e.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                Skipped.WithLabels(scope).Inc();
                return;
            }

            var archivedIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var a in archived)
            {
                var n = AuctionIdentity.NormalizeUuid(a.Uuid);
                if (!string.IsNullOrEmpty(n)) archivedIds.Add(n);
            }
            var scyllaIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var a in scyllaAuctions)
            {
                var n = AuctionIdentity.NormalizeUuid(a.Uuid);
                if (!string.IsNullOrEmpty(n)) scyllaIds.Add(n);
            }

            var scyllaOnly = scyllaIds.Count(id => !archivedIds.Contains(id));
            var s3Only = archivedIds.Count(id => !scyllaIds.Contains(id));
            var matches = scyllaIds.Count - scyllaOnly;

            if (matches > 0) Matches.WithLabels(scope).Inc(matches);
            if (scyllaOnly > 0)
            {
                ScyllaOnly.WithLabels(scope).Inc(scyllaOnly);
                logger.LogWarning("Shadow {Scope} {Tag}/{Month:yyyy-MM}: {Missing} Scylla rows missing in S3 (matches={Matches}, s3Only={S3Only})",
                    scope, tag, monthStart, scyllaOnly, matches, s3Only);
            }
            if (s3Only > 0) S3Only.WithLabels(scope).Inc(s3Only);
        }
        catch (Exception e)
        {
            Errors.WithLabels(scope).Inc();
            logger.LogDebug(e, "Shadow read failure for {Scope} {Tag} {Month:yyyy-MM}", scope, tag, month);
        }
    }

    /// <summary>
    /// Compare a Scylla player auctions result against the S3 player index for the years the auctions span.
    /// </summary>
    public void ComparePlayer(Guid player, IReadOnlyCollection<SaveAuction> scyllaAuctions)
    {
        if (!IsEnabled || playerIndex == null || !ShouldSample()) return;
        var years = scyllaAuctions.Select(a => a.End.Year).Distinct().ToArray();
        _ = Task.Run(() => ComparePlayerInternal(player, years, scyllaAuctions));
    }

    private async Task ComparePlayerInternal(Guid player, int[] years, IReadOnlyCollection<SaveAuction> scyllaAuctions)
    {
        const string scope = "player";
        try
        {
            var archivedIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var year in years)
            {
                try
                {
                    var entries = await playerIndex.GetParticipation(player, year);
                    foreach (var e in entries) archivedIds.Add(e.AuctionUid.ToString("X"));
                }
                catch (AmazonS3Exception e) when (e.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    Skipped.WithLabels(scope).Inc();
                }
            }

            var scyllaIds = scyllaAuctions
                .Select(a => AuctionService.Instance.GetId(AuctionIdentity.NormalizeUuid(a.Uuid) ?? a.Uuid).ToString("X"))
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            var scyllaOnly = scyllaIds.Count(id => !archivedIds.Contains(id));
            var matches = scyllaIds.Count - scyllaOnly;
            if (matches > 0) Matches.WithLabels(scope).Inc(matches);
            if (scyllaOnly > 0)
            {
                ScyllaOnly.WithLabels(scope).Inc(scyllaOnly);
                logger.LogWarning("Shadow player {Player}: {Missing} Scylla rows missing in S3 player index (matches={Matches})",
                    player, scyllaOnly, matches);
            }
        }
        catch (Exception e)
        {
            Errors.WithLabels(scope).Inc();
            logger.LogDebug(e, "Shadow player read failure for {Player}", player);
        }
    }
}
