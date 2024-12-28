using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System.Linq;
using Microsoft.Extensions.Logging;
using Coflnet.Sky.Core;
using System;
using MoreLinq;
using System.Collections.Generic;

namespace Coflnet.Sky.Auctions.Services;

public class DeletingBackGroundService : BackgroundService
{

    private IServiceScopeFactory scopeFactory;
    private ILogger<DeletingBackGroundService> logger;
    private Prometheus.Counter deleteCount = Prometheus.Metrics.CreateCounter("sky_auctions_delete_count", "How many auctions were deleted");

    public DeletingBackGroundService(IServiceScopeFactory scopeFactory, ILogger<DeletingBackGroundService> logger)
    {
        this.scopeFactory = scopeFactory;
        this.logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            await Delete();
            await Task.Delay(5000);
        }
    }


    private async Task Delete()
    {
        var backoff = new DateTime(2024, 12, 29) - DateTime.UtcNow;
        if(backoff.TotalMilliseconds > 0)
        {
            await Task.Delay(backoff);
        }
        var threeYearsAgo = DateTime.UtcNow.AddYears(-3);
        var biggestDate = new DateTime();
        var highest = 0;
        while (biggestDate < threeYearsAgo)
        {
            using var context = new HypixelContext();
            var batch = await context.Auctions.Where(a=>a.Id > highest).OrderBy(a=>a.Id).Take(128).ToListAsync();
            biggestDate = batch.LastOrDefault()?.End ?? DateTime.UtcNow;
            logger.LogInformation("Deleting batch");
            var w1 = DeleteBatch(threeYearsAgo, batch.Take(64).ToList());
            var w2 = DeleteBatch(threeYearsAgo, batch.Skip(64).ToList());
            await Task.WhenAll(w1, w2);
            logger.LogInformation("sheduled batch for delete {id}", highest);
            highest = batch.Max(b=>b.Id);
            await Task.Delay(60_000);
        }
    }

    private async Task DeleteBatch(DateTime threeYearsAgo, List<SaveAuction> batch)
    {
        var scope = scopeFactory.CreateScope();
        var service = scope.ServiceProvider.GetRequiredService<RestoreService>();
        foreach (var item in batch.OrderBy(i => i.End).Batch(32))
        {
            if (item.First().End > threeYearsAgo)
            {
                logger.LogInformation($"Reached end date {item.First().End}");
                return;
            }
            var guids = item.Select(i => Guid.Parse(i.Uuid)).ToArray();
            try
            {
                await service.RemoveAuction(guids);
                logger.LogInformation($"Deleted {string.Join(", ", guids)}");
            }
            catch (Exception e)
            {
                logger.LogError(e, $"Error while deleting {string.Join(", ", guids)}");
                return;
            }
        }
    }
}