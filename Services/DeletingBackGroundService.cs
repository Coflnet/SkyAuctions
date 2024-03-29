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
            await Task.Delay(1000);
            await Delete();
        }
    }


    private async Task Delete()
    {
        var threeYearsAgo = DateTime.UtcNow.AddYears(-3);
        var biggestDate = new DateTime();
        while (biggestDate < threeYearsAgo)
        {
            using var scope = scopeFactory.CreateScope();
            using var context = new HypixelContext();
            var service = scope.ServiceProvider.GetRequiredService<RestoreService>();
            var batch = await context.Auctions.Take(100).ToListAsync();
            biggestDate = batch.LastOrDefault()?.End ?? DateTime.UtcNow;
            foreach (var item in batch)
            {
                if (item.End > threeYearsAgo)
                {
                    logger.LogInformation($"Reached end date {item.End}");
                    return;
                }
                try
                {
                    await service.RemoveAuction(Guid.Parse(item.Uuid));
                    logger.LogInformation($"Deleted {item.Uuid}");
                }
                catch (Exception e)
                {
                    logger.LogError(e, $"Error while deleting {item.Uuid}");
                    return;
                }
            }
        }
    }
}