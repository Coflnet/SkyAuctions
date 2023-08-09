using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.Auctions.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using System.Linq;
using Microsoft.Extensions.Logging;
using Coflnet.Sky.Auctions.Controllers;
using Coflnet.Sky.Core;
using System;

namespace Coflnet.Sky.Auctions.Services;

public class SellsCollector : BackgroundService
{
    private IServiceScopeFactory scopeFactory;
    private IConfiguration config;
    private ILogger<SellsCollector> logger;
    private ScyllaService scyllaService;
    private Prometheus.Counter consumeCount = Prometheus.Metrics.CreateCounter("sky_base_conume", "How many messages were consumed");

    public SellsCollector(
        IServiceScopeFactory scopeFactory, IConfiguration config, ILogger<SellsCollector> logger, ScyllaService scyllaService)
    {
        this.scopeFactory = scopeFactory;
        this.config = config;
        this.logger = logger;
        this.scyllaService = scyllaService;
    }
    /// <summary>
    /// Called by asp.net on startup
    /// </summary>
    /// <param name="stoppingToken">is canceled when the applications stops</param>
    /// <returns></returns>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await scyllaService.Create();
        var batchSize = 1000;
        using var scope = scopeFactory.CreateScope();
        using var context = new HypixelContext();
        var hadMore = true;
        var offset = 0;//33571000;
        var maxTime = DateTime.UtcNow.AddDays(-14);
        var tag = "";
        Task lastTask = null;
        while (batchSize < 600_000_000)
        {
            var batch = await context.Auctions
                //.Where(a => a.ItemId == i && a.End < maxTime)
                .Where(a => a.Id >= offset && a.Id < offset + batchSize)
                .Include(a => a.Bids).Include(a => a.Enchantments).Include(a => a.NbtData).Include(a => a.NBTLookup).Include(a => a.CoopMembers)
                //.Skip(offset).Take(batchSize)
                .AsNoTracking().ToListAsync();
            offset += batchSize;
            if (lastTask != null)
                await lastTask;
            hadMore = batch.Count > 0;
            await Task.Delay(50);

            if (!hadMore)
                continue;
            lastTask = scyllaService.InsertAuctions(batch);
            tag = batch.LastOrDefault()?.Tag;
            Console.Write($"\rFinished {offset} {tag} {batch.Last().End}");
        }
        logger.LogInformation($"Finished completely");

        await Coflnet.Kafka.KafkaConsumer.ConsumeBatch<SaveAuction>(
                    config,
                    config["TOPICS:SOLD_AUCTION"],
                    scyllaService.InsertAuctions,
                    stoppingToken,
                    "sky-auctions",
                    100
        );
    }

    private BaseService GetService()
    {
        return scopeFactory.CreateScope().ServiceProvider.GetRequiredService<BaseService>();
    }
}