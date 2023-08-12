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
using System.Threading.Channels;

namespace Coflnet.Sky.Auctions.Services;

public class SellsCollector : BackgroundService
{
    private const string RedisProgressKey = "lastMigratedAuctionIndex";
    private IServiceScopeFactory scopeFactory;
    private IConfiguration config;
    private ILogger<SellsCollector> logger;
    private ScyllaService scyllaService;
    private static int currentOffset = 0;
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
        var hadMore = true;
        currentOffset = await CacheService.Instance.GetFromRedis<int>(RedisProgressKey);
        logger.LogInformation($"Starting at {currentOffset}");
        var maxTime = DateTime.UtcNow.AddDays(-14);
        var tag = "";
        var channel = Channel.CreateUnbounded<Func<Task>>();
        for (int i = 0; i < 150; i++)
        {
            _ = Task.Run(async () =>
            {
                while (await channel.Reader.WaitToReadAsync())
                {
                    var errorCount = 0;
                    while (channel.Reader.TryRead(out var action))
                    {
                        try { await action(); errorCount = 0; }
                        catch (Exception e)
                        {
                            logger.LogError(e, "Error while executing action");
                            await Task.Delay(500 * (++errorCount));
                            channel.Writer.TryWrite(action);
                        }
                    }
                }
            });
        }
        while (currentOffset < 600_000_000)
        {
            using var scope = scopeFactory.CreateScope();
            using var context = new HypixelContext();
            logger.LogDebug($"Loading batch {currentOffset} from db");
            var batch = await context.Auctions
                //.Where(a => a.ItemId == i && a.End < maxTime)
                .Where(a => a.Id >= currentOffset && a.Id < currentOffset + batchSize)
                .Include(a => a.Bids).Include(a => a.Enchantments).Include(a => a.NbtData).Include(a => a.NBTLookup).Include(a => a.CoopMembers)
                //.Skip(offset).Take(batchSize)
                .AsNoTracking().ToListAsync();
            currentOffset += batchSize;
            hadMore = batch.Count > 0;

            if (!hadMore)
                continue;
            foreach (var auction in batch)
            {
                channel.Writer.TryWrite(() => scyllaService.InsertAuction(auction));
                if (channel.Reader.Count > 1000)
                    await Task.Delay(20);
            }
            channel.Writer.TryWrite(async () =>
            {
                var toStore = currentOffset - batchSize * 2;
                await SetOffset(toStore);
                logger.LogInformation($"Reached offset {currentOffset} {tag} {batch.Last().End}");
            });
            tag = batch.LastOrDefault()?.Tag;
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

    public static async Task SetOffset(int toStore)
    {
        if (Math.Abs(toStore - currentOffset) > 10000)
            currentOffset = toStore;
        await CacheService.Instance.SaveInRedis(RedisProgressKey, toStore);
    }

    private BaseService GetService()
    {
        return scopeFactory.CreateScope().ServiceProvider.GetRequiredService<BaseService>();
    }
}