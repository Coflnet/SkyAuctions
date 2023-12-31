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
using MoreLinq;
using System.Collections.Generic;

namespace Coflnet.Sky.Auctions.Services;

public class SellsCollector : BackgroundService
{
    private const string RedisProgressKey = "lastMigratedAuctionIndex";
    private const int BatchSize = 2500;
    private IServiceScopeFactory scopeFactory;
    private IConfiguration config;
    private ILogger<SellsCollector> logger;
    private ScyllaService scyllaService;
    private static int currentOffset = 0;
    private Prometheus.Counter consumeCount = Prometheus.Metrics.CreateCounter("sky_base_conume", "How many messages were consumed");
    private Prometheus.Counter batchInsertCount = Prometheus.Metrics.CreateCounter("sky_auctions_batch_count", "How many batches were sent to scylla");
    private static Prometheus.Gauge offsetGauge = Prometheus.Metrics.CreateGauge("sky_auctions_offset", "Current offset in the database");

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
        //wait Migrate();
        logger.LogInformation($"Finished completely");
        await Task.Delay(1000);

        while (!stoppingToken.IsCancellationRequested)
            await Coflnet.Kafka.KafkaConsumer.ConsumeBatch<SaveAuction>(
                    config,
                    new string[] { config["TOPICS:SOLD_AUCTION"], config["TOPICS:NEW_AUCTION"] },
                    async ab =>
                    {
                        await InsertSells(ab);
                        consumeCount.Inc(ab.Count());
                    },
                    stoppingToken,
                    "sky-auctions",
                    100
            );
    }

    private async Task Migrate()
    {
        var hadMore = true;
        currentOffset = await CacheService.Instance.GetFromRedis<int>(RedisProgressKey);
        logger.LogInformation($"Starting at {currentOffset}");
        var maxTime = DateTime.UtcNow.AddDays(-14);
        var tag = "";
        var channel = Channel.CreateUnbounded<Func<Task>>();
        StartWorkers(channel, 50);
        while (currentOffset < 597_500_000)
        {
            using var scope = scopeFactory.CreateScope();
            using var context = new HypixelContext();
            logger.LogInformation($"Loading batch {currentOffset} from db");
            var batch = await context.Auctions
                //.Where(a => a.ItemId == i && a.End < maxTime)
                .Where(a => a.Id >= currentOffset && a.Id < currentOffset + BatchSize)
                .Include(a => a.Bids).Include(a => a.Enchantments).Include(a => a.NbtData).Include(a => a.NBTLookup).Include(a => a.CoopMembers)
                //.Skip(offset).Take(batchSize)
                .AsNoTracking().ToListAsync();
            currentOffset += BatchSize;
            hadMore = batch.Count > 0;
            logger.LogInformation($"Loaded batch {currentOffset} from db");

            if (!hadMore)
                continue;
            foreach (var group in batch.GroupBy(a => a.Tag).Select(g => g.Batch(4)))
            {
                foreach (var groupBatch in group)
                {
                    channel.Writer.TryWrite(async () =>
                    {
                        try
                        {
                            await scyllaService.InsertAuctionsOfTag(groupBatch);
                            consumeCount.Inc(groupBatch.Count());
                            batchInsertCount.Inc();
                        }
                        catch (Cassandra.WriteTimeoutException e)
                        {
                            logger.LogError(e, $"Timeout Error while inserting {groupBatch.First().Tag}");
                            throw;
                        }
                        catch (System.Exception)
                        {
                            logger.LogError($"Error while inserting {groupBatch.First().Tag}\n{Newtonsoft.Json.JsonConvert.SerializeObject(groupBatch)}");
                            throw;
                        }
                    });
                }
                if (channel.Reader.Count > 500)
                    await Task.Delay(20);
            }
            foreach (var item in batch.SelectMany(b =>
            {
                foreach (var bid in b.Bids)
                {
                    bid.AuctionId = b.Uuid;
                }
                return b.Bids;
            }).GroupBy(g => g.Bidder).Batch(3).ToList())
            {
                channel.Writer.TryWrite(async () =>
                {
                    try
                    {
                        await scyllaService.InsertBids(item.SelectMany(i => i));
                        batchInsertCount.Inc();
                    }
                    catch (System.Exception)
                    {
                        logger.LogError($"Error while inserting {item.First().Key}\n{Newtonsoft.Json.JsonConvert.SerializeObject(item)}");
                        throw;
                    }
                });
                if (channel.Reader.Count > 200)
                    await Task.Delay(20);
            }
            channel.Writer.TryWrite(async () =>
            {
                var toStore = currentOffset - BatchSize * 5;
                await SetOffset(toStore);
                logger.LogInformation($"Reached offset {currentOffset} {tag} {batch.Last().End}");
            });
            tag = batch.LastOrDefault()?.Tag;
        }
    }

    private async Task InsertSells(IEnumerable<SaveAuction> ab)
    {
        var bidsTask = Parallel.ForEachAsync(ab.SelectMany(a =>
        {
            foreach (var item in a.Bids)
            {
                item.AuctionId = a.Uuid;
            }
            return a.Bids;
        }).GroupBy(g => g.Bidder).Batch(20).ToList(), async (b, c) =>
        {
            await scyllaService.InsertBids(b.OrderByDescending(b => b.Count()).SelectMany(i => i));
        });
        await Parallel.ForEachAsync(ab.GroupBy(a => a.Tag).Select(g => g.Batch(10)).SelectMany(g => g), async (a, c) =>
        {
            try
            {
                await scyllaService.InsertAuctionsOfTag(a);
            }
            catch (System.Exception)
            {
                logger.LogError($"Error while inserting {a.First().Tag}\n{Newtonsoft.Json.JsonConvert.SerializeObject(a)}");
                throw;
            }
        });
        await Task.WhenAll(bidsTask);
    }

    private void StartWorkers(Channel<Func<Task>> channel, int count)
    {
        var errorCount = 0;
        for (int i = 0; i < count; i++)
        {
            _ = Task.Run(async () =>
            {
                while (await channel.Reader.WaitToReadAsync())
                {
                    while (channel.Reader.TryRead(out var action))
                    {
                        try { await action(); errorCount = 0; }
                        catch (Exception e)
                        {
                            logger.LogError(e, "Error while executing action");
                            channel.Writer.TryWrite(action);
                            // thread safe increment
                            Interlocked.Increment(ref errorCount);
                            await Task.Delay(100 * errorCount);
                        }
                    }
                }
            });
        }
    }

    public static async Task SetOffset(int toStore)
    {
        if (Math.Abs(toStore - currentOffset) > BatchSize * 10)
            currentOffset = toStore;
        offsetGauge.Set(toStore);
        await CacheService.Instance.SaveInRedis(RedisProgressKey, toStore);
    }

    private BaseService GetService()
    {
        return scopeFactory.CreateScope().ServiceProvider.GetRequiredService<BaseService>();
    }
}