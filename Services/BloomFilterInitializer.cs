using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Coflnet.Sky.Auctions.Services;

/// <summary>
/// Initializes the bloom filter from S3 on application startup
/// </summary>
public class BloomFilterInitializer : IHostedService
{
    private readonly BloomFilterService bloomFilter;
    private readonly ILogger<BloomFilterInitializer> logger;

    /// <summary>
    /// Creates a new BloomFilterInitializer
    /// </summary>
    public BloomFilterInitializer(BloomFilterService bloomFilter, ILogger<BloomFilterInitializer> logger)
    {
        this.bloomFilter = bloomFilter;
        this.logger = logger;
    }

    /// <summary>
    /// Initializes the bloom filter on startup
    /// </summary>
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Initializing bloom filter...");
        await bloomFilter.Initialize(ct: cancellationToken);
        logger.LogInformation("Bloom filter initialized");
    }

    /// <summary>
    /// Flushes the bloom filter on shutdown
    /// </summary>
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Flushing bloom filter on shutdown...");
        await bloomFilter.FlushIfDirty(cancellationToken);
    }
}
