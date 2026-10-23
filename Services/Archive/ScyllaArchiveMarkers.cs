using System;
using System.Globalization;
using System.Threading.Tasks;
using Coflnet.Sky.Core.Archive;
using Microsoft.Extensions.Configuration;
using StackExchange.Redis;

namespace Coflnet.Sky.Auctions.Archive
{
    /// <summary>
    /// Progress markers of the Scylla archive sweep. SkyAuctions must not write MariaDB (its DBConnection is a read-only user),
    /// so tag-month, month and sweep progress live in Redis (<see cref="RedisScyllaArchiveMarkers"/>).
    /// </summary>
    public interface IScyllaArchiveMarkers
    {
        /// <summary>Value of a marker, null when it is not set.</summary>
        Task<string> GetAsync(string key);

        /// <summary>Set a marker (no expiry).</summary>
        Task SetAsync(string key, string value);
    }

    /// <summary>Marker keys and values (<c>auctions_archive:v2:*</c>, next to the bazaar archive markers in sky-settings-redis).</summary>
    public static class ScyllaArchiveMarkerKeys
    {
        /// <summary>Common prefix.</summary>
        public const string Prefix = "auctions_archive:v2:";
        /// <summary>Set when every month in scope is complete.</summary>
        public const string SweepComplete = Prefix + "sweep_complete";

        /// <summary><c>auctions_archive:v2:{tag}:{yyyy-MM}</c> = <see cref="Archived"/> | <see cref="Empty"/> | <see cref="OverlapOnly"/>.</summary>
        public static string TagMonth(string tag, int month) => Prefix + tag + ":" + ArchiveMonths.Format(month);

        /// <summary><c>auctions_archive:v2:month:{yyyy-MM}</c> = iso time every tag of the month was done.</summary>
        public static string Month(int month) => Prefix + "month:" + ArchiveMonths.Format(month);

        /// <summary><c>archived:{scyllaOnlyRows}:{newParts}:{iso}</c></summary>
        public static string Archived(int scyllaOnlyRows, int newParts, DateTime at) => FormattableString.Invariant($"archived:{scyllaOnlyRows}:{newParts}:{Iso(at)}");

        /// <summary><c>empty:{iso}</c>: neither phase 1 nor Scylla had a row of the tag-month.</summary>
        public static string Empty(DateTime at) => "empty:" + Iso(at);

        /// <summary><c>overlap_only:{iso}</c>: phase 1 already had every Scylla row.</summary>
        public static string OverlapOnly(DateTime at) => "overlap_only:" + Iso(at);

        /// <summary>Round-trip UTC timestamp.</summary>
        public static string Iso(DateTime at) => at.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture);
    }

    /// <summary>Markers in Redis; the multiplexer is resolved on first use so a missing or unreachable host shows up as a retried error, not a crash at startup.</summary>
    public sealed class RedisScyllaArchiveMarkers : IScyllaArchiveMarkers
    {
        private readonly Lazy<IConnectionMultiplexer> redis;

        /// <summary>Create with a factory of the archive's own multiplexer (<see cref="ScyllaArchiveRedis.ServiceKey"/>).</summary>
        public RedisScyllaArchiveMarkers(Func<IConnectionMultiplexer> redisFactory)
        {
            redis = new Lazy<IConnectionMultiplexer>(redisFactory ?? throw new ArgumentNullException(nameof(redisFactory)));
        }

        /// <inheritdoc/>
        public async Task<string> GetAsync(string key)
        {
            var value = await redis.Value.GetDatabase().StringGetAsync(key).ConfigureAwait(false);
            return value.HasValue ? value.ToString() : null;
        }

        /// <inheritdoc/>
        public Task SetAsync(string key, string value) => redis.Value.GetDatabase().StringSetAsync(key, value);
    }

    /// <summary>The Redis connection of the archive markers (not the service's <c>REDIS_HOST</c>, sky-auctions-redis is retired).</summary>
    public static class ScyllaArchiveRedis
    {
        /// <summary>DI key of the archive's <see cref="IConnectionMultiplexer"/>.</summary>
        public const string ServiceKey = "scylla-archive";

        /// <summary><c>ARCHIVE:REDIS_HOST</c>, else <c>SETTINGS_REDIS_HOST</c> (sky-settings-redis, also holds the bazaar archive markers); null when neither is set.</summary>
        public static string ResolveHost(IConfiguration config)
        {
            var host = config?["ARCHIVE:REDIS_HOST"];
            if (string.IsNullOrWhiteSpace(host))
                host = config?["SETTINGS_REDIS_HOST"];
            return string.IsNullOrWhiteSpace(host) ? null : host;
        }

        /// <summary>Connection options for the resolved host; never aborts on a failed first connect (operations fail and are retried instead).</summary>
        public static ConfigurationOptions Options(IConfiguration config)
        {
            var host = ResolveHost(config) ?? throw new InvalidOperationException("Neither ARCHIVE:REDIS_HOST nor SETTINGS_REDIS_HOST is set; the Scylla archive sweep needs Redis for its markers");
            var options = ConfigurationOptions.Parse(host);
            options.AbortOnConnectFail = false;
            return options;
        }

        /// <summary>Connect (used by the keyed DI registration).</summary>
        public static IConnectionMultiplexer Connect(IConfiguration config) => ConnectionMultiplexer.Connect(Options(config));
    }
}
