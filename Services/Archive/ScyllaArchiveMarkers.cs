using System;
using System.Collections.Generic;
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

        /// <summary>Add members to a set (idempotent).</summary>
        Task AddToSetAsync(string key, IReadOnlyCollection<string> members);

        /// <summary>Every member of a set (empty when it does not exist).</summary>
        Task<IReadOnlyList<string>> GetSetAsync(string key);

        /// <summary>Delete keys.</summary>
        Task DeleteAsync(params string[] keys);
    }

    /// <summary>Marker keys and values (<c>auctions_archive:v2:*</c>, next to the bazaar archive markers in sky-settings-redis).</summary>
    public static class ScyllaArchiveMarkerKeys
    {
        /// <summary>Common prefix.</summary>
        public const string Prefix = "auctions_archive:v2:";
        /// <summary>Set when every month in scope is complete.</summary>
        public const string SweepComplete = Prefix + "sweep_complete";
        /// <summary>Prefix of the partition scan progress (deleted when the scan completed and its result is cached in R2).</summary>
        public const string PartitionScanPrefix = Prefix + "partitions:";
        /// <summary>Set of the in-scope partitions found so far, <c>{tag}:{timekey}</c>.</summary>
        public const string PartitionScanFound = PartitionScanPrefix + "found";
        /// <summary><c>{nextRange}/{rangeCount}/{oldestMonth}/{newestMonth}</c>; another range count or scope restarts the scan.</summary>
        public const string PartitionScanNext = PartitionScanPrefix + "next_range";

        /// <summary>Set member of a partition.</summary>
        public static string Partition(string tag, short timeKey) => tag + ":" + timeKey.ToString(CultureInfo.InvariantCulture);

        /// <summary>Inverse of <see cref="Partition"/> (the timekey is after the last ':'); false for anything else.</summary>
        public static bool TryParsePartition(string member, out (string Tag, short TimeKey) partition)
        {
            partition = default;
            var colon = member?.LastIndexOf(':') ?? -1;
            if (colon <= 0 || !short.TryParse(member.AsSpan(colon + 1), NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var key))
                return false;
            partition = (member[..colon], key);
            return true;
        }

        /// <summary>Value of <see cref="PartitionScanNext"/>.</summary>
        public static string ScanProgress(int nextRange, int rangeCount, int oldestMonth, int newestMonth)
            => FormattableString.Invariant($"{nextRange}/{rangeCount}/{oldestMonth}/{newestMonth}");

        /// <summary>The next range of a stored progress value if it belongs to the same range count and months, else 0 (restart).</summary>
        public static int ResumeRange(string progress, int rangeCount, int oldestMonth, int newestMonth)
        {
            var parts = progress?.Split('/');
            if (parts == null || parts.Length != 4
                || !int.TryParse(parts[0], NumberStyles.None, CultureInfo.InvariantCulture, out var next)
                || parts[1] != rangeCount.ToString(CultureInfo.InvariantCulture)
                || parts[2] != oldestMonth.ToString(CultureInfo.InvariantCulture)
                || parts[3] != newestMonth.ToString(CultureInfo.InvariantCulture)
                || next > rangeCount)
                return 0;
            return next;
        }

        /// <summary><c>auctions_archive:v2:{tag}:{yyyy-MM}</c> = <see cref="Archived"/> | <see cref="Empty"/> | <see cref="OverlapOnly"/>.</summary>
        public static string TagMonth(string tag, int month) => Prefix + tag + ":" + ArchiveMonths.Format(month);

        /// <summary><c>auctions_archive:v2:month:{yyyy-MM}</c> = iso time every tag of the month was done.</summary>
        public static string Month(int month) => Prefix + "month:" + ArchiveMonths.Format(month);

        /// <summary><c>auctions_archive:v2:redo:{yyyy-MM}</c>: set by the operator (any value) to have the sweep archive the month again.</summary>
        public static string Redo(int month) => Prefix + "redo:" + ArchiveMonths.Format(month);

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

        /// <inheritdoc/>
        public Task AddToSetAsync(string key, IReadOnlyCollection<string> members)
        {
            if (members == null || members.Count == 0)
                return Task.CompletedTask;
            var values = new RedisValue[members.Count];
            var i = 0;
            foreach (var m in members)
                values[i++] = m;
            return redis.Value.GetDatabase().SetAddAsync(key, values);
        }

        /// <inheritdoc/>
        public async Task<IReadOnlyList<string>> GetSetAsync(string key)
        {
            var values = await redis.Value.GetDatabase().SetMembersAsync(key).ConfigureAwait(false);
            var result = new List<string>(values.Length);
            foreach (var v in values)
                result.Add(v.ToString());
            return result;
        }

        /// <inheritdoc/>
        public Task DeleteAsync(params string[] keys)
        {
            var redisKeys = new RedisKey[keys.Length];
            for (var i = 0; i < keys.Length; i++)
                redisKeys[i] = keys[i];
            return redis.Value.GetDatabase().KeyDeleteAsync(redisKeys);
        }
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
