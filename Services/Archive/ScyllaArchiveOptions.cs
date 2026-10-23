using System;
using Coflnet.Sky.Core.Archive;
using Microsoft.Extensions.Configuration;

namespace Coflnet.Sky.Auctions.Archive
{
    /// <summary>
    /// <c>ARCHIVE:*</c> settings of the Scylla archive sweep (phase 2), defaults as in appsettings.json.
    /// Months are <c>yyyyMM</c>, configured as <c>yyyy-MM</c>.
    /// </summary>
    public record ScyllaArchiveOptions
    {
        /// <summary>Hard ceiling of the part number: <c>archived_months.parts</c> is a tinyint unsigned part count (so at most 255 parts, 0-254).</summary>
        public const int HighestStorablePart = byte.MaxValue - 1;

        public bool Enabled { get; init; }
        public int StartDelaySeconds { get; init; } = 600;
        /// <summary>Oldest month swept (SkyBlock auctions start in 2019-06; Scylla holds rows from 2019-07).</summary>
        public int OldestMonth { get; init; } = 201907;
        /// <summary>Newest month swept; the sweep never goes past it.</summary>
        public int NewestMonth { get; init; } = 202103;
        public int PageSize { get; init; } = 500;
        public int PagePauseMs { get; init; } = 100;
        public int PartitionPauseMs { get; init; } = 500;
        public int TagPauseMs { get; init; } = 1000;
        public int MonthPauseMs { get; init; } = 60000;
        public int IdlePauseMinutes { get; init; } = 60;
        /// <summary>Pause after a pass that ended waiting for SkyIndexer to import the previous month (pipeline gate).</summary>
        public int ImportWaitMinutes { get; init; } = 5;
        public int MaxRowsPerPart { get; init; } = 50000;
        /// <summary>Highest part number a tag-month may use (clamped to <see cref="HighestStorablePart"/>).</summary>
        public int MaxPart { get; init; } = 255;
        /// <summary>Retries of one Scylla page after its first failure (backoff 2 s, 4 s, 8 s, ...), resuming from the same paging state.</summary>
        public int PageRetries { get; init; } = 3;
        public int CircuitBreakerFailures { get; init; } = 5;
        public int CircuitBreakerPauseMinutes { get; init; } = 10;
        public int PartitionListMaxAgeDays { get; init; } = 30;
        /// <summary>Cross-check mismatches logged per tag-month (all are counted).</summary>
        public int CrosscheckLogLimit { get; init; } = 5;
        /// <summary>Redis of the progress markers; empty = <c>SETTINGS_REDIS_HOST</c> (see <see cref="ScyllaArchiveRedis.ResolveHost"/>).</summary>
        public string RedisHost { get; init; } = "";

        /// <summary>The part limit that is actually enforced.</summary>
        public int EffectiveMaxPart => Math.Clamp(MaxPart, 0, HighestStorablePart);

        /// <summary>Read <c>ARCHIVE:*</c>; missing or invalid values fall back to the defaults.</summary>
        public static ScyllaArchiveOptions From(IConfiguration config)
        {
            var d = new ScyllaArchiveOptions();
            var s = config.GetSection("ARCHIVE");
            return new ScyllaArchiveOptions
            {
                Enabled = Bool(s["ENABLED"], false),
                StartDelaySeconds = Int(s["START_DELAY_SECONDS"], d.StartDelaySeconds),
                OldestMonth = Month(s["OLDEST_MONTH"], d.OldestMonth),
                NewestMonth = Month(s["NEWEST_MONTH"], d.NewestMonth),
                PageSize = Math.Max(1, Int(s["PAGE_SIZE"], d.PageSize)),
                PagePauseMs = Int(s["PAGE_PAUSE_MS"], d.PagePauseMs),
                PartitionPauseMs = Int(s["PARTITION_PAUSE_MS"], d.PartitionPauseMs),
                TagPauseMs = Int(s["TAG_PAUSE_MS"], d.TagPauseMs),
                MonthPauseMs = Int(s["MONTH_PAUSE_MS"], d.MonthPauseMs),
                IdlePauseMinutes = Math.Max(1, Int(s["IDLE_PAUSE_MINUTES"], d.IdlePauseMinutes)),
                ImportWaitMinutes = Math.Max(1, Int(s["IMPORT_WAIT_MINUTES"], d.ImportWaitMinutes)),
                MaxRowsPerPart = Math.Max(1, Int(s["MAX_ROWS_PER_PART"], d.MaxRowsPerPart)),
                MaxPart = Int(s["MAX_PART"], d.MaxPart),
                PageRetries = Math.Max(0, Int(s["PAGE_RETRIES"], d.PageRetries)),
                CircuitBreakerFailures = Math.Max(1, Int(s["CIRCUIT_BREAKER_FAILURES"], d.CircuitBreakerFailures)),
                CircuitBreakerPauseMinutes = Int(s["CIRCUIT_BREAKER_PAUSE_MINUTES"], d.CircuitBreakerPauseMinutes),
                PartitionListMaxAgeDays = Int(s["PARTITION_LIST_MAX_AGE_DAYS"], d.PartitionListMaxAgeDays),
                CrosscheckLogLimit = Math.Max(0, Int(s["CROSSCHECK_LOG_LIMIT"], d.CrosscheckLogLimit)),
                RedisHost = s["REDIS_HOST"] ?? d.RedisHost,
            };
        }

        private static int Month(string value, int fallback) => string.IsNullOrWhiteSpace(value) ? fallback : ArchiveMonths.Parse(value);
        private static int Int(string value, int fallback) => int.TryParse(value, out var v) ? v : fallback;
        private static bool Bool(string value, bool fallback) => bool.TryParse(value, out var v) ? v : fallback;
    }
}
