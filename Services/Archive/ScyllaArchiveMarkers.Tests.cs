using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Moq;
using NUnit.Framework;
using StackExchange.Redis;

namespace Coflnet.Sky.Auctions.Archive;

public class ScyllaArchiveMarkersTests
{
    private static IConfiguration Config(params (string key, string value)[] values)
    {
        var dict = new Dictionary<string, string>();
        foreach (var (key, value) in values)
            dict[key] = value;
        return new ConfigurationBuilder().AddInMemoryCollection(dict).Build();
    }

    [Test]
    public void RedisHostFallsBackToSettingsRedisHost()
    {
        Assert.That(ScyllaArchiveRedis.ResolveHost(Config(("SETTINGS_REDIS_HOST", "sky-settings-redis"))), Is.EqualTo("sky-settings-redis"));
        Assert.That(ScyllaArchiveRedis.ResolveHost(Config(("SETTINGS_REDIS_HOST", "sky-settings-redis"), ("ARCHIVE:REDIS_HOST", "other:6380"))), Is.EqualTo("other:6380"));
        Assert.That(ScyllaArchiveRedis.ResolveHost(Config(("ARCHIVE:REDIS_HOST", ""), ("REDIS_HOST", "sky-auctions-redis"))), Is.Null, "never the retired service Redis");
    }

    [Test]
    public void ConnectionOptionsNeverAbortAndAMissingHostIsAClearError()
    {
        var options = ScyllaArchiveRedis.Options(Config(("SETTINGS_REDIS_HOST", "sky-settings-redis")));
        Assert.That(options.AbortOnConnectFail, Is.False);
        Assert.That(options.EndPoints, Has.Count.EqualTo(1));
        var error = Assert.Throws<InvalidOperationException>(() => ScyllaArchiveRedis.Options(Config()));
        Assert.That(error.Message, Does.Contain("ARCHIVE:REDIS_HOST").And.Contain("SETTINGS_REDIS_HOST"));
    }

    [Test]
    public async Task RedisMarkersReadAndWriteStrings_AndConnectLazily()
    {
        var db = new Mock<IDatabase>();
        db.Setup(d => d.StringGetAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()))
            .Returns((RedisKey k, CommandFlags _) => Task.FromResult(k == "auctions_archive:v2:month:2020-09" ? (RedisValue)"2026-10-23" : RedisValue.Null));
        var redis = new Mock<IConnectionMultiplexer>();
        redis.Setup(r => r.GetDatabase(It.IsAny<int>(), It.IsAny<object>())).Returns(db.Object);
        var connects = 0;
        var store = new RedisScyllaArchiveMarkers(() => { connects++; return redis.Object; });
        Assert.That(connects, Is.Zero, "no connection at construction");

        Assert.That(await store.GetAsync(ScyllaArchiveMarkerKeys.TagMonth("HYPERION", 202009)), Is.Null);
        Assert.That(await store.GetAsync(ScyllaArchiveMarkerKeys.Month(202009)), Is.EqualTo("2026-10-23"));
        try
        {
            await store.SetAsync(ScyllaArchiveMarkerKeys.TagMonth("HYPERION", 202009), "empty:x");
        }
        catch (NullReferenceException)
        {
            // the loose mock returns no Task for the set overload; only the recorded call matters here
        }
        var set = db.Invocations.Single(i => i.Method.Name == nameof(IDatabase.StringSetAsync));
        Assert.That((string)(RedisKey)set.Arguments[0], Is.EqualTo("auctions_archive:v2:HYPERION:2020-09"));
        Assert.That((string)(RedisValue)set.Arguments[1], Is.EqualTo("empty:x"));
        Assert.That(set.Arguments.OfType<TimeSpan?>().All(t => t == null), Is.True, "markers never expire");
        Assert.That(connects, Is.EqualTo(1));
    }

    [Test]
    public void MarkerKeysAndValues()
    {
        var at = new DateTime(2026, 10, 23, 12, 0, 0, DateTimeKind.Utc);
        Assert.That(ScyllaArchiveMarkerKeys.Month(202009), Is.EqualTo("auctions_archive:v2:month:2020-09"));
        Assert.That(ScyllaArchiveMarkerKeys.SweepComplete, Is.EqualTo("auctions_archive:v2:sweep_complete"));
        Assert.That(ScyllaArchiveMarkerKeys.Archived(12, 2, at), Is.EqualTo("archived:12:2:2026-10-23T12:00:00.0000000Z"));
        Assert.That(ScyllaArchiveMarkerKeys.Empty(at), Is.EqualTo("empty:2026-10-23T12:00:00.0000000Z"));
        Assert.That(ScyllaArchiveMarkerKeys.OverlapOnly(at), Is.EqualTo("overlap_only:2026-10-23T12:00:00.0000000Z"));
    }
}
