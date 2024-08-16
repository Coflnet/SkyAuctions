using System.Threading.Tasks;
using Cassandra.Data.Linq;
using System.Collections.Generic;
using System.Linq;
using RestSharp;
using Microsoft.Extensions.Configuration;
using Coflnet.Sky.Api.Client.Api;
using Newtonsoft.Json;
using Cassandra;
using Microsoft.Extensions.Logging;
using Coflnet.Sky.Core;

namespace Coflnet.Sky.Auctions.Services;

public class ProfileClient
{
    private RestClient profileClient = null;
    private IPricesApi pricesApi;
    private ILogger<ProfileClient> logger;
    public ProfileClient(IConfiguration config, IPricesApi pricesApi, ILogger<ProfileClient> logger)
    {
        profileClient = new RestClient(config["PROFILE_BASE_URL"]);
        this.pricesApi = pricesApi;
        this.logger = logger;
    }

    public async Task<BuyerLookup> GetLookup(string playerId, string profile)
    {
        try
        {
            return await TryGetLookup(playerId, profile);
        }
        catch (System.Exception e)
        {
            logger.LogError(e, $"Failed to get profile for {playerId}");
            await Task.Delay(1000);
            return await TryGetLookup(playerId, profile);
        }
    }

    private async Task<BuyerLookup> TryGetLookup(string playerId, string profile)
    {
        var request = new RestRequest($"api/profile/{playerId}/hypixel", Method.Get);
        var response = await profileClient.ExecuteAsync<HypixelProfile>(request);
        if (response.Content.Length < 100)
        {
            // failed
            logger.LogError($"Failed to get profile for {playerId} with response {response.Content}");
            return new();
        }
        logger.LogInformation($"Got response {JsonConvert.SerializeObject(response.Data)} for {playerId}");
        var social = response.Data.SocialMedia?.Links ?? new();
        social["TWITTER"] = response.Data.SocialMedia?.Twitter;
        var lookup = new BuyerLookup()
        {
            Name = response.Data.Displayname,
            Uuid = playerId,
            SocialLinks = social,
            LastLogin = response.Data.LastLogin
        };
        var useLast = profile == "00000000000000000000000000000001";
        if (response.Data.Stats != null && response.Data.Stats.Skyblock.Profiles.TryGetValue(profile, out var profileData) || useLast)
        {
            var toUseProfileId = useLast ? "latest" : profile;
            var skyBlockProfileRequest = new RestRequest($"api/profile/{playerId}/{toUseProfileId}", Method.Get);
            var skyBlockProfileResponse = await profileClient.ExecuteAsync(skyBlockProfileRequest);
            var items = await pricesApi.ApiProfileItemsPostAsync(JsonConvert.DeserializeObject<Api.Client.Model.Member>(skyBlockProfileResponse.Content));
            logger.LogInformation($"Got items {JsonConvert.SerializeObject(items).Truncate(100)} for {playerId} profile {profile}");
            var uids = items.SelectMany(i => i.Value.Select(a => (a?.FlatNbt?.GetValueOrDefault("uid", a.Tag), i.Key))).Where(i => i.Item1 != null);
            lookup.ItemsInInventory = uids.GroupBy(i=>i.Item1).Select(i=>i.First()).ToDictionary(i => i.Item1, i => i.Key);
            return lookup;
        }
        logger.LogInformation($"Profile {profile} not found for {playerId} options {string.Join(", ", (response.Data?.Stats?.Skyblock?.Profiles ?? new()).Keys)}");
        lookup.ProfileNotFound = true;
        return lookup;
    }

    public class HypixelProfile
    {
        public string Displayname { get; set; }
        public long LastLogin { get; set; }
        public SocialMedia SocialMedia { get; set; }
        public HypixelStats Stats { get; set; }
    }

    public class SocialMedia
    {
        public string Twitter { get; set; }
        public Dictionary<string,string> Links { get; set; }
    }

    public class HypixelStats
    {
        public Skyblock Skyblock { get; set; }

    }
    public class Skyblock
    {
        public Dictionary<string, object> Profiles { get; set; }
    }

}
