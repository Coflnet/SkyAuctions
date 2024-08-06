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
        var request = new RestRequest($"api/profile/{playerId}/hypixel", Method.Get);
        var response = await profileClient.ExecuteAsync<HypixelProfile>(request);
        if(response.Content.Length < 100)
        {
            // failed
            logger.LogError($"Failed to get profile for {playerId} with response {response.Content}");
            return new();
        }
        logger.LogInformation($"Got response {JsonConvert.SerializeObject(response.Data)}");
        var lookup = new BuyerLookup()
        {
            Name = response.Data.Displayname,
            Uuid = playerId,
            DiscordId = response.Data.SocialMedia?.Links?.DISCORD,
            LastLogin = response.Data.LastLogin
        };
        var useLast = profile == "00000000000000000000000000000001";
        if (response.Data.Stats.Skyblock.Profiles.TryGetValue(profile, out var profileData) || useLast)
        {
            var toUseProfileId = useLast ? "latest" : profile;
            var skyBlockProfileRequest = new RestRequest($"api/profile/{playerId}/{toUseProfileId}", Method.Get);
            var skyBlockProfileResponse = await profileClient.ExecuteAsync(skyBlockProfileRequest);
            var items = await pricesApi.ApiProfileItemsPostAsync(JsonConvert.DeserializeObject<Api.Client.Model.Member>(skyBlockProfileResponse.Content));
            logger.LogInformation($"Got items {JsonConvert.SerializeObject(items)}");
            var uids = items.SelectMany(i => i.Value.Select(a => (a?.FlatNbt?.GetValueOrDefault("uid"),i.Key))).Where(i => i.Item1 != null);
            lookup.ItemsInInventory = uids.ToDictionary(i => i.Item1, i => i.Key);
            return lookup;
        } 
        logger.LogInformation($"Profile {profile} not found for {playerId} options {string.Join(", ", response.Data.Stats.Skyblock.Profiles.Keys)}");
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
        public Links Links { get; set; }
    }

    public class Links
    {
        public string DISCORD { get; set; }
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
