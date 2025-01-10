using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;

namespace Coflnet.Sky.Auctions.Services;

[ApiController]
[Route("export")]
public class ExportController : ControllerBase
{
    ExportService exportService;
    public ExportController(ExportService exportService)
    {
        this.exportService = exportService;
    }

    [HttpPost]
    public async Task<ExportService.ExportRequest> RequestExport([FromBody] ExportService.ExportRequest request)
    {
        return await exportService.RequestExport(request);
    }

    [HttpPost("thomas")]
    public async Task RequestAll()
    {
        var toExport = new List<string>(){
"GOLDEN_APPLE_FLUX",
"FROG_BARN_SKIN",
"PET_SKIN_CHICKEN_TURKEY",
"PET_SKIN_JERRY_HANDSOME",
"PET_SKIN_RABBIT_AQUAMARINE",
"PET_SKIN_RABBIT_ROSE",
"PURPLE_EGG",
"PET_SKIN_CHICKEN_BABY_CHICK",
"GREEN_EGG",
"BLUE_EGG",
"SHADOW_ASSASSIN_MAUVE",
"SHADOW_ASSASSIN_CRIMSON",
"SHADOW_ASSASSIN_ADMIRAL",
"PET_SKIN_ELEPHANT_MONOCHROME",
"PET_SKIN_WHALE_ORCA",
"ENDERPACK",
"PET_SKIN_ELEPHANT_RED",
"PET_SKIN_ELEPHANT_GREEN",
"PET_SKIN_ELEPHANT_PURPLE",
"PET_SKIN_DRAGON_NEON_PURPLE",
"PET_SKIN_DRAGON_NEON_RED",
"PET_SKIN_DRAGON_NEON_BLUE",
"PET_SKIN_MONKEY_GOLDEN",
"PET_SKIN_SHEEP_NEON_GREEN",
"PET_SKIN_SHEEP_NEON_YELLOW",
"PET_SKIN_SHEEP_NEON_RED",
"PET_SKIN_SHEEP_NEON_BLUE",
"SNOW_SNOWGLOBE",
"PET_SKIN_JERRY_GREEN_ELF",
"PET_SKIN_JERRY_RED_ELF",
"PET_SKIN_YETI_GROWN_UP",
"PET_SKIN_ELEPHANT_ORANGE",
"PET_SKIN_ELEPHANT_BLUE",
"PET_SKIN_ELEPHANT_PINK",
"SUPERIOR_SHIMMER",
"PET_SKIN_WITHER",
"PET_SKIN_SHEEP_WHITE",
"PET_SKIN_SHEEP_LIGHT_GREEN",
"PET_SKIN_SHEEP_PURPLE",
"PET_SKIN_SHEEP_LIGHT_BLUE",
"PET_SKIN_SHEEP_PINK",
"UNSTABLE_BABY",
"YOUNG_BABY",
"WISE_BABY",
"PROTECTOR_BABY",
"HOLY_BABY",
"STRONG_BABY",
"TARANTULA_BLACK_WIDOW",
"PET_SKIN_TIGER_TWILIGHT",
"FROZEN_BLAZE_ICICLE",
"PET_SKIN_GUARDIAN",
"REAPER_SPIRIT",
"PET_SKIN_RABBIT",
"SUPERIOR_BABY",
"PET_SKIN_ENDERMAN",
"PET_SKIN_ENDER_DRAGON_BABY",
"PET_SKIN_ENDER_DRAGON_BABY_BLUE",
"PIRATE_BOMB_FLUX",
"PET_SKIN_WHALE_COSMIC",
"PET_SKIN_GRIFFIN_REINDRAKE",
"PET_SKIN_SHEEP_WHITE_WOOLY",
"PET_SKIN_SHEEP_BLACK_WOOLY",
"PET_SKIN_GOLDEN_DRAGON_ANUBIS",
"PET_SKIN_BEE_RGBEE",
"PET_SKIN_ENDER_DRAGON_UNDEAD",
"PET_SKIN_MEGALODON_BABY",
"PET_SKIN_ENDERMAN_XENON",
"PET_SKIN_KUUDRA_LOYALTY",
"PET_SKIN_DOLPHIN_SNUBNOSE_PURPLE",
"PET_SKIN_DOLPHIN_SNUBNOSE_GREEN",
"PET_SKIN_DOLPHIN_SNUBNOSE_RED",
"PET_SKIN_BLAZE_FROZEN",
"PET_SKIN_OCELOT_SNOW_TIGER",
"PET_SKIN_PHOENIX_ICE",
"PET_SKIN_BAT_VAMPIRE",
"PET_SKIN_TIGER_SABER_TOOTH",
"PET_SKIN_SQUID_GLOW",
"PET_SKIN_WOLF"
        };
        var i = 0;
        foreach (var item in toExport)
        {
            await exportService.RequestExport(new ExportService.ExportRequest()
            {
                ByEmail = "thomaswilcox" + (i++),
                ItemTag = item,
                Filters = new Dictionary<string, string>(){
                    {"EndBefore", DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString()},
                    {"EndAfter", new DateTimeOffset(new DateTime(2019,7,20)).ToUnixTimeSeconds().ToString() } },
                Columns = ["uuid", "endedAt", "itemUid", "highestBidAmount"],
                DiscordWebhookUrl = "https://discord.com/api/webhooks/1327326866713083986/eAKW-c-aDGt2IDqpjI--snFSzxaYgEa6_va5D1YG0oWQ11r04FYyBghtMftFzn1PFhnv"
            });
        }
    }

    [HttpDelete("{email}")]
    public async Task CancelExport(string email)
    {
        await exportService.CancelExport(email);
    }

    [HttpGet("{email}")]
    public async Task<IEnumerable<ExportService.ExportRequest>> GetExport(string email)
    {
        return await exportService.GetExports(email);
    }
}
