using Coflnet.Core;
using Coflnet.Security.OpenBao;
using Coflnet.Sky.Core;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Coflnet.Sky.Auctions;
public class Program
{
    public static void Main(string[] args)
    {
        var host = CreateHostBuilder(args).Build();
        HypixelContext.SetConfiguration(host.Services.GetRequiredService<IConfiguration>());
        host.Run();
    }

    public static IHostBuilder CreateHostBuilder(string[] args) =>
        Host.CreateDefaultBuilder(args)
            .ConfigureAppConfiguration((_, config) => config.AddOpenBaoFromEnvironment())
            .ConfigureLogging((context, logging) =>
            {
                logging.AddOpenTelemetryLogging(
                    context.Configuration,
                    context.Configuration["JAEGER_SERVICE_NAME"] ?? "sky-auctions");
            })
            .ConfigureWebHostDefaults(webBuilder =>
            {
                webBuilder.UseStartup<Startup>();
            });
}
