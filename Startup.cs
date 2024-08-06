using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using Cassandra;
using System.IO;
using System.Reflection;
using Coflnet.Sky.Auctions.Models;
using Coflnet.Sky.Auctions.Services;
using Coflnet.Sky.Core;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.OpenApi.Models;
using Prometheus;
using System.Security.Cryptography;
using StackExchange.Redis;
using Coflnet.Sky.Filter;
using Coflnet.Sky.PlayerName.Client.Api;
using Coflnet.Sky.Api.Client.Api;

namespace Coflnet.Sky.Auctions;
public class Startup
{
    public Startup(IConfiguration configuration)
    {
        Configuration = configuration;
    }

    public IConfiguration Configuration { get; }

    // This method gets called by the runtime. Use this method to add services to the container.
    public void ConfigureServices(IServiceCollection services)
    {
        services.AddControllers().AddNewtonsoftJson();
        services.AddSwaggerGen(c =>
        {
            c.SwaggerDoc("v1", new OpenApiInfo { Title = "SkyAuctions", Version = "v1" });
            // Set the comments path for the Swagger JSON and UI.
            var xmlFile = $"{Assembly.GetExecutingAssembly().GetName().Name}.xml";
            var xmlPath = Path.Combine(AppContext.BaseDirectory, xmlFile);
            c.IncludeXmlComments(xmlPath);
        });

        // Replace with your server version and type.
        // Use 'MariaDbServerVersion' for MariaDB.
        // Alternatively, use 'ServerVersion.AutoDetect(connectionString)'.
        // For common usages, see pull request #1233.
        var serverVersion = new MariaDbServerVersion(new Version(Configuration["MARIADB_VERSION"]));

        // Replace 'YourDbContext' with the name of your own DbContext derived class.
        services.AddDbContext<BaseDbContext>(
            dbContextOptions => dbContextOptions
                .UseMySql(Configuration["DB_CONNECTION"], serverVersion)
                .EnableSensitiveDataLogging() // <-- These two calls are optional but help
                .EnableDetailedErrors()       // <-- with debugging (remove for production).
        );
        services.AddHostedService<SellsCollector>();
        services.AddJaeger(Configuration);
        services.AddTransient<BaseService>();
        services.AddSingleton<IConnectionMultiplexer>(provider => ConnectionMultiplexer.Connect(Configuration["REDIS_HOST"]));
        services.AddSingleton<ISession>(p =>
        {
            Console.WriteLine("Connecting to Cassandra...");
            var builder = Cluster.Builder().AddContactPoints(Configuration["CASSANDRA:HOSTS"].Split(","))
                .WithLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
                .WithCompression(CompressionType.LZ4)
                .WithCredentials(Configuration["CASSANDRA:USER"], Configuration["CASSANDRA:PASSWORD"])
                .WithDefaultKeyspace(Configuration["CASSANDRA:KEYSPACE"]);

            Console.WriteLine("Connecting to servers " + Configuration["CASSANDRA:HOSTS"]);
            Console.WriteLine("Using keyspace " + Configuration["CASSANDRA:KEYSPACE"]);
            Console.WriteLine("Using replication class " + Configuration["CASSANDRA:REPLICATION_CLASS"]);
            Console.WriteLine("Using replication factor " + Configuration["CASSANDRA:REPLICATION_FACTOR"]);
            Console.WriteLine("Using user " + Configuration["CASSANDRA:USER"]);
            Console.WriteLine("Using password " + Configuration["CASSANDRA:PASSWORD"].Truncate(2) + "...");
            var certificatePaths = Configuration["CASSANDRA:X509Certificate_PATHS"];
            Console.WriteLine("Using certificate paths " + certificatePaths);
            if (!string.IsNullOrEmpty(certificatePaths))
                Console.WriteLine("Hash of certificate file " + SHA256CheckSum(certificatePaths));
            Console.WriteLine("Using certificate password " + Configuration["CASSANDRA:X509Certificate_PASSWORD"].Truncate(2) + "...");
            var validationCertificatePath = Configuration["CASSANDRA:X509Certificate_VALIDATION_PATH"];
            if (!string.IsNullOrEmpty(certificatePaths))
            {
                var password = Configuration["CASSANDRA:X509Certificate_PASSWORD"] ?? throw new InvalidOperationException("CASSANDRA:X509Certificate_PASSWORD must be set if CASSANDRA:X509Certificate_PATHS is set.");
                CustomRootCaCertificateValidator certificateValidator = null;
                if (!string.IsNullOrEmpty(validationCertificatePath))
                    certificateValidator = new CustomRootCaCertificateValidator(new X509Certificate2(validationCertificatePath, password));
                var sslOptions = new SSLOptions(
                    // TLSv1.2 is required as of October 9, 2019.
                    // See: https://www.instaclustr.com/removing-support-for-outdated-encryption-mechanisms/
                    SslProtocols.Tls12,
                    false,
                    // Custom validator avoids need to trust the CA system-wide.
                    (sender, certificate, chain, errors) => certificateValidator?.Validate(certificate, chain, errors) ?? true
                ).SetCertificateCollection(new(certificatePaths.Split(',').Select(p => new X509Certificate2(p, password)).ToArray()));
                builder.WithSSL(sslOptions);
            }
            var cluster = builder.Build();
            var session = cluster.ConnectAndCreateDefaultKeyspaceIfNotExists(new Dictionary<string, string>()
            {
                            {"class", Configuration["CASSANDRA:REPLICATION_CLASS"]},
                            {"replication_factor", Configuration["CASSANDRA:REPLICATION_FACTOR"]}
            });
            Console.WriteLine("Connected to Cassandra");
            return session;
        });
        services.AddHostedService<DeletingBackGroundService>();
        services.AddSingleton<ScyllaService>();
        services.AddSingleton<QueryService>();
        services.AddSingleton<FilterEngine>();
        services.AddTransient<RestoreService>();
        services.AddSingleton<ExportService>();
        services.AddSingleton<ProfileClient>();
        services.AddHostedService(s=>s.GetRequiredService<ExportService>());
        services.AddSingleton<IPricesApi>(s=> new PricesApi(Configuration["Api_BASE_URL"]));
        services.AddTransient<HypixelContext>(options =>
        {
            return new HypixelContext();
        });
        services.AddSingleton<IPlayerNameApi>(di => new PlayerNameApi(Configuration["PLAYERNAME_BASE_URL"]));
        services.AddResponseCaching();
        services.AddResponseCompression();
    }

    /// <summary>
    /// https://stackoverflow.com/a/51966515
    /// </summary>
    /// <param name="filePath"></param>
    /// <returns></returns>
    public string SHA256CheckSum(string filePath)
    {
        using (SHA256 SHA256 = SHA256Managed.Create())
        {
            using (FileStream fileStream = File.OpenRead(filePath))
                return Convert.ToBase64String(SHA256.ComputeHash(fileStream));
        }
    }

    // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
    public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
    {
        if (env.IsDevelopment())
        {
            app.UseDeveloperExceptionPage();
        }
        app.UseSwagger();
        app.UseSwaggerUI(c =>
        {
            c.SwaggerEndpoint("/swagger/v1/swagger.json", "SkyAuctions v1");
            c.RoutePrefix = "api";
        });

        app.UseResponseCaching();
        app.UseResponseCompression();

        app.UseRouting();

        app.UseAuthorization();

        app.UseEndpoints(endpoints =>
        {
            endpoints.MapMetrics();
            endpoints.MapControllers();
        });
    }
}
