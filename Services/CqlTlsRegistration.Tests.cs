using System;
using System.Collections.Generic;
using Coflnet.Core.Security;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;

namespace Coflnet.Sky.Auctions.Services;

public class CqlTlsRegistrationTests
{
    private static ServiceProvider Services(Dictionary<string, string> tls)
    {
        tls["MARIADB_VERSION"] = "10.11.6";
        tls["DB_CONNECTION"] = "Server=synthetic-only";
        tls["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] = "http://127.0.0.1:4318";
        var config = new ConfigurationBuilder().AddInMemoryCollection(tls).Build();
        var services = new ServiceCollection().AddSingleton<IConfiguration>(config);
        new Startup(config).ConfigureServices(services);
        return services.BuildServiceProvider();
    }

    [Test]
    public void MissingPemKeyFailsClosedRatherThanSelectingLegacyTls()
    {
        using var services = Services(new() { ["CASSANDRA:TLS:CA_CERT_PATH"] = "/synthetic/ca" });
        Assert.Throws<InvalidOperationException>(() => services.GetRequiredService<PemTlsOptions>());
    }

    [Test]
    public void PemOwnerIsProcessOwnedAndReadsFilesOnlyWhenRequested()
    {
        var services = Services(new() {
            ["CASSANDRA:TLS:CA_CERT_PATH"] = "/synthetic-missing/ca",
            ["CASSANDRA:TLS:CERT_PATH"] = "/synthetic-missing/cert",
            ["CASSANDRA:TLS:KEY_PATH"] = "/synthetic-missing/key" });
        var owner = services.GetRequiredService<PemTlsOptions>();
        Assert.Throws<System.IO.DirectoryNotFoundException>(() => owner.GetCertificate());
        services.Dispose();
        Assert.Throws<ObjectDisposedException>(() => owner.GetCertificate());
    }

    [Test]
    public void LegacyConfigurationDoesNotActivatePem()
    {
        using var services = Services(new());
        Assert.That(services.GetService<PemTlsOptions>(), Is.Null);
    }
}
