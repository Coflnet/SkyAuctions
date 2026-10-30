using System;
using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using NUnit.Framework;

namespace Coflnet.Sky.Auctions;

public class CustomRootCaCertificateValidatorTests
{
    [Test]
    public void ConfiguredRootIsRequiredEvenWhenPlatformReportsNoErrors()
    {
        using var rootKey = RSA.Create(2048);
        var rootRequest = new CertificateRequest("CN=synthetic-root", rootKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        rootRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        using var root = rootRequest.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
        using var leafKey = RSA.Create(2048);
        var leafRequest = new CertificateRequest("CN=localhost", leafKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        leafRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, true));
        leafRequest.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(new OidCollection { new("1.3.6.1.5.5.7.3.1") }, true));
        using var leaf = leafRequest.Create(root, DateTimeOffset.UtcNow.AddHours(-1), DateTimeOffset.UtcNow.AddHours(1), RandomNumberGenerator.GetBytes(16));
        using var chain = new X509Chain();
        chain.ChainPolicy.ExtraStore.Add(root);
        var validator = new CustomRootCaCertificateValidator(root);
        Assert.That(validator.Validate(leaf, chain, SslPolicyErrors.RemoteCertificateChainErrors), Is.True);
        Assert.That(validator.Validate(leaf, chain, SslPolicyErrors.RemoteCertificateNameMismatch), Is.False);
        using var otherKey = RSA.Create(2048);
        using var other = new CertificateRequest("CN=other-root", otherKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1)
            .CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
        Assert.That(new CustomRootCaCertificateValidator(other).Validate(leaf, chain, SslPolicyErrors.None), Is.False);
    }
}
