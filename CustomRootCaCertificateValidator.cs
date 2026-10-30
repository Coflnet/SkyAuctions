using System.Security.Cryptography.X509Certificates;

namespace Coflnet.Sky.Auctions;

/// <summary>Validates the server name and chain against the configured root.</summary>
public class CustomRootCaCertificateValidator : Coflnet.Cassandra.CustomRootCaCertificateValidator
{
    public CustomRootCaCertificateValidator(X509Certificate2 trustedRootCertificateAuthority)
        : base(trustedRootCertificateAuthority) { }
}
