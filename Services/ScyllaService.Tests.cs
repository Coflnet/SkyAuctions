using Cassandra;
using NUnit.Framework;
using Moq;
using Microsoft.Extensions.Logging.Abstractions;
using Coflnet.Sky.Core;
using System.Threading.Tasks;
using System;

namespace Coflnet.Sky.Auctions;

public class ScyllaServiceTest
{
    //[Test]
    public async Task InsertAuctionTag()
    {
        var mockSession = new Mock<ISession>();
        var service = new ScyllaService(mockSession.Object, NullLogger<ScyllaService>.Instance);
        await service.Create();
        await service.InsertAuctionsOfTag(new SaveAuction[]{
            new(){
                Uuid = Guid.NewGuid().ToString(),
                Tag = "test"
            }
        });
    }
}
