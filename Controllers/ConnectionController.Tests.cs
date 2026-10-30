using System;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using Microsoft.AspNetCore.Mvc;
using Moq;
using NUnit.Framework;

namespace Coflnet.Sky.Auctions.Controllers;

public class ConnectionControllerTests
{
    [Test]
    public async Task ReadsOnlySystemLocalWithBoundedDriverTimeout()
    {
        var session = new Mock<ISession>();
        session.Setup(s => s.ExecuteAsync(It.IsAny<IStatement>())).ReturnsAsync(new RowSet());
        Assert.That(await new ConnectionController(session.Object).Ready(CancellationToken.None), Is.TypeOf<OkObjectResult>());
        session.Verify(s => s.ExecuteAsync(It.Is<SimpleStatement>(q =>
            q.QueryString == "SELECT key FROM system.local" && q.ReadTimeoutMillis == 5000)), Times.Once);
    }

    [Test]
    public async Task RejectsBrokenConnectionWithoutExposingDriverDetails()
    {
        var session = new Mock<ISession>();
        session.Setup(s => s.ExecuteAsync(It.IsAny<IStatement>())).ThrowsAsync(new Exception("private synthetic detail"));
        var response = (ObjectResult)await new ConnectionController(session.Object).Ready(CancellationToken.None);
        Assert.That(response.StatusCode, Is.EqualTo(503));
        Assert.That(response.Value, Is.EqualTo("not ready"));
    }

    [Test]
    public async Task CancellationAlsoFailsClosed()
    {
        var session = new Mock<ISession>();
        session.Setup(s => s.ExecuteAsync(It.IsAny<IStatement>())).Returns(new TaskCompletionSource<RowSet>().Task);
        var response = (ObjectResult)await new ConnectionController(session.Object).Ready(new CancellationToken(true));
        Assert.That(response.StatusCode, Is.EqualTo(503));
    }
}
