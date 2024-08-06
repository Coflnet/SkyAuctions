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
