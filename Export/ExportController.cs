using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;

namespace Coflnet.Sky.Auctions.Services;

[ApiController]
[Route("export")]
public class ExportController : ControllerBase
{
    private readonly ExportService exportService;

    public ExportController(ExportService exportService)
    {
        this.exportService = exportService;
    }

    [HttpPost("jobs")]
    public async Task<ExportService.ExportRequest> RequestExport([FromBody] ExportService.ExportRequest request)
    {
        return await exportService.RequestExport(request);
    }

    [HttpGet("jobs/{email}")]
    public async Task<IEnumerable<ExportService.ExportRequest>> GetJobs(string email)
    {
        return await exportService.GetJobs(email);
    }

    [HttpGet("jobs/{email}/scheduled")]
    public async Task<IEnumerable<ExportService.ExportRequest>> GetScheduled(string email)
    {
        return await exportService.GetScheduled(email);
    }

    [HttpGet("jobs/{email}/exports")]
    public async Task<IEnumerable<ExportService.ExportRequest>> GetExports(string email)
    {
        return await exportService.GetExports(email);
    }

    [HttpPost("jobs/{email}/{jobId:guid}/abort")]
    public async Task AbortScheduled(string email, Guid jobId)
    {
        await exportService.AbortScheduled(email, jobId);
    }

    [HttpDelete("jobs/{email}/{jobId:guid}")]
    public async Task DeleteExport(string email, Guid jobId)
    {
        await exportService.DeleteExport(email, jobId);
    }

    // Legacy routes kept for compatibility.
    [HttpPost]
    public async Task<ExportService.ExportRequest> LegacyRequestExport([FromBody] ExportService.ExportRequest request)
    {
        return await exportService.RequestExport(request);
    }

    [HttpDelete("{email}")]
    public async Task LegacyCancelExport(string email)
    {
        var scheduled = await exportService.GetScheduled(email);
        foreach (var job in scheduled)
        {
            await exportService.AbortScheduled(email, job.JobId);
        }
    }

    [HttpGet("{email}")]
    public async Task<IEnumerable<ExportService.ExportRequest>> LegacyGetExport(string email)
    {
        return await exportService.GetJobs(email);
    }
}
