using System.Threading;
using System.Threading.Tasks;
using EventProcessor.Api.Ingestion;
using EventProcessor.Application.DTOs;
using EventProcessor.Infrastructure.Settings;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.RateLimiting;
using Microsoft.Extensions.Options;

namespace EventProcessor.Api.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    [EnableRateLimiting("ingestion")]
    public class EventsController : ControllerBase
    {
        private readonly IEventIngestor _ingestor;
        private readonly IngestSettings _settings;

        public EventsController(IEventIngestor ingestor, IOptions<IngestSettings> settings)
        {
            _ingestor = ingestor;
            _settings = settings.Value;
        }

        [HttpPost]
        public async Task<IActionResult> Post([FromBody] EventDto eventDto, CancellationToken ct)
        {
            return await _ingestor.IngestAsync(new[] { eventDto }, ct)
                ? Accepted()
                : StatusCode(503, "Server is overloaded");
        }

        // High-throughput path: one HTTP request carries up to Ingest:MaxBatchEvents events,
        // so per-request overhead is paid once per batch instead of once per event
        [HttpPost("batch")]
        public async Task<IActionResult> PostBatch([FromBody] EventDto[] events, CancellationToken ct)
        {
            if (events.Length == 0) return BadRequest("Empty batch");
            if (events.Length > _settings.MaxBatchEvents) return BadRequest($"Batch larger than {_settings.MaxBatchEvents} events");

            return await _ingestor.IngestAsync(events, ct)
                ? Accepted(new { accepted = events.Length })
                : StatusCode(503, "Server is overloaded");
        }
    }
}
