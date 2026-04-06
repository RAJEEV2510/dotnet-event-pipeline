using System.Threading;
using System.Threading.Tasks;
using EventProcessor.Api.BackgroundServices;
using EventProcessor.Application.DTOs;
using EventProcessor.Application.Interfaces;
using EventProcessor.Domain;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.RateLimiting;
using Microsoft.Extensions.Logging;

namespace EventProcessor.Api.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    [EnableRateLimiting("ingestion")]
    public class EventsController : ControllerBase
    {
        private readonly OutboxQueue _queue;
        private readonly ILogger<EventsController> _logger;

        public EventsController(OutboxQueue queue, ILogger<EventsController> logger)
        {
            _queue = queue;
            _logger = logger;
        }

        [HttpPost]
        public Task<IActionResult> Post([FromBody] EventDto eventDto, CancellationToken ct)
        {
            // Push to memory queue for extreme performance
            var outboxMessage = new OutboxMessage
            {
                EventId = eventDto.EventId,
                Type = eventDto.Type,
                Content = eventDto.Payload,
                CreatedAt = eventDto.CreatedAt
            };

            if (_queue.Writer.TryWrite(outboxMessage))
            {
                return Task.FromResult<IActionResult>(Accepted());
            }

            // If queue is full, return 503 Service Unavailable or 429
            return Task.FromResult<IActionResult>(StatusCode(503, "Server is overloaded"));
        }
    }
}
