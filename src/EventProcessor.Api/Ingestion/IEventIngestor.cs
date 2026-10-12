using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using EventProcessor.Application.DTOs;

namespace EventProcessor.Api.Ingestion
{
    public interface IEventIngestor
    {
        // false = system is saturated; the caller should back off (HTTP 503)
        Task<bool> IngestAsync(IReadOnlyList<EventDto> events, CancellationToken ct);
    }
}
