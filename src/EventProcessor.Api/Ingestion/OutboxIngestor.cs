using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using EventProcessor.Api.BackgroundServices;
using EventProcessor.Application.DTOs;
using EventProcessor.Domain;

namespace EventProcessor.Api.Ingestion
{
    // Outbox mode: buffer in memory, OutboxWriterService persists to PostgreSQL, OutboxProcessor publishes
    public class OutboxIngestor : IEventIngestor
    {
        private readonly OutboxQueue _queue;

        public OutboxIngestor(OutboxQueue queue)
        {
            _queue = queue;
        }

        public Task<bool> IngestAsync(IReadOnlyList<EventDto> events, CancellationToken ct)
        {
            foreach (var eventDto in events)
            {
                var message = new OutboxMessage
                {
                    EventId = eventDto.EventId,
                    Type = eventDto.Type,
                    Content = eventDto.Payload,
                    CreatedAt = eventDto.CreatedAt
                };

                if (!_queue.Writer.TryWrite(message))
                    return Task.FromResult(false);
            }

            return Task.FromResult(true);
        }
    }
}
