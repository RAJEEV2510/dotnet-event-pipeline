using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using EventProcessor.Application.DTOs;
using EventProcessor.Application.Interfaces;
using Microsoft.Extensions.Logging;

namespace EventProcessor.Api.Ingestion
{
    // Direct mode: the request returns 202 only after Kafka has acknowledged every event (acks=all),
    // so an accepted event is durable without a PostgreSQL write on the ingestion path
    public class DirectKafkaIngestor : IEventIngestor
    {
        private readonly IEventPublisher _publisher;
        private readonly ILogger<DirectKafkaIngestor> _logger;

        public DirectKafkaIngestor(IEventPublisher publisher, ILogger<DirectKafkaIngestor> logger)
        {
            _publisher = publisher;
            _logger = logger;
        }

        public async Task<bool> IngestAsync(IReadOnlyList<EventDto> events, CancellationToken ct)
        {
            try
            {
                await _publisher.PublishBatchAsync(events);
                return true;
            }
            catch (KafkaException ex)
            {
                _logger.LogError(ex, "Kafka rejected a batch of {Count} events", events.Count);
                return false;
            }
        }
    }
}
