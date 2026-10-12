using System;
using System.Text.Json;
using System.Threading.Tasks;
using Confluent.Kafka;
using EventProcessor.Application.DTOs;
using EventProcessor.Application.Interfaces;
using EventProcessor.Infrastructure.Settings;
using Microsoft.Extensions.Options;

namespace EventProcessor.Infrastructure.Messaging
{
    public class KafkaPublisher : IEventPublisher, IDisposable
    {
        private readonly KafkaSettings _settings;
        private readonly IProducer<string, string> _producer;

        public KafkaPublisher(IOptions<KafkaSettings> settings)
        {
            _settings = settings.Value;

            var config = new ProducerConfig
            {
                BootstrapServers = _settings.BootstrapServers,
                // Idempotent producer: broker drops duplicates caused by producer retries,
                // and acks=all waits for every in-sync replica before confirming
                EnableIdempotence = true,
                Acks = Acks.All,
                // Small linger lets concurrent outbox publishes share one request
                LingerMs = _settings.LingerMs,
                CompressionType = CompressionType.Lz4
            };

            _producer = new ProducerBuilder<string, string>(config).Build();
        }

        public async Task PublishAsync(EventDto eventDto)
        {
            // Keyed by EventId so redeliveries of the same event land on the same partition
            await _producer.ProduceAsync(_settings.Topic, new Message<string, string>
            {
                Key = eventDto.EventId.ToString(),
                Value = JsonSerializer.Serialize(eventDto)
            });
        }

        public void Dispose()
        {
            _producer.Flush(TimeSpan.FromSeconds(10));
            _producer.Dispose();
        }
    }
}
