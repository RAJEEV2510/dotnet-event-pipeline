using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
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
        private readonly IProducer<string, byte[]> _producer;

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
                // Linger + large batches let thousands of concurrent produces share one request
                LingerMs = _settings.LingerMs,
                BatchSize = _settings.ProducerBatchBytes,
                QueueBufferingMaxMessages = _settings.ProducerQueueMaxMessages,
                CompressionType = CompressionType.Lz4
            };

            _producer = new ProducerBuilder<string, byte[]>(config).Build();
        }

        public async Task PublishAsync(EventDto eventDto)
        {
            await _producer.ProduceAsync(_settings.Topic, ToMessage(eventDto));
        }

        public async Task PublishBatchAsync(IReadOnlyList<EventDto> events)
        {
            if (events.Count == 0) return;

            // One completion for the whole batch instead of one Task per event:
            // Produce() is fire-and-forget with a delivery callback, so a 5,000-event batch
            // is handed to librdkafka in one pass and acknowledged as a unit.
            var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var pending = events.Count;
            Exception? firstError = null;

            void OnDelivery(DeliveryReport<string, byte[]> report)
            {
                if (report.Error.IsError)
                    Interlocked.CompareExchange(ref firstError, new ProduceException<string, byte[]>(report.Error, report), null);

                if (Interlocked.Decrement(ref pending) == 0)
                {
                    if (firstError != null) done.TrySetException(firstError);
                    else done.TrySetResult();
                }
            }

            foreach (var eventDto in events)
            {
                var message = ToMessage(eventDto);
                while (true)
                {
                    try
                    {
                        _producer.Produce(_settings.Topic, message, OnDelivery);
                        break;
                    }
                    catch (ProduceException<string, byte[]> ex) when (ex.Error.Code == ErrorCode.Local_QueueFull)
                    {
                        // Local buffer is full (backpressure): wait for librdkafka to drain it, then retry
                        await Task.Delay(20);
                    }
                }
            }

            await done.Task;
        }

        // Keyed by EventId so redeliveries of the same event land on the same partition
        private static Message<string, byte[]> ToMessage(EventDto eventDto) => new()
        {
            Key = eventDto.EventId.ToString(),
            Value = JsonSerializer.SerializeToUtf8Bytes(eventDto)
        };

        public void Dispose()
        {
            _producer.Flush(TimeSpan.FromSeconds(10));
            _producer.Dispose();
        }
    }
}
