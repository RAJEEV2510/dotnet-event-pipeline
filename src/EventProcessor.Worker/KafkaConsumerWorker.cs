using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using EventProcessor.Application.DTOs;
using EventProcessor.Application.Interfaces;
using EventProcessor.Domain;
using EventProcessor.Infrastructure.Messaging;
using EventProcessor.Infrastructure.Settings;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;

namespace EventProcessor.Worker
{
    // Kafka counterpart of EventConsumerWorker. Each replica joins one consumer group, so Kafka
    // spreads partitions across pods; scaling the Deployment up to the partition count adds throughput.
    public class KafkaConsumerWorker : BackgroundService
    {
        private readonly ILogger<KafkaConsumerWorker> _logger;
        private readonly IServiceProvider _serviceProvider;
        private readonly KafkaSettings _settings;

        public KafkaConsumerWorker(
            ILogger<KafkaConsumerWorker> logger,
            IServiceProvider serviceProvider,
            IOptions<KafkaSettings> settings)
        {
            _logger = logger;
            _serviceProvider = serviceProvider;
            _settings = settings.Value;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await KafkaTopicInitializer.EnsureTopicsAsync(_settings, _logger, stoppingToken);

            // Consume() blocks, so run the loop on its own thread instead of a pool thread
            await Task.Factory.StartNew(() => ConsumeLoop(stoppingToken), stoppingToken,
                TaskCreationOptions.LongRunning, TaskScheduler.Default).Unwrap();
        }

        private async Task ConsumeLoop(CancellationToken stoppingToken)
        {
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = _settings.BootstrapServers,
                GroupId = _settings.GroupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                // Offsets are committed by hand only after a batch is safely in PostgreSQL (at-least-once)
                EnableAutoCommit = false,
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky
            };

            using var consumer = new ConsumerBuilder<string, string>(consumerConfig)
                .SetPartitionsAssignedHandler((_, partitions) =>
                    _logger.LogInformation("Assigned partitions: {Partitions}", string.Join(",", partitions.Select(p => p.Partition.Value))))
                .Build();
            using var dlqProducer = new ProducerBuilder<string, string>(new ProducerConfig
            {
                BootstrapServers = _settings.BootstrapServers,
                EnableIdempotence = true,
                Acks = Acks.All
            }).Build();

            consumer.Subscribe(_settings.Topic);
            _logger.LogInformation("Kafka worker consuming {Topic} as group {GroupId}", _settings.Topic, _settings.GroupId);

            var batch = new List<ConsumeResult<string, string>>(_settings.MaxBatchSize);
            var batchTimer = Stopwatch.StartNew();

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    ConsumeResult<string, string>? result;
                    try
                    {
                        result = consumer.Consume(TimeSpan.FromMilliseconds(100));
                    }
                    catch (ConsumeException ex) when (!ex.Error.IsFatal)
                    {
                        _logger.LogWarning("Kafka consume error: {Reason}", ex.Error.Reason);
                        continue;
                    }

                    if (result != null && !result.IsPartitionEOF)
                    {
                        if (batch.Count == 0) batchTimer.Restart();
                        batch.Add(result);
                    }

                    // Flush on size or age, whichever comes first
                    if (batch.Count >= _settings.MaxBatchSize ||
                        (batch.Count > 0 && batchTimer.ElapsedMilliseconds >= _settings.MaxBatchWaitMs))
                    {
                        await FlushBatchAsync(batch, consumer, dlqProducer, stoppingToken);
                        batch.Clear();
                    }
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
            }
            finally
            {
                if (batch.Count > 0)
                {
                    await FlushBatchAsync(batch, consumer, dlqProducer, CancellationToken.None);
                }
                consumer.Close();
            }
        }

        private async Task FlushBatchAsync(
            List<ConsumeResult<string, string>> batch,
            IConsumer<string, string> consumer,
            IProducer<string, string> dlqProducer,
            CancellationToken ct)
        {
            var events = new List<Event>(batch.Count);
            var poison = new List<ConsumeResult<string, string>>();

            foreach (var result in batch)
            {
                try
                {
                    var dto = JsonSerializer.Deserialize<EventDto>(result.Message.Value);
                    if (dto == null) { poison.Add(result); continue; }

                    events.Add(new Event
                    {
                        EventId = dto.EventId,
                        Type = dto.Type,
                        Payload = dto.Payload,
                        CreatedAt = dto.CreatedAt,
                        ProcessedAt = DateTime.UtcNow
                    });
                }
                catch (JsonException)
                {
                    poison.Add(result);
                }
            }

            var sw = Stopwatch.StartNew();
            try
            {
                using var scope = _serviceProvider.CreateScope();
                var repository = scope.ServiceProvider.GetRequiredService<IEventRepository>();

                var retryPolicy = Policy
                    .Handle<Exception>(ex => ex is not OperationCanceledException)
                    .WaitAndRetryAsync(3, attempt => TimeSpan.FromSeconds(Math.Pow(2, attempt)));

                // Duplicates from redelivery are absorbed by ON CONFLICT (event_id) DO NOTHING
                await retryPolicy.ExecuteAsync(token => repository.BulkInsertAsync(events, token), ct);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogError(ex, "Max retries reached. Sending batch of {Count} to {DeadLetterTopic}.", batch.Count, _settings.DeadLetterTopic);
                poison = batch;
            }

            foreach (var result in poison)
            {
                await dlqProducer.ProduceAsync(_settings.DeadLetterTopic, result.Message, CancellationToken.None);
            }

            // Commit the highest offset per partition (+1 = next message to read)
            var offsets = batch
                .GroupBy(r => r.TopicPartition)
                .Select(g => new TopicPartitionOffset(g.Key, g.Max(r => r.Offset.Value) + 1));
            consumer.Commit(offsets);

            _logger.LogInformation("BATCH_COMPLETE: {Count} events in {ElapsedMs}ms ({DeadLettered} dead-lettered)",
                batch.Count, sw.ElapsedMilliseconds, poison.Count);
        }
    }
}
