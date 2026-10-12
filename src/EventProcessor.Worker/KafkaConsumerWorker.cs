using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Channels;
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
    //
    // Two stages run concurrently inside each pod:
    //   consume loop  -> builds batches (up to MaxBatchSize or MaxBatchWaitMs) -> bounded channel
    //   flush loop    -> COPY batch into PostgreSQL -> store offsets
    // so the pod keeps fetching from Kafka while the previous batch is being written.
    // Offsets are stored only after the batch is in PostgreSQL and auto-committed from there,
    // which keeps delivery at-least-once; ON CONFLICT (event_id) absorbs any redelivery.
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

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = _settings.BootstrapServers,
                GroupId = _settings.GroupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                // Commit only offsets we stored ourselves, after the PostgreSQL write
                EnableAutoCommit = true,
                EnableAutoOffsetStore = false,
                AutoCommitIntervalMs = 1000,
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
                // Pull large chunks per request instead of many small fetches
                FetchMinBytes = 64 * 1024,
                FetchWaitMaxMs = 50,
                MaxPartitionFetchBytes = 8 * 1024 * 1024
            };

            using var consumer = new ConsumerBuilder<string, byte[]>(consumerConfig)
                .SetPartitionsAssignedHandler((_, partitions) =>
                    _logger.LogInformation("Assigned partitions: {Partitions}", string.Join(",", partitions.Select(p => p.Partition.Value))))
                .Build();
            using var dlqProducer = new ProducerBuilder<string, byte[]>(new ProducerConfig
            {
                BootstrapServers = _settings.BootstrapServers,
                EnableIdempotence = true,
                Acks = Acks.All
            }).Build();

            var batches = Channel.CreateBounded<List<ConsumeResult<string, byte[]>>>(
                new BoundedChannelOptions(_settings.FlushQueueDepth) { SingleReader = true, SingleWriter = true });

            consumer.Subscribe(_settings.Topic);
            _logger.LogInformation("Kafka worker consuming {Topic} as group {GroupId} (batch {BatchSize}, wait {WaitMs} ms)",
                _settings.Topic, _settings.GroupId, _settings.MaxBatchSize, _settings.MaxBatchWaitMs);

            // Consume() blocks, so the consume loop gets its own thread
            var consumeTask = Task.Factory.StartNew(() => ConsumeLoop(consumer, batches.Writer, stoppingToken),
                CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default).Unwrap();
            var flushTask = FlushLoop(consumer, dlqProducer, batches.Reader);

            await Task.WhenAll(consumeTask, flushTask);
            consumer.Close();
        }

        private async Task ConsumeLoop(
            IConsumer<string, byte[]> consumer,
            ChannelWriter<List<ConsumeResult<string, byte[]>>> output,
            CancellationToken stoppingToken)
        {
            var batch = new List<ConsumeResult<string, byte[]>>(_settings.MaxBatchSize);
            var batchTimer = Stopwatch.StartNew();

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    ConsumeResult<string, byte[]>? result;
                    try
                    {
                        result = consumer.Consume(TimeSpan.FromMilliseconds(50));
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

                    // Hand off on size or age, whichever comes first. WriteAsync waits when the
                    // flusher is behind, which stops consumption (backpressure) instead of buffering forever.
                    if (batch.Count >= _settings.MaxBatchSize ||
                        (batch.Count > 0 && batchTimer.ElapsedMilliseconds >= _settings.MaxBatchWaitMs))
                    {
                        await output.WriteAsync(batch, stoppingToken);
                        batch = new List<ConsumeResult<string, byte[]>>(_settings.MaxBatchSize);
                    }
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
            }
            finally
            {
                // On shutdown, hand over whatever is left so it is written before the process exits
                if (batch.Count > 0) await output.WriteAsync(batch, CancellationToken.None);
                output.Complete();
            }
        }

        private async Task FlushLoop(
            IConsumer<string, byte[]> consumer,
            IProducer<string, byte[]> dlqProducer,
            ChannelReader<List<ConsumeResult<string, byte[]>>> input)
        {
            await foreach (var batch in input.ReadAllAsync())
            {
                await FlushBatchAsync(batch, consumer, dlqProducer);
            }
        }

        private async Task FlushBatchAsync(
            List<ConsumeResult<string, byte[]>> batch,
            IConsumer<string, byte[]> consumer,
            IProducer<string, byte[]> dlqProducer)
        {
            var events = new List<Event>(batch.Count);
            var deadLetters = new List<ConsumeResult<string, byte[]>>();
            var processedAt = DateTime.UtcNow;

            foreach (var result in batch)
            {
                try
                {
                    var dto = JsonSerializer.Deserialize<EventDto>(result.Message.Value);
                    if (dto == null) { deadLetters.Add(result); continue; }

                    events.Add(new Event
                    {
                        EventId = dto.EventId,
                        Type = dto.Type,
                        Payload = dto.Payload,
                        CreatedAt = dto.CreatedAt,
                        ProcessedAt = processedAt
                    });
                }
                catch (JsonException)
                {
                    deadLetters.Add(result);
                }
            }

            var sw = Stopwatch.StartNew();
            try
            {
                using var scope = _serviceProvider.CreateScope();
                var repository = scope.ServiceProvider.GetRequiredService<IEventRepository>();

                var retryPolicy = Policy
                    .Handle<Exception>()
                    .WaitAndRetryAsync(3, attempt => TimeSpan.FromSeconds(Math.Pow(2, attempt)));

                // Duplicates from redelivery are absorbed by ON CONFLICT (event_id) DO NOTHING
                await retryPolicy.ExecuteAsync(() => repository.BulkInsertAsync(events, CancellationToken.None));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Max retries reached. Sending batch of {Count} to {DeadLetterTopic}.", batch.Count, _settings.DeadLetterTopic);
                deadLetters = batch;
            }

            foreach (var result in deadLetters)
            {
                await dlqProducer.ProduceAsync(_settings.DeadLetterTopic, result.Message);
            }

            // Store the next offset to read for each partition; the consumer auto-commits stored offsets.
            // A partition revoked during a rebalance can't be stored: its new owner re-reads those
            // events and the database drops the duplicates.
            foreach (var group in batch.GroupBy(r => r.TopicPartition))
            {
                try
                {
                    consumer.StoreOffset(new TopicPartitionOffset(group.Key, group.Max(r => r.Offset.Value) + 1));
                }
                catch (KafkaException ex) when (ex.Error.Code == ErrorCode.Local_State)
                {
                    _logger.LogInformation("Partition {Partition} was revoked before its offset could be stored", group.Key.Partition.Value);
                }
            }

            var seconds = Math.Max(sw.Elapsed.TotalSeconds, 0.001);
            _logger.LogInformation("BATCH_COMPLETE: {Count} events in {ElapsedMs}ms ({Rate:N0} events/sec, {DeadLettered} dead-lettered)",
                batch.Count, sw.ElapsedMilliseconds, events.Count / seconds, deadLetters.Count);
        }
    }
}
