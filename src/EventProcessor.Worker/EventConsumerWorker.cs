using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using EventProcessor.Application.DTOs;
using EventProcessor.Application.Interfaces;
using EventProcessor.Domain;
using EventProcessor.Infrastructure.Settings;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace EventProcessor.Worker
{
    public class EventConsumerWorker : BackgroundService
    {
        private readonly ILogger<EventConsumerWorker> _logger;
        private readonly IServiceProvider _serviceProvider;
        private readonly RabbitMQSettings _settings;
        private readonly Channel<(EventDto eventDto, ulong deliveryTag, IModel channel)> _channel;

        private IConnection? _connection;
        private readonly List<IModel> _consumerModels = new();
        private const int ConsumerCount = 10;
        private const int BatchProcessorCount = 3;

        public EventConsumerWorker(
            ILogger<EventConsumerWorker> logger,
            IServiceProvider serviceProvider,
            IOptions<RabbitMQSettings> settings)
        {
            _logger = logger;
            _serviceProvider = serviceProvider;
            _settings = settings.Value;

            _channel = Channel.CreateBounded<(EventDto, ulong, IModel)>(new BoundedChannelOptions(50_000)
            {
                FullMode = BoundedChannelFullMode.Wait
            });
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Worker starting with {ConsumerCount} consumers and {BatchProcessorCount} batch processors...",
                ConsumerCount, BatchProcessorCount);

            InitializeRabbitMQ();

            if (_connection == null)
            {
                _logger.LogCritical("Could not initialize RabbitMQ connection");
                return;
            }

            for (int i = 0; i < ConsumerCount; i++)
            {
                var consumerModel = _connection.CreateModel();
                _consumerModels.Add(consumerModel);
                consumerModel.BasicQos(0, (ushort)_settings.PrefetchCount, false);

                var consumer = new AsyncEventingBasicConsumer(consumerModel);
                consumer.Received += async (model, ea) =>
                {
                    try
                    {
                        var body = ea.Body.ToArray();
                        var message = Encoding.UTF8.GetString(body);
                        var eventDto = JsonSerializer.Deserialize<EventDto>(message);

                        if (eventDto != null)
                        {
                            await _channel.Writer.WriteAsync((eventDto, ea.DeliveryTag, consumerModel), stoppingToken);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error processing individual message. Sending to DLQ.");
                        consumerModel.BasicNack(ea.DeliveryTag, false, false);
                    }
                };

                consumerModel.BasicConsume(queue: _settings.QueueName, autoAck: false, consumer: consumer);
            }

            // Start multiple batch processors
            var processors = new Task[BatchProcessorCount];
            for (int i = 0; i < BatchProcessorCount; i++)
            {
                processors[i] = Task.Run(() => ProcessBatchesAsync(stoppingToken), stoppingToken);
            }

            await Task.WhenAll(processors);
        }

        private void InitializeRabbitMQ()
        {
            var factory = new ConnectionFactory
            {
                HostName = _settings.HostName,
                Port = _settings.Port,
                UserName = _settings.UserName,
                Password = _settings.Password,
                DispatchConsumersAsync = true
            };

            _connection = factory.CreateConnection();
        }

        private async Task ProcessBatchesAsync(CancellationToken stoppingToken)
        {
            var batch = new List<(EventDto eventDto, ulong deliveryTag, IModel channel)>();
            var maxBatchSize = 2000;

            while (!stoppingToken.IsCancellationRequested || _channel.Reader.Count > 0)
            {
                try
                {
                    using var cts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                    if (stoppingToken.IsCancellationRequested) cts.CancelAfter(500);

                    if (await _channel.Reader.WaitToReadAsync(cts.Token))
                    {
                        while (batch.Count < maxBatchSize && _channel.Reader.TryRead(out var item))
                        {
                            batch.Add(item);
                        }
                    }

                    if (batch.Count > 0)
                    {
                        await FlushBatchAsync(batch, stoppingToken.IsCancellationRequested ? CancellationToken.None : stoppingToken);
                        batch.Clear();
                    }
                }
                catch (OperationCanceledException)
                {
                    if (batch.Count > 0)
                    {
                        await FlushBatchAsync(batch, CancellationToken.None);
                        batch.Clear();
                    }
                    if (stoppingToken.IsCancellationRequested && _channel.Reader.Count == 0) break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Critical error in batch processing loop");
                    await Task.Delay(1000, stoppingToken);
                }
            }
        }

        private async Task FlushBatchAsync(List<(EventDto eventDto, ulong deliveryTag, IModel channel)> batch, CancellationToken ct)
        {
            _logger.LogInformation("Flushing batch of {Count} events", batch.Count);

            using var scope = _serviceProvider.CreateScope();
            var repository = scope.ServiceProvider.GetRequiredService<IEventRepository>();

            var events = batch.Select(x => new Event
            {
                EventId = x.eventDto.EventId,
                Type = x.eventDto.Type,
                Payload = x.eventDto.Payload,
                CreatedAt = x.eventDto.CreatedAt,
                ProcessedAt = DateTime.UtcNow
            }).ToList();

            var retryPolicy = Policy
                .Handle<Exception>()
                .WaitAndRetryAsync(3, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)));

            var sw = Stopwatch.StartNew();
            try
            {
                await retryPolicy.ExecuteAsync(async () =>
                {
                    await repository.BulkInsertAsync(events, ct);
                });

                // Acknowledge each message on its respective channel
                foreach (var item in batch)
                {
                    item.channel.BasicAck(item.deliveryTag, false);
                }

                sw.Stop();
                _logger.LogInformation("BATCH_COMPLETE: {Count} events in {ElapsedMs}ms", batch.Count, sw.ElapsedMilliseconds);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Max retries reached. Failing batch to DLQ.");
                foreach (var item in batch)
                {
                    item.channel.BasicNack(item.deliveryTag, false, false);
                }
            }
        }

        public override void Dispose()
        {
            foreach (var model in _consumerModels) model.Dispose();
            _connection?.Dispose();
            base.Dispose();
        }
    }
}
