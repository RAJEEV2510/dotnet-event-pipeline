using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using EventProcessor.Application.DTOs;
using EventProcessor.Application.Interfaces;
using EventProcessor.Domain;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace EventProcessor.Api.BackgroundServices
{
    public class OutboxProcessor : BackgroundService
    {
        private readonly IServiceProvider _serviceProvider;
        private readonly ILogger<OutboxProcessor> _logger;
        private readonly IEventPublisher _publisher;

        public OutboxProcessor(
            IServiceProvider serviceProvider,
            ILogger<OutboxProcessor> logger,
            IEventPublisher publisher)
        {
            _serviceProvider = serviceProvider;
            _logger = logger;
            _publisher = publisher;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("OutboxProcessor starting...");

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var processed = await ProcessOutboxMessagesAsync(stoppingToken);
                    // If we processed a full batch, immediately loop again (don't sleep)
                    if (processed < 1000)
                    {
                        await Task.Delay(20, stoppingToken);
                    }
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing outbox messages");
                    await Task.Delay(1000, stoppingToken);
                }
            }
        }

        private async Task<int> ProcessOutboxMessagesAsync(CancellationToken ct)
        {
            using var scope = _serviceProvider.CreateScope();
            var outboxRepository = scope.ServiceProvider.GetRequiredService<IOutboxRepository>();

            var messages = await outboxRepository.GetUnprocessedMessagesAsync(1000, ct);
            var outboxMessages = messages.ToList();

            if (outboxMessages.Count == 0) return 0;

            _logger.LogInformation("Processing {Count} outbox messages", outboxMessages.Count);

            var successfullyPublished = new ConcurrentBag<long>();
            var failedToPublish = new ConcurrentBag<long>();

            await Parallel.ForEachAsync(
                outboxMessages,
                new ParallelOptions { MaxDegreeOfParallelism = 10, CancellationToken = ct },
                async (message, _) =>
                {
                    try
                    {
                        var eventDto = new EventDto(message.EventId, message.Type, message.Content, message.CreatedAt);
                        await _publisher.PublishAsync(eventDto);
                        successfullyPublished.Add(message.Id);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to publish event {EventId}. Retry count: {RetryCount}", message.EventId, message.RetryCount);
                        failedToPublish.Add(message.Id);
                    }
                });

            if (successfullyPublished.Count > 0)
            {
                await outboxRepository.MarkAsProcessedAsync(successfullyPublished, ct);
                _logger.LogInformation("Successfully published {Count} events", successfullyPublished.Count);
            }

            if (failedToPublish.Count > 0)
            {
                await outboxRepository.IncrementRetryCountAsync(failedToPublish, ct);
                _logger.LogWarning("Failed to publish {Count} events. Retry counts incremented.", failedToPublish.Count);
            }

            return outboxMessages.Count;
        }
    }
}
