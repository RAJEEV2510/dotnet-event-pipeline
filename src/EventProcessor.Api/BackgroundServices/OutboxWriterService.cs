using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using EventProcessor.Application.Interfaces;
using EventProcessor.Domain;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace EventProcessor.Api.BackgroundServices
{
    public class OutboxWriterService : BackgroundService
    {
        private readonly OutboxQueue _queue;
        private readonly IServiceProvider _serviceProvider;
        private readonly ILogger<OutboxWriterService> _logger;
        private const int WriterCount = 3;
        private const int MaxBatchSize = 2000;

        public OutboxWriterService(
            OutboxQueue queue,
            IServiceProvider serviceProvider,
            ILogger<OutboxWriterService> logger)
        {
            _queue = queue;
            _serviceProvider = serviceProvider;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("OutboxWriterService starting with {WriterCount} writers...", WriterCount);

            var writers = new Task[WriterCount];
            for (int i = 0; i < WriterCount; i++)
            {
                var writerId = i;
                writers[i] = Task.Run(() => RunWriterAsync(writerId, stoppingToken), stoppingToken);
            }

            await Task.WhenAll(writers);
        }

        private async Task RunWriterAsync(int writerId, CancellationToken stoppingToken)
        {
            var batch = new List<OutboxMessage>();

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    if (await _queue.Reader.WaitToReadAsync(stoppingToken))
                    {
                        while (batch.Count < MaxBatchSize && _queue.Reader.TryRead(out var message))
                        {
                            batch.Add(message);
                        }

                        if (batch.Count > 0)
                        {
                            await FlushBatchAsync(batch, stoppingToken);
                            batch.Clear();
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error in OutboxWriter-{WriterId}", writerId);
                    await Task.Delay(1000, stoppingToken);
                }
            }

            // Final flush on shutdown
            while (_queue.Reader.TryRead(out var message))
            {
                batch.Add(message);
                if (batch.Count >= MaxBatchSize)
                {
                    await FlushBatchAsync(batch, CancellationToken.None);
                    batch.Clear();
                }
            }
            if (batch.Count > 0) await FlushBatchAsync(batch, CancellationToken.None);
        }

        private async Task FlushBatchAsync(List<OutboxMessage> batch, CancellationToken ct)
        {
            using var scope = _serviceProvider.CreateScope();
            var repository = scope.ServiceProvider.GetRequiredService<IOutboxRepository>();
            await repository.BulkSaveAsync(batch, ct);
        }
    }
}
