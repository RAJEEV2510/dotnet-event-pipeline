using System;
using System.Threading;
using System.Threading.Tasks;
using EventProcessor.Application.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace EventProcessor.Api.BackgroundServices
{
    public class OutboxCleanupService : BackgroundService
    {
        private readonly IServiceProvider _serviceProvider;
        private readonly ILogger<OutboxCleanupService> _logger;

        public OutboxCleanupService(IServiceProvider serviceProvider, ILogger<OutboxCleanupService> logger)
        {
            _serviceProvider = serviceProvider;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("OutboxCleanupService starting...");

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    using var scope = _serviceProvider.CreateScope();
                    var outboxRepository = scope.ServiceProvider.GetRequiredService<IOutboxRepository>();

                    // Clean up processed messages older than 1 minute
                    int deletedCount = await outboxRepository.CleanupProcessedAsync(TimeSpan.FromMinutes(1), stoppingToken);
                    
                    if (deletedCount > 0)
                    {
                        _logger.LogInformation("Cleaned up {Count} processed outbox messages", deletedCount);
                    }
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error during outbox cleanup");
                }

                await Task.Delay(TimeSpan.FromMinutes(1), stoppingToken); // Run every 1 minute
            }
        }
    }
}
