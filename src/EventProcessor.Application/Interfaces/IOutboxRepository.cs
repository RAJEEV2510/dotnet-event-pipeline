using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using EventProcessor.Domain;

namespace EventProcessor.Application.Interfaces
{
    public interface IOutboxRepository
    {
        Task SaveAsync(OutboxMessage message, CancellationToken ct);
        Task BulkSaveAsync(IEnumerable<OutboxMessage> messages, CancellationToken ct);
        Task<IEnumerable<OutboxMessage>> GetUnprocessedMessagesAsync(int batchSize, CancellationToken ct);
        Task MarkAsProcessedAsync(IEnumerable<long> ids, CancellationToken ct);
        Task IncrementRetryCountAsync(IEnumerable<long> ids, CancellationToken ct);
        Task<int> CleanupProcessedAsync(TimeSpan threshold, CancellationToken ct);
    }
}
