using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using EventProcessor.Application.Interfaces;
using EventProcessor.Domain;
using Npgsql;
using NpgsqlTypes;

namespace EventProcessor.Infrastructure.Persistence
{
    public class PostgreSQLOutboxRepository : IOutboxRepository
    {
        private readonly NpgsqlDataSource _dataSource;

        public PostgreSQLOutboxRepository(NpgsqlDataSource dataSource)
        {
            _dataSource = dataSource;
        }

        public async Task SaveAsync(OutboxMessage message, CancellationToken ct)
        {
            await using var connection = await _dataSource.OpenConnectionAsync(ct);
            const string sql = @"
                INSERT INTO outbox_messages (event_id, type, content, created_at, retry_count)
                VALUES (@EventId, @Type, @Content, @CreatedAt, 0);";

            await connection.ExecuteAsync(new CommandDefinition(sql, message, cancellationToken: ct));
        }

        public async Task BulkSaveAsync(IEnumerable<OutboxMessage> messages, CancellationToken ct)
        {
            var messageList = messages.ToList();
            if (messageList.Count == 0) return;

            await using var connection = await _dataSource.OpenConnectionAsync(ct);

            await using var writer = await connection.BeginBinaryImportAsync(
                "COPY outbox_messages (event_id, type, content, created_at, retry_count) FROM STDIN (FORMAT BINARY)", ct);

            foreach (var message in messageList)
            {
                await writer.StartRowAsync(ct);
                await writer.WriteAsync(message.EventId, NpgsqlDbType.Uuid, ct);
                await writer.WriteAsync(message.Type, NpgsqlDbType.Text, ct);
                await writer.WriteAsync(message.Content, NpgsqlDbType.Text, ct);
                await writer.WriteAsync(message.CreatedAt, NpgsqlDbType.Timestamp, ct);
                await writer.WriteAsync(0, NpgsqlDbType.Integer, ct);
            }

            await writer.CompleteAsync(ct);
        }

        public async Task<IEnumerable<OutboxMessage>> GetUnprocessedMessagesAsync(int batchSize, CancellationToken ct)
        {
            await using var connection = await _dataSource.OpenConnectionAsync(ct);
            const string sql = @"
                SELECT id, event_id as EventId, type, content, created_at as CreatedAt, retry_count as RetryCount
                FROM outbox_messages
                WHERE processed_at IS NULL AND retry_count < 5
                ORDER BY id ASC
                LIMIT @BatchSize
                FOR UPDATE SKIP LOCKED;";

            return await connection.QueryAsync<OutboxMessage>(new CommandDefinition(sql, new { BatchSize = batchSize }, cancellationToken: ct));
        }

        public async Task MarkAsProcessedAsync(IEnumerable<long> ids, CancellationToken ct)
        {
            var idArray = ids.ToArray();
            if (!idArray.Any()) return;

            await using var connection = await _dataSource.OpenConnectionAsync(ct);
            const string sql = @"
                UPDATE outbox_messages
                SET processed_at = @ProcessedAt
                WHERE id = ANY(@Ids);";

            await connection.ExecuteAsync(new CommandDefinition(sql, new { ProcessedAt = DateTime.UtcNow, Ids = idArray }, cancellationToken: ct));
        }

        public async Task IncrementRetryCountAsync(IEnumerable<long> ids, CancellationToken ct)
        {
            var idArray = ids.ToArray();
            if (!idArray.Any()) return;

            await using var connection = await _dataSource.OpenConnectionAsync(ct);
            const string sql = @"
                UPDATE outbox_messages
                SET retry_count = retry_count + 1
                WHERE id = ANY(@Ids);";

            await connection.ExecuteAsync(new CommandDefinition(sql, new { Ids = idArray }, cancellationToken: ct));
        }

        public async Task<int> CleanupProcessedAsync(TimeSpan threshold, CancellationToken ct)
        {
            var thresholdTime = DateTime.UtcNow.Subtract(threshold);
            var totalDeleted = 0;
            const int batchSize = 10_000;

            await using var connection = await _dataSource.OpenConnectionAsync(ct);
            const string sql = @"
                DELETE FROM outbox_messages
                WHERE id IN (
                    SELECT id FROM outbox_messages
                    WHERE processed_at < @Threshold
                    LIMIT @BatchSize
                );";

            int deleted;
            do
            {
                deleted = await connection.ExecuteAsync(new CommandDefinition(
                    sql,
                    new { Threshold = thresholdTime, BatchSize = batchSize },
                    cancellationToken: ct));
                totalDeleted += deleted;
            }
            while (deleted == batchSize);

            return totalDeleted;
        }
    }
}
