using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using EventProcessor.Application.Interfaces;
using EventProcessor.Domain;
using Npgsql;
using NpgsqlTypes;

namespace EventProcessor.Infrastructure.Persistence
{
    public class PostgreSQLEventRepository : IEventRepository
    {
        private readonly NpgsqlDataSource _dataSource;

        public PostgreSQLEventRepository(NpgsqlDataSource dataSource)
        {
            _dataSource = dataSource;
        }

        public async Task BulkInsertAsync(IEnumerable<Event> events, CancellationToken ct)
        {
            var eventList = events.ToList();
            if (eventList.Count == 0) return;

            await using var connection = await _dataSource.OpenConnectionAsync(ct);

            // Use a temp table + COPY + INSERT ... ON CONFLICT for dedup
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
                CREATE TEMP TABLE _events_staging (
                    event_id UUID, type TEXT, payload TEXT, created_at TIMESTAMP, processed_at TIMESTAMP
                ) ON COMMIT DROP;";
            await cmd.ExecuteNonQueryAsync(ct);

            await using var writer = await connection.BeginBinaryImportAsync(
                "COPY _events_staging (event_id, type, payload, created_at, processed_at) FROM STDIN (FORMAT BINARY)", ct);

            foreach (var e in eventList)
            {
                await writer.StartRowAsync(ct);
                await writer.WriteAsync(e.EventId, NpgsqlDbType.Uuid, ct);
                await writer.WriteAsync(e.Type, NpgsqlDbType.Text, ct);
                await writer.WriteAsync(e.Payload, NpgsqlDbType.Text, ct);
                await writer.WriteAsync(e.CreatedAt, NpgsqlDbType.Timestamp, ct);
                await writer.WriteAsync(e.ProcessedAt, NpgsqlDbType.Timestamp, ct);
            }

            await writer.CompleteAsync(ct);

            await using var insertCmd = connection.CreateCommand();
            insertCmd.CommandText = @"
                INSERT INTO events (event_id, type, payload, created_at, processed_at)
                SELECT event_id, type, payload, created_at, processed_at FROM _events_staging
                ON CONFLICT (event_id) DO NOTHING;";
            await insertCmd.ExecuteNonQueryAsync(ct);
        }
    }
}
