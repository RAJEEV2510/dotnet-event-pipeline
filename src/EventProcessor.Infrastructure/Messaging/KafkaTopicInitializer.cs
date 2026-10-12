using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using EventProcessor.Infrastructure.Settings;
using Microsoft.Extensions.Logging;

namespace EventProcessor.Infrastructure.Messaging
{
    public static class KafkaTopicInitializer
    {
        // Creates the main and dead-letter topics if they don't exist yet.
        // Retries because in Kubernetes the broker may still be starting when the app boots.
        public static async Task EnsureTopicsAsync(KafkaSettings settings, ILogger logger, CancellationToken ct = default)
        {
            using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = settings.BootstrapServers }).Build();

            var topics = new[]
            {
                new TopicSpecification { Name = settings.Topic, NumPartitions = settings.Partitions, ReplicationFactor = settings.ReplicationFactor },
                new TopicSpecification { Name = settings.DeadLetterTopic, NumPartitions = 1, ReplicationFactor = settings.ReplicationFactor }
            };

            for (var attempt = 1; ; attempt++)
            {
                try
                {
                    await admin.CreateTopicsAsync(topics);
                    logger.LogInformation("Created Kafka topics {Topics}", string.Join(", ", topics.Select(t => t.Name)));
                    return;
                }
                catch (CreateTopicsException ex) when (ex.Results.All(r => r.Error.Code is ErrorCode.NoError or ErrorCode.TopicAlreadyExists))
                {
                    logger.LogInformation("Kafka topics already exist");
                    return;
                }
                catch (KafkaException ex) when (attempt < 30)
                {
                    logger.LogWarning("Kafka not ready ({Reason}); retrying topic creation ({Attempt}/30)", ex.Error.Reason, attempt);
                    await Task.Delay(TimeSpan.FromSeconds(2), ct);
                }
            }
        }
    }
}
