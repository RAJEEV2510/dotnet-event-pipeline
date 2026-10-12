namespace EventProcessor.Infrastructure.Settings
{
    public class MessagingSettings
    {
        // Which broker the outbox publishes to and the worker consumes from: "RabbitMQ" or "Kafka"
        public string Transport { get; set; } = "RabbitMQ";

        public bool UseKafka => string.Equals(Transport, "Kafka", System.StringComparison.OrdinalIgnoreCase);
    }

    public class KafkaSettings
    {
        public string BootstrapServers { get; set; } = "localhost:9092";
        public string Topic { get; set; } = "events";
        public string DeadLetterTopic { get; set; } = "events-dlq";
        public string GroupId { get; set; } = "event-processor";
        public int Partitions { get; set; } = 6;
        public short ReplicationFactor { get; set; } = 1;
        public int LingerMs { get; set; } = 5;
        public int MaxBatchSize { get; set; } = 2000;
        public int MaxBatchWaitMs { get; set; } = 200;
    }

    public class RabbitMQSettings
    {
        public string HostName { get; set; } = "localhost";
        public int Port { get; set; } = 5672;
        public string UserName { get; set; } = "guest";
        public string Password { get; set; } = "guest";
        public string ExchangeName { get; set; } = "events_exchange";
        public string QueueName { get; set; } = "events_queue";
        public string DeadLetterExchange { get; set; } = "events_dlx";
        public string DeadLetterQueue { get; set; } = "events_dlq";
        public int PrefetchCount { get; set; } = 100;
    }

    public class PostgreSQLSettings
    {
        public string ConnectionString { get; set; } = "Host=localhost;Port=54321;Database=postgres;Username=postgres;Password=password;";
    }
}
