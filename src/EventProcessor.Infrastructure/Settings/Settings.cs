namespace EventProcessor.Infrastructure.Settings
{
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
