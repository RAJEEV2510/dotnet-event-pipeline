using System;

namespace EventProcessor.Domain
{
    public class OutboxMessage
    {
        public long Id { get; set; }
        public Guid EventId { get; set; }
        public string Type { get; set; } = string.Empty;
        public string Content { get; set; } = string.Empty;
        public DateTime CreatedAt { get; set; }
        public DateTime? ProcessedAt { get; set; }
        public int RetryCount { get; set; }
    }
}
