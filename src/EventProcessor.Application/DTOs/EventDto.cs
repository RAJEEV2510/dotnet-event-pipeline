using System;

namespace EventProcessor.Application.DTOs
{
    public record EventDto(Guid EventId, string Type, string Payload, DateTime CreatedAt);
}
