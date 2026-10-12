using System.Collections.Generic;
using System.Threading.Tasks;
using EventProcessor.Application.DTOs;

namespace EventProcessor.Application.Interfaces
{
    public interface IEventPublisher
    {
        Task PublishAsync(EventDto eventDto);

        // Completes when every event in the batch is acknowledged by the broker
        Task PublishBatchAsync(IReadOnlyList<EventDto> events);
    }
}
