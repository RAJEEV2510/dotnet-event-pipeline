using System.Threading.Tasks;
using EventProcessor.Application.DTOs;

namespace EventProcessor.Application.Interfaces
{
    public interface IEventPublisher
    {
        Task PublishAsync(EventDto eventDto);
    }
}
