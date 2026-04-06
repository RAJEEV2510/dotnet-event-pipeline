using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using EventProcessor.Domain;

namespace EventProcessor.Application.Interfaces
{
    public interface IEventRepository
    {
        Task BulkInsertAsync(IEnumerable<Event> events, CancellationToken ct);
    }
}
