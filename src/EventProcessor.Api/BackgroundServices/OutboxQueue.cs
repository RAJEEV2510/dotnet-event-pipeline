using System.Threading.Channels;
using EventProcessor.Domain;

namespace EventProcessor.Api.BackgroundServices
{
    public class OutboxQueue
    {
        private readonly Channel<OutboxMessage> _channel;

        public OutboxQueue()
        {
            // Bounded channel to prevent memory overflow during massive spikes
            _channel = Channel.CreateBounded<OutboxMessage>(new BoundedChannelOptions(100000)
            {
                FullMode = BoundedChannelFullMode.Wait
            });
        }

        public ChannelReader<OutboxMessage> Reader => _channel.Reader;
        public ChannelWriter<OutboxMessage> Writer => _channel.Writer;
    }
}
