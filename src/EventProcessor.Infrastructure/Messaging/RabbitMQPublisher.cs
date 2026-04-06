using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using EventProcessor.Application.DTOs;
using EventProcessor.Application.Interfaces;
using EventProcessor.Infrastructure.Settings;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace EventProcessor.Infrastructure.Messaging
{
    public class RabbitMQPublisher : IEventPublisher, IDisposable
    {
        private readonly RabbitMQSettings _settings;
        private readonly IConnection _connection;
        private readonly ConcurrentBag<IModel> _channelPool;
        private readonly SemaphoreSlim _poolSemaphore;
        private const int PoolSize = 10;

        public RabbitMQPublisher(IOptions<RabbitMQSettings> settings)
        {
            _settings = settings.Value;

            var factory = new ConnectionFactory
            {
                HostName = _settings.HostName,
                Port = _settings.Port,
                UserName = _settings.UserName,
                Password = _settings.Password,
                DispatchConsumersAsync = true
            };

            _connection = factory.CreateConnection();
            _channelPool = new ConcurrentBag<IModel>();
            _poolSemaphore = new SemaphoreSlim(PoolSize, PoolSize);

            // Pre-create channels and declare topology on the first one
            for (int i = 0; i < PoolSize; i++)
            {
                var channel = _connection.CreateModel();

                if (i == 0)
                {
                    // Setup DLX and DLQ
                    channel.ExchangeDeclare(_settings.DeadLetterExchange, ExchangeType.Direct);
                    channel.QueueDeclare(_settings.DeadLetterQueue, true, false, false);
                    channel.QueueBind(_settings.DeadLetterQueue, _settings.DeadLetterExchange, "");

                    // Setup main Exchange and Queue with DLX
                    channel.ExchangeDeclare(_settings.ExchangeName, ExchangeType.Direct);
                    var args = new Dictionary<string, object>
                    {
                        { "x-dead-letter-exchange", _settings.DeadLetterExchange }
                    };
                    channel.QueueDeclare(_settings.QueueName, true, false, false, args);
                    channel.QueueBind(_settings.QueueName, _settings.ExchangeName, "");
                }

                _channelPool.Add(channel);
            }
        }

        public async Task PublishAsync(EventDto eventDto)
        {
            var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(eventDto));

            await _poolSemaphore.WaitAsync();
            IModel? channel = null;
            try
            {
                if (!_channelPool.TryTake(out channel))
                {
                    // Shouldn't happen due to semaphore, but create a new one as fallback
                    channel = _connection.CreateModel();
                }

                var properties = channel.CreateBasicProperties();
                properties.Persistent = true;

                channel.BasicPublish(
                    exchange: _settings.ExchangeName,
                    routingKey: "",
                    basicProperties: properties,
                    body: body);
            }
            finally
            {
                if (channel != null)
                    _channelPool.Add(channel);
                _poolSemaphore.Release();
            }
        }

        public void Dispose()
        {
            _poolSemaphore.Dispose();
            while (_channelPool.TryTake(out var channel))
            {
                channel.Dispose();
            }
            _connection?.Dispose();
        }
    }
}
