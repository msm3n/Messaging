using System;
using RabbitMQ.Client;
using Microsoft.Extensions.Logging;

namespace Lykke.Messaging.RabbitMq
{
    public delegate void MessageCallback(
        IBasicProperties properties,
        byte[] body,
        Action<bool> ack);

    internal class Consumer : DefaultBasicConsumer, IDisposable
    {
        private static readonly ILogger<Consumer> _logger = Log.For<Consumer>();
        private readonly MessageCallback _callback;
        private bool _disposed;

        public Consumer(
            IModel model,
            MessageCallback callback)
            : base(model)
        {
            _callback = callback ?? throw new ArgumentNullException(nameof(callback));
        }

        public override void HandleBasicDeliver(
            string consumerTag,
            ulong deliveryTag,
            bool redelivered,
            string exchange,
            string routingKey,
            IBasicProperties properties,
            byte[] body)
        {
            try
            {
                _callback(properties, body, ack =>
                {
                    if (ack) Model.BasicAck(deliveryTag, false);
                    else Model.BasicNack(deliveryTag, false, true);
                });
            }
            catch (Exception e)
            {
                _logger.LogError(e,
                    "Error processing message. DeliveryTag={DeliveryTag}, RoutingKey={RoutingKey}",
                    deliveryTag, routingKey);
                Model.BasicNack(deliveryTag, false, true);
            }
        }

        public void Dispose()
        {
            if (_disposed) return;
            lock (Model)
            {
                if (Model.IsOpen)
                {
                    try { Model.BasicCancel(ConsumerTag); }
                    catch (Exception e)
                    {
                        _logger.LogError(e,
                            "Error cancelling consumer. ConsumerTag={ConsumerTag}",
                            ConsumerTag);
                    }
                }
            }
            _disposed = true;
        }
    }
}