using System;
using RabbitMQ.Client;
using Common.Log;
using Lykke.Common.Log;

namespace Lykke.Messaging.RabbitMq
{
    internal class Consumer : DefaultBasicConsumer, IDisposable
    {
        private readonly ILog _log;
        private readonly Action<IBasicProperties, byte[], Action<bool>> m_Callback;

        [Obsolete]
        public Consumer(
            ILog log,
            IModel model,
            Action<IBasicProperties, byte[], Action<bool>> callback)
            : base(model)
        {
            _log = log;
            m_Callback = callback ?? throw new ArgumentNullException("callback");
        }

        public Consumer(
            ILogFactory logFactory,
            IModel model,
            Action<IBasicProperties, byte[], Action<bool>> callback)
            
            : base(model)
        {
            if (logFactory == null)
            {
                throw new ArgumentNullException(nameof(logFactory));
            }

            _log = logFactory.CreateLog(this);
            m_Callback = callback ?? throw new ArgumentNullException(nameof(callback));
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
                m_Callback(properties, body, ack =>
                {
                    if (ack)
                        Model.BasicAck(deliveryTag, false);
                    else
                        Model.BasicNack(deliveryTag, false, true);
                });
            }
            catch (Exception e)
            {
                _log.WriteError(nameof(Consumer), nameof(HandleBasicDeliver), e);
            }
        }

        public void Dispose()
        {
            lock (Model)
            {
                if (Model.IsOpen)
                {
                    try
                    {
                        Model.BasicCancel(ConsumerTag);
                    }
                    catch (Exception e)
                    {
                        _log.WriteError(nameof(Consumer), nameof(Dispose), e);
                    }
                }
            }
        }
    }
}