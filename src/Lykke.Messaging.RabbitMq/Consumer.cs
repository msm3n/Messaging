using System;
using RabbitMQ.Client;

namespace Lykke.Messaging.RabbitMq
{
    internal class Consumer:DefaultBasicConsumer, IDisposable
    {
        private readonly Action<IBasicProperties, byte[], Action<bool>> m_Callback;

        public Consumer(IModel model, Action<IBasicProperties, byte[], Action<bool>> callback)
            : base(model)
        {
            m_Callback = callback ?? throw new ArgumentNullException("callback");
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
                //TODO:log
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
                        //TODO: log
                    }
                }
            }
        }
    }
}