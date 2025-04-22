using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using RabbitMQ.Client;
using Microsoft.Extensions.Logging;

namespace Lykke.Messaging.RabbitMq
{
    public class SharedConsumer : DefaultBasicConsumer, IDisposable
    {
        private static readonly ILogger<SharedConsumer> _log = Log.For<SharedConsumer>();

        private readonly ConcurrentDictionary<string, MessageCallback> m_Callbacks
            = new ConcurrentDictionary<string, MessageCallback>();
        private readonly AutoResetEvent m_CallBackAdded = new AutoResetEvent(false);
        private readonly ManualResetEvent m_Stop = new ManualResetEvent(false);

        public SharedConsumer(IModel model) : base(model)
        {
        }

        public void AddCallback(MessageCallback callback, string messageType)
        {
            if (callback == null) throw new ArgumentNullException(nameof(callback));
            if (string.IsNullOrEmpty(messageType)) throw new ArgumentNullException(nameof(messageType));
            if (!m_Callbacks.TryAdd(messageType, callback))
                throw new InvalidOperationException("Attempt to subscribe for same destination twice.");
            m_CallBackAdded.Set();
        }

        public bool RemoveCallback(string messageType)
        {
            if (!m_Callbacks.TryRemove(messageType, out _))
                throw new InvalidOperationException("Unsubscribe from not subscribed message type");
            if (!m_Callbacks.IsEmpty)
                return true;
            Stop();
            return false;
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
            bool waitForCallback = true;
            while (true)
            {
                if (waitForCallback)
                    m_CallBackAdded.Reset();
                m_Callbacks.TryGetValue(properties.Type, out var callback);

                if (callback != null)
                {
                    try
                    {
                        callback(properties, body, ack =>
                        {
                            if (ack)
                                Model.BasicAck(deliveryTag, false);
                            else
                                //TODO: allow callback to decide whether to redeliver
                                Model.BasicNack(deliveryTag, false, true);
                        });
                    }
                    catch (Exception e)
                    {
                        _log.LogError(e, "Error in {Class}.{Method}", nameof(SharedConsumer), nameof(HandleBasicDeliver));
                    }
                    return;
                }

                if (!waitForCallback || WaitHandle.WaitAny(new WaitHandle[] { m_CallBackAdded, m_Stop }) == 1)
                {
                    //The registered callback is not the one we are waiting for. (Nack message for later redelivery and free processing thread for it to process other callback registration)
                    //or subscription is canceling, returning message of unknown type to queue
                    Model.BasicNack(deliveryTag, false, true);
                    break;
                }
                waitForCallback = false;
            }
        }

        public void Dispose()
        {
            Stop();
        }

        private void Stop()
        {
            m_Stop.Set();
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
                        _log.LogError(e, "Error in {Class}.{Method}", nameof(SharedConsumer), nameof(Stop));
                    }
                }
            }
        }
    }
}