using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using RabbitMQ.Client;
using Common.Log;
using Lykke.Common.Log;

namespace Lykke.Messaging.RabbitMq
{
    public class SharedConsumer : DefaultBasicConsumer,IDisposable
    {
        private readonly ILog _log;
        private readonly Dictionary<string, Action<IBasicProperties, byte[], Action<bool>>> m_Callbacks
            = new Dictionary<string, Action<IBasicProperties, byte[], Action<bool>>>();
        private readonly AutoResetEvent m_CallBackAdded = new AutoResetEvent(false);
        private readonly ManualResetEvent m_Stop = new ManualResetEvent(false);

        [Obsolete]
        public SharedConsumer(ILog log, IModel model) : base(model)
        {
            _log = log;
        }

        public SharedConsumer(ILogFactory logFactory, IModel model) : base(model)
        {
            if (logFactory == null)
            {
                throw new ArgumentNullException(nameof(logFactory));
            }

            _log = logFactory.CreateLog(this);
        }

        public void AddCallback(Action<IBasicProperties, byte[], Action<bool>> callback, string messageType)
        {
            if (callback == null) throw new ArgumentNullException("callback");
            if (string.IsNullOrEmpty(messageType)) throw new ArgumentNullException("messageType");
            lock (m_Callbacks)
            {
                if (m_Callbacks.ContainsKey(messageType))
                    throw new InvalidOperationException("Attempt to subscribe for same destination twice.");
                m_Callbacks[messageType] = callback;
                m_CallBackAdded.Set();
            }
        }

        public bool RemoveCallback(string messageType)
        {
            lock (m_Callbacks)
            {
                if (!m_Callbacks.Remove(messageType))
                    throw new InvalidOperationException("Unsubscribe from not subscribed message type");
                if (m_Callbacks.Any())
                    return true;
            }
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
                Action<IBasicProperties, byte[], Action<bool>> callback;
                lock (m_Callbacks)
                {
                    if(waitForCallback)
                        m_CallBackAdded.Reset();
                    m_Callbacks.TryGetValue(properties.Type, out callback);
                }
                if (callback != null)
                {
                    try
                    {
                        callback(properties, body,ack =>
                            {
                                if(ack)
                                    Model.BasicAck(deliveryTag, false);
                                else
                                    //TODO: allow callback to decide whether to redeliver
                                    Model.BasicNack(deliveryTag, false,true);
                            });
                    }
                    catch (Exception e)
                    {
                        _log.WriteError(nameof(SharedConsumer), nameof(HandleBasicDeliver), e);
                    }
                    return;
                }

                if (!waitForCallback || WaitHandle.WaitAny(new WaitHandle[] { m_CallBackAdded, m_Stop }) == 1)
                {
                    //The registered callback is not the one we are waiting for. (Nack message for later redelivery and free processing thread for it to process other callback registration)
                    //or subscription is canceling, returning message of unknown type to queue
                    Model.BasicNack(deliveryTag, false,true);
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
                        _log.WriteError(nameof(SharedConsumer), nameof(Stop), e);
                    }
                }
            }
        }
    }
}