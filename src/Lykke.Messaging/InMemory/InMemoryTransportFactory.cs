using System;
using System.Collections.Generic;
using Common.Log;
using Lykke.Messaging.Transports;

namespace Lykke.Messaging.InMemory
{
    internal class InMemoryTransportFactory : ITransportFactory
    {
        private readonly Dictionary<TransportInfo, InMemoryTransport> m_Transports = new Dictionary<TransportInfo, InMemoryTransport>();

        public string Name
        {
            get { return "InMemory"; }
        }

        [Obsolete]
        public ITransport Create(ILog log, TransportInfo transportInfo, Action onFailure)
        {
            lock (m_Transports)
            {
                InMemoryTransport transport;
                if (!m_Transports.TryGetValue(transportInfo, out transport))
                {
                    transport = new InMemoryTransport();
                    m_Transports.Add(transportInfo, transport);
                }
                return transport;
            }
        }

        public ITransport Create(TransportInfo transportInfo, Action onFailure)
        {
            lock (m_Transports)
            {
                if (m_Transports.TryGetValue(transportInfo, out var transport))
                {
                    return transport;
                }

                transport = new InMemoryTransport();
                m_Transports.Add(transportInfo, transport);
                return transport;
            }
        }
    }
}