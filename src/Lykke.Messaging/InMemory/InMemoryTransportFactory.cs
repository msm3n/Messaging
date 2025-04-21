using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Lykke.Messaging.Transports;

namespace Lykke.Messaging.InMemory
{
    internal class InMemoryTransportFactory : ITransportFactory
    {
        private readonly Dictionary<TransportInfo, InMemoryTransport> m_Transports = new Dictionary<TransportInfo, InMemoryTransport>();

        public string Name => "InMemory";

        [Obsolete]
        public ITransport Create(ILoggerFactory loggerFactory, TransportInfo transportInfo, Action onFailure)
        {
            // forward to the new overload; ILoggerFactory is not used for in‑memory transport
            return Create(transportInfo, onFailure);
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