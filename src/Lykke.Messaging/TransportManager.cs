using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Common.Log;
using Lykke.Common.Log;
using Lykke.Messaging.Contract;
using Lykke.Messaging.InMemory;
using Lykke.Messaging.Transports;

namespace Lykke.Messaging
{
    internal class TransportManager : ITransportManager
    {
        private readonly Dictionary<TransportInfo, ResolvedTransport> m_Transports = new Dictionary<TransportInfo, ResolvedTransport>();
        private readonly ILog _log;
        private readonly ITransportResolver m_TransportResolver;
        private readonly ManualResetEvent m_IsDisposed = new ManualResetEvent(false);
        private readonly ITransportFactory[] m_TransportFactories;
        private readonly  ILogFactory _logFactory;

        [Obsolete]
        public TransportManager(ILog log, ITransportResolver transportResolver, params ITransportFactory[] transportFactories)
        {
            m_TransportFactories = transportFactories.Concat(new[] {new InMemoryTransportFactory()}).ToArray();
            _log = log;
            m_TransportResolver = transportResolver ?? throw new ArgumentNullException(nameof(transportResolver));
        }

        public TransportManager(ILogFactory logFactory, ITransportResolver transportResolver, params ITransportFactory[] transportFactories)
        {
            _logFactory = logFactory ?? throw new ArgumentNullException(nameof(logFactory));
            m_TransportFactories = transportFactories.Concat(new[] { new InMemoryTransportFactory() }).ToArray();
            m_TransportResolver = transportResolver ?? throw new ArgumentNullException(nameof(transportResolver));
        }

        public ITransportResolver TransportResolver => m_TransportResolver;

        #region IDisposable Members

        public void Dispose()
        {
            m_IsDisposed.Set();
            lock (m_Transports)
            {
                foreach (var transport in m_Transports.Values.Distinct())
                {
                    transport.Dispose();
                }
                m_Transports.Clear();
            }
        }

        #endregion

        public event TransportEventHandler TransportEvents;

        public IMessagingSession GetMessagingSession(string transportId, string name, Action onFailure = null)
        {
            ResolvedTransport transport = ResolveTransport(transportId);

            try
            {
                return transport.GetSession(transportId, name, onFailure);
            }
            catch (Exception e)
            {
                throw new TransportException($"Failed to create processing group {name} on transport {transportId}", e);
            }
        }

        internal ResolvedTransport ResolveTransport(string transportId)
        {
            if (m_IsDisposed.WaitOne(0))
                throw new ObjectDisposedException($"Can not create transport {transportId}. TransportManager instance is disposed");

            var transportInfo = m_TransportResolver.GetTransport(transportId);

            if (transportInfo == null)
                throw new ApplicationException($"Transport '{transportId}' is not resolvable");
            var factory = m_TransportFactories.FirstOrDefault(f => f.Name == transportInfo.Messaging);
            if (factory == null)
                throw new ApplicationException($"Can not create transport '{transportId}', {transportInfo.Messaging} messaging is not supported");

            if (!m_Transports.TryGetValue(transportInfo, out var transport))
            {
                lock (m_Transports)
                {
                    if (!m_Transports.TryGetValue(transportInfo, out transport))
                    {
                        transport = _logFactory == null
                            ? new ResolvedTransport(_log, transportInfo, () => ProcessTransportFailure(transportInfo), factory)
                            : new ResolvedTransport(_logFactory, transportInfo, () => ProcessTransportFailure(transportInfo), factory);
                        m_Transports.Add(transportInfo, transport);
                    }
                }
            }
            return transport;
        }

        internal virtual void ProcessTransportFailure(TransportInfo transportInfo)
        {
            ResolvedTransport transport;
            lock (m_Transports)
            {
                if (!m_Transports.TryGetValue(transportInfo, out transport))
                    return;
                m_Transports.Remove(transportInfo);
            }

            var handler = TransportEvents;
            if (handler == null) return;

            lock (transport)
            {
                foreach (var transportId in transport.KnownIds)
                {
                    handler(transportId, Contract.TransportEvents.Failure);
                }
            }
        }

        public bool VerifyDestination(
            string transportId,
            Destination destination,
            EndpointUsage usage,
            bool configureIfRequired,
            out string error)
        {
            ResolvedTransport transport = ResolveTransport(transportId);

            try
            {
                return transport.VerifyDestination(
                    destination,
                    usage,
                    configureIfRequired,
                    out error);
            }
            catch (Exception e)
            {
                throw new TransportException($"Destination {destination} is not properly configured on transport {transportId}", e);
            }
        }
    }
}