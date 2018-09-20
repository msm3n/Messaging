using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Common.Log;
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

        public TransportManager(ILog log, ITransportResolver transportResolver, params ITransportFactory[] transportFactories)
        {
            m_TransportFactories = transportFactories.Concat(new[] {new InMemoryTransportFactory()}).ToArray();
            _log = log;
            m_TransportResolver = transportResolver ?? throw new ArgumentNullException("transportResolver");
        }

        public ITransportResolver TransportResolver
        {
            get { return m_TransportResolver; }
        }

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
                throw new TransportException(string.Format("Failed to create processing group {0} on transport {1}", name, transportId), e);
            }
        }

        internal ResolvedTransport ResolveTransport(string transportId)
        {
            if (m_IsDisposed.WaitOne(0))
                throw new ObjectDisposedException(string.Format("Can not create transport {0}. TransportManager instance is disposed", transportId));

            var transportInfo = m_TransportResolver.GetTransport(transportId);

            if (transportInfo == null)
                throw new ApplicationException(string.Format("Transport '{0}' is not resolvable", transportId));
            var factory = m_TransportFactories.FirstOrDefault(f => f.Name == transportInfo.Messaging);
            if (factory == null)
                throw new ApplicationException(
                    string.Format("Can not create transport '{0}', {1} messaging is not supported", transportId, transportInfo.Messaging));

            ResolvedTransport transport;

            if (!m_Transports.TryGetValue(transportInfo, out transport))
            {
                lock (m_Transports)
                {
                    if (!m_Transports.TryGetValue(transportInfo, out transport))
                    {
                        transport = new ResolvedTransport(_log, transportInfo, () => ProcessTransportFailure(transportInfo), factory);
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
                throw new TransportException(
                    string.Format("Destination {0} is not properly configured on transport {1}", destination, transportId), e);
            }
        }
    }
}