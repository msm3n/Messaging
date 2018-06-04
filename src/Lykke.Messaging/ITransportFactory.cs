using System;
using Common.Log;
using Lykke.Messaging.Transports;

namespace Lykke.Messaging
{
    public interface ITransportFactory
    {
        string Name { get; }

        [Obsolete]
        ITransport Create(ILog log, TransportInfo transportInfo, Action onFailure);

        ITransport Create(TransportInfo transportInfo, Action onFailure);
    }
}