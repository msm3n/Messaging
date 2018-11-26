using System;
using Lykke.Messaging.Contract;
using Lykke.Messaging.Transports;

namespace Lykke.Messaging
{
    internal interface ITransportManager : IDisposable
    {
        event TransportEventHandler TransportEvents;
        IMessagingSession GetMessagingSession(Endpoint endpoint, string name, Action onFailure = null);
    }
}