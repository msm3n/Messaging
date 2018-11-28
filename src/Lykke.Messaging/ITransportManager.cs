using System;
using Lykke.Messaging.Contract;
using Lykke.Messaging.Transports;

namespace Lykke.Messaging
{
    public interface ITransportManager : IDisposable
    {
        event TransportEventHandler TransportEvents;
        IMessagingSession GetMessagingSession(string transportId, string name, Action onFailure = null);
    }
}