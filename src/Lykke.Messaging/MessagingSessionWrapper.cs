using System;
using Common.Log;
using Lykke.Messaging.Contract;
using Lykke.Messaging.Transports;

namespace Lykke.Messaging
{
    internal class MessagingSessionWrapper:IMessagingSession
    {
        private readonly ILog _log;

        private IMessagingSession _messagingSession;

        public string TransportId { get; private set; }
        public string Name { get; private set; }

        public event Action OnFailure;

        public MessagingSessionWrapper(ILog log, string transportId, string name)
        {
            _log = log;

            TransportId = transportId;
            Name = name;
        }

        public void SetSession(IMessagingSession messagingSession)
        {
            _messagingSession = messagingSession;
        }

        public void ReportFailure()
        {
            if (OnFailure == null)
                return;

            foreach (var handler in OnFailure.GetInvocationList())
            {
                try
                {
                    handler.DynamicInvoke();
                }
                catch (Exception e)
                {
                    _log.WriteError(nameof(MessagingSessionWrapper), nameof(ReportFailure), e);
                }
            }
        }

        public void Dispose()
        {
            if (_messagingSession == null)
                return;
            _messagingSession.Dispose();
            _messagingSession = null;
        }

        public void Send(string destination, BinaryMessage message, int ttl)
        {
            _messagingSession.Send(destination, message, ttl);
        }

        public RequestHandle SendRequest(string destination, BinaryMessage message, Action<BinaryMessage> callback)
        {
            return _messagingSession.SendRequest(destination, message, callback);
        }

        public IDisposable RegisterHandler(string destination, Func<BinaryMessage, BinaryMessage> handler, string messageType)
        {
            return _messagingSession.RegisterHandler(destination, handler, messageType);
        }

        public IDisposable Subscribe(string destination, Action<BinaryMessage, Action<bool>> callback, string messageType)
        {
            return _messagingSession.Subscribe(destination,callback, messageType);
        }

        public Destination CreateTemporaryDestination()
        {
            return _messagingSession.CreateTemporaryDestination();
        }
    }
}