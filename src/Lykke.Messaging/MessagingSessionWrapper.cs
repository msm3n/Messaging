using System;
using Lykke.Messaging.Contract;
using Lykke.Messaging.Transports;

namespace Lykke.Messaging
{
    internal class MessagingSessionWrapper:IMessagingSession
    {
        public string TransportId { get; private set; }
        public string Name { get; private set; }
        private IMessagingSession MessagingSession { get; set; }
        public event Action OnFailure;

        public MessagingSessionWrapper(string transportId, string name)
        {
            TransportId = transportId;
            Name = name;
        }

        public void SetSession(IMessagingSession messagingSession)
        {
            MessagingSession = messagingSession;
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
                catch (Exception)
                {
                    //TODO: log
                }
            }
        }

        public void Dispose()
        {
            if (MessagingSession == null)
                return;
            MessagingSession.Dispose();
            MessagingSession = null;
        }

        public void Send(string destination, BinaryMessage message, int ttl)
        {
            MessagingSession.Send(destination, message, ttl);
        }

        public RequestHandle SendRequest(string destination, BinaryMessage message, Action<BinaryMessage> callback)
        {
            return MessagingSession.SendRequest(destination, message, callback);
        }

        public IDisposable RegisterHandler(string destination, Func<BinaryMessage, BinaryMessage> handler, string messageType)
        {
            return MessagingSession.RegisterHandler(destination, handler, messageType);
        }

        public IDisposable Subscribe(string destination, Action<BinaryMessage, Action<bool>> callback, string messageType)
        {
            return MessagingSession.Subscribe(destination,callback, messageType);
        }

        public Destination CreateTemporaryDestination()
        {
            return MessagingSession.CreateTemporaryDestination();
        }
    }
}