using System;
using Microsoft.Extensions.Logging;
using Lykke.Messaging.Contract;
using Lykke.Messaging.Transports;

namespace Lykke.Messaging
{
    public class MessagingSessionWrapper : IMessagingSession
    {
        private static readonly ILogger<MessagingSessionWrapper> _logger = Log.For<MessagingSessionWrapper>();

        private IMessagingSession _messagingSession;

        public string TransportId { get; private set; }
        public string Name { get; private set; }

        public event Action OnFailure;

        public MessagingSessionWrapper(string transportId, string name)
        {
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

            // вариант с LINQ:
            // foreach (Action handler in OnFailure.GetInvocationList().Cast<Action>())

            // вариант без LINQ:
            foreach (Delegate d in OnFailure.GetInvocationList())
            {
                var handler = (Action)d;
                try
                {
                    handler();
                }
                catch (Exception e)
                {
                    _logger.LogError(e, "Handler {Handler} in ReportFailure threw", handler.Method.Name);
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
            return _messagingSession.Subscribe(destination, callback, messageType);
        }

        public Destination CreateTemporaryDestination()
        {
            return _messagingSession.CreateTemporaryDestination();
        }
    }
}