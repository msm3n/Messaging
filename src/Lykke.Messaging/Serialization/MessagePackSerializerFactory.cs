using Common.Log;
using Lykke.Common.Log;
using System;

namespace Lykke.Messaging.Serialization
{
    public class MessagePackSerializerFactory : ISerializerFactory
    {
        private readonly ILog _log;
        private readonly ILogFactory _logFactory;

        public string SerializationFormat => "messagepack";

        [Obsolete]
        public MessagePackSerializerFactory(ILog log)
        {
            _log = log ?? throw new ArgumentNullException(nameof(log));
        }

        public MessagePackSerializerFactory(ILogFactory logFactory)
        {
            _logFactory = logFactory ?? throw new ArgumentNullException(nameof(logFactory));
        }

        public IMessageSerializer<TMessage> Create<TMessage>()
        {
            return _logFactory != null
                ? new MessagePackSerializer<TMessage>(_logFactory)
                : new MessagePackSerializer<TMessage>(_log);
        }

        public static class Defaults
        {
            public static MessagePack.IFormatterResolver FormatterResolver { get; set; } = null;
        }
    }
}