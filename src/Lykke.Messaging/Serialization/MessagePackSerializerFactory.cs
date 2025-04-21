using Common.Log;
using Lykke.Common.Log;
using System;

namespace Lykke.Messaging.Serialization
{
    public class MessagePackSerializerFactory : ISerializerFactory
    {

        public SerializationFormat SerializationFormat => SerializationFormat.MessagePack;

        public IMessageSerializer<TMessage> Create<TMessage>()
        {
            return new MessagePackSerializer<TMessage>();
        }

        public static class Defaults
        {
            public static MessagePack.IFormatterResolver FormatterResolver { get; set; } = null;
        }
    }
}