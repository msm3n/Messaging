using System;
using Common.Log;
using Lykke.Common.Log;

namespace Lykke.Messaging.Serialization
{
    internal class MessagePackSerializer<TMessage> : IMessageSerializer<TMessage>
    {
        private readonly ResilientBinarySerializer<TMessage> _serializer;

        [Obsolete("Please, use the overload which consumes ILogFactory")]
        public MessagePackSerializer(ILog log)
        {
            _serializer = new ResilientBinarySerializer<TMessage>(log, SerializationFormat.MessagePack);
        }

        public MessagePackSerializer(ILogFactory logFactory)
        {
            _serializer = new ResilientBinarySerializer<TMessage>(logFactory, SerializationFormat.MessagePack);
        }

        public byte[] Serialize(TMessage message)
        {
            return _serializer.Serialize(message);
        }

        public TMessage Deserialize(byte[] message)
        {
            return _serializer.Deserialize(message);
        }
    }
}