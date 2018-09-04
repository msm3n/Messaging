using System;
using Common.Log;
using Lykke.Common.Log;

namespace Lykke.Messaging.Serialization
{
    internal class ProtobufSerializer<TMessage> : IMessageSerializer<TMessage>
    {
        private readonly ResilientBinarySerializer<TMessage> _serializer;

        [Obsolete("Please, use the overload which consumes ILogFactory")]
        public ProtobufSerializer(ILog log)
        {
            _serializer = new ResilientBinarySerializer<TMessage>(log, SerializationFormat.ProtoBuf);
        }

        public ProtobufSerializer(ILogFactory logFactory)
        {
            _serializer = new ResilientBinarySerializer<TMessage>(logFactory, SerializationFormat.ProtoBuf);
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
