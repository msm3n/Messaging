using System;
using System.Linq;
using Common.Log;
using Lykke.Common.Log;
using ProtoBuf;

namespace Lykke.Messaging.Serialization
{
    public class ProtobufSerializerFactory : ISerializerFactory
    {
        public SerializationFormat SerializationFormat => SerializationFormat.ProtoBuf;


        public IMessageSerializer<TMessage> Create<TMessage>()
        {
            return new ProtobufSerializer<TMessage>();
        }
    }
}
