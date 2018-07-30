using Lykke.Messaging.Serialization;
using System;

namespace Lykke.Messaging
{
    public interface ISerializationManager
    {
        byte[] Serialize<TMessage>(SerializationFormat format,TMessage message);
        TMessage Deserialize<TMessage>(SerializationFormat format, byte[] message);
        void RegisterSerializer(SerializationFormat format, Type targetType, object serializer);
        void RegisterSerializerFactory(ISerializerFactory serializerFactory);
    }
}
