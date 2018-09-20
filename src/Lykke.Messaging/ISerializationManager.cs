using System;

namespace Lykke.Messaging
{
    public interface ISerializationManager
    {
        byte[] Serialize<TMessage>(string format,TMessage message);
        TMessage Deserialize<TMessage>(string format, byte[] message);
        void RegisterSerializer(string format, Type targetType, object serializer);
        void RegisterSerializerFactory(ISerializerFactory serializerFactory);
    }
}
