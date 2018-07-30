using Lykke.Messaging.Serialization;

namespace Lykke.Messaging
{
    public interface ISerializerFactory
    {
        SerializationFormat SerializationFormat { get; }
        IMessageSerializer<TMessage> Create<TMessage>();
    }
}
