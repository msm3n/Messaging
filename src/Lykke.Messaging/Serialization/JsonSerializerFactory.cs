namespace Lykke.Messaging.Serialization
{
    public class JsonSerializerFactory : ISerializerFactory
    {
        public SerializationFormat SerializationFormat => SerializationFormat.Json;

        public IMessageSerializer<TMessage> Create<TMessage>()
        {
            return new JsonSerializer<TMessage>();
        }
    }
}