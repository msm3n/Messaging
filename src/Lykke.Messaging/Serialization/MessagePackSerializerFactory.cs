namespace Lykke.Messaging.Serialization
{
    public class MessagePackSerializerFactory : ISerializerFactory
    {
        public string SerializationFormat => "messagepack";

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