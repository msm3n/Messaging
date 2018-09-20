using System;

namespace Lykke.Messaging.Serialization
{
    internal class MessagePackSerializer<TMessage> : IMessageSerializer<TMessage>
    {
        private readonly ProtobufSerializer<TMessage> _protobufSerializer;

        public MessagePackSerializer()
        {
            _protobufSerializer = new ProtobufSerializer<TMessage>();
        }

        public byte[] Serialize(TMessage message)
        {
            return MessagePack.MessagePackSerializer.Serialize(message, MessagePackSerializerFactory.Defaults.FormatterResolver);
        }

        public TMessage Deserialize(byte[] message)
        {
            try
            {
                return MessagePack.MessagePackSerializer.Deserialize<TMessage>(message, MessagePackSerializerFactory.Defaults.FormatterResolver);
            }
            catch (Exception)
            {
                return _protobufSerializer.Deserialize(message);
            }
        }
    }
}