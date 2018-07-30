using Common.Log;
using Lykke.Common.Log;
using System;

namespace Lykke.Messaging.Serialization
{
    internal class MessagePackSerializer<TMessage> : IMessageSerializer<TMessage>
    {
        private readonly ProtobufSerializer<TMessage> _protobufSerializer;
        private readonly ILog _log;
        private readonly ILog _newLog;

        private bool _fallbackDeserializerWorks = false;

        [Obsolete]
        public MessagePackSerializer(ILog log)
        {
            _protobufSerializer = new ProtobufSerializer<TMessage>();
            _log = log ?? throw new ArgumentNullException(nameof(log));
        }

        public MessagePackSerializer(ILogFactory logFactory)
        {
            _protobufSerializer = new ProtobufSerializer<TMessage>();
            _newLog = logFactory.CreateLog(this);
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
            catch (Exception ex)
            {
                if (_fallbackDeserializerWorks)
                {
                    if (_newLog != null)
                        _newLog.Warning("MessagePack deserializer failed, using ProtoBuf", ex);
                    else
                        _log.WriteWarning(nameof(Deserialize), message, "MessagePack deserializer failed, using ProtoBuf");
                }
                else
                {
                    if (_newLog != null)
                        _newLog.Error(ex);
                    else
                        _log.WriteError(nameof(Deserialize), message, ex);
                }
                try
                {
                    var result = _protobufSerializer.Deserialize(message);
                    if (!_fallbackDeserializerWorks)
                        _fallbackDeserializerWorks = true;
                    return result;
                }
                catch (Exception)
                {
                    throw ex;
                }
            }
        }
    }
}