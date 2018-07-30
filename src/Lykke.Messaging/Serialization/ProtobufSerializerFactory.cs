using System.Linq;
using ProtoBuf;

namespace Lykke.Messaging.Serialization
{
    public class ProtobufSerializerFactory : ISerializerFactory
    {
        public SerializationFormat SerializationFormat => SerializationFormat.ProtoBuf;

        public IMessageSerializer<TMessage> Create<TMessage>()
        {
            //TODO: may affect performance
            if (
                typeof(TMessage).GetCustomAttributes(typeof(ProtoContractAttribute), false).Any()
                )
                return new ProtobufSerializer<TMessage>() ;
            return null;
        }
    }
}
