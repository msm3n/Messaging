using Lykke.Messaging.Serialization;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Lykke.Messaging
{
    public static class SerializationManagerExtensions
    {
        static readonly Dictionary<Type, Func<ISerializationManager, SerializationFormat, byte[], object>> m_Deserializers =
            new Dictionary<Type, Func<ISerializationManager, SerializationFormat, byte[], object>>();
        static readonly Dictionary<Type, Func<ISerializationManager, SerializationFormat, object, byte[]>> m_Serializers =
            new Dictionary<Type, Func<ISerializationManager, SerializationFormat, object, byte[]>>();

        public static byte[] SerializeObject(this ISerializationManager manager, SerializationFormat format, object message)
        {
            if (message == null)
                return null;

            Func<ISerializationManager, SerializationFormat, object, byte[]> serialize;
            lock (m_Serializers)
            {
                var type = message.GetType();
                if (!m_Serializers.TryGetValue(type, out serialize))
                {
                    serialize = CreateSerializer(type);
                    m_Serializers.Add(type, serialize);
                }
            }
            return serialize(manager, format, message);
        }

        public static object Deserialize(this ISerializationManager manager, SerializationFormat format, byte[] message, Type type)
        {
            Func<ISerializationManager, SerializationFormat, byte[], object> deserialize;
            lock (m_Deserializers)
            {
                if (!m_Deserializers.TryGetValue(type, out deserialize))
                {
                    deserialize = CreateDeserializer(type);
                    m_Deserializers.Add(type, deserialize);
                }
            }
            return deserialize(manager, format, message);
        }

        private static Func<ISerializationManager, SerializationFormat, byte[], object> CreateDeserializer(Type type)
        {
            var format = Expression.Parameter(typeof(SerializationFormat), "format");
            var manager = Expression.Parameter(typeof(ISerializationManager), "manger");
            var message = Expression.Parameter(typeof(byte[]), "message");
            var call = Expression.Call(manager, "Deserialize", new[] { type }, format, message);
            var convert = Expression.Convert(call, typeof(object));
            var lambda = (Expression<Func<ISerializationManager, SerializationFormat, byte[], object>>)Expression.Lambda(convert, manager, format, message);
            return lambda.Compile();
        }

        private static Func<ISerializationManager, SerializationFormat, object, byte[]> CreateSerializer(Type type)
        {
            var format = Expression.Parameter(typeof(SerializationFormat), "format");
            var manager = Expression.Parameter(typeof(ISerializationManager), "manger");
            var message = Expression.Parameter(typeof(object), "message");
            var call = Expression.Call(manager, "Serialize", new[] { type }, format, Expression.Convert(message,type));
            var lambda = (Expression<Func<ISerializationManager, SerializationFormat, object, byte[]>>)Expression.Lambda(call, manager, format,message);
            return lambda.Compile();
        }
    }
}
