using System;
using System.Runtime.Serialization;

namespace Lykke.Messaging
{
    public class InvalidSubscriptionException : InvalidOperationException
    {
        public InvalidSubscriptionException()
        {
        }

        public InvalidSubscriptionException(string message) : base(message)
        {
        }

        public InvalidSubscriptionException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected InvalidSubscriptionException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}