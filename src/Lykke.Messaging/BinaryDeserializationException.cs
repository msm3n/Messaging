using System;

namespace Lykke.Messaging
{
    public class BinaryDeserializationException : InvalidOperationException
    {
        public BinaryDeserializationException(string message) : base(message)
        {
        }
    }
}
