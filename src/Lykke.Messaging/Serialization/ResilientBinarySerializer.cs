﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Extensions.Logging;
using ProtoBuf;

namespace Lykke.Messaging.Serialization
{
    /// <summary>
    /// NOT FOR PUBLIC USE. Serializer for:
    /// - MessagePack;
    /// - Protobuf;
    /// - Anything else what will be added in future.
    /// </summary>
    internal class ResilientBinarySerializer<TMessage> : IMessageSerializer<TMessage>
    {
        #region Fields

        private static readonly ILogger<ResilientBinarySerializer<TMessage>> _logger = Log.For<ResilientBinarySerializer<TMessage>>();

        private readonly List<SerializationFormat> _formatPriorityList = new List<SerializationFormat>
        {
            SerializationFormat.MessagePack,
            SerializationFormat.ProtoBuf
            // TODO: add each newly-enabled message format here.
        };

        #endregion

        #region Props

        public SerializationFormat NativeFormat { get; private set; }

        #endregion

        #region Construction

        public ResilientBinarySerializer(SerializationFormat nativeFormat)
        {
            ApplyNativeSerializationFormat(nativeFormat);
        }

        private void ApplyNativeSerializationFormat(SerializationFormat nativeFormat)
        {
            if (!_formatPriorityList.Contains(nativeFormat))
                throw new ArgumentException($"The required serialization format {nativeFormat} is not currently supported.");
            NativeFormat = nativeFormat;

            // We'll start deserialization attempts from the native format.
            _formatPriorityList.Remove(NativeFormat);
            _formatPriorityList.Insert(0, NativeFormat);
        }

        #endregion

        #region Public::IMessageSerializer

        public byte[] Serialize(TMessage message)
        {
            switch (NativeFormat)
            {
                case SerializationFormat.MessagePack:
                    return MessagePack.MessagePackSerializer.Serialize(message, MessagePackSerializerFactory.Defaults.FormatterResolver);

                case SerializationFormat.ProtoBuf:
                    using (var memStream = new MemoryStream())
                    {
                        Serializer.Serialize(memStream, message);
                        return memStream.ToArray();
                    }

                // TODO: add each newly-enabled message format handler here.

                // This can not be due to parameter check in constructors. Though, the compiler does not know about it.
                default:
                    throw new InvalidOperationException($"Unsupported serialization format: {NativeFormat}");
            }
        }

        public TMessage Deserialize(byte[] message)
        {
            // We do not use the native format explicitly here. Instead, we determine which format was successfull in
            // the last attempt and try to use it first. If it now fails, we loop through other supported formats.

            var lastCompatibleFormat = _formatPriorityList.First();
            TMessage result = default(TMessage);

            foreach (var fmt in _formatPriorityList)
            {
                try
                {
                    switch (fmt)
                    {
                        case SerializationFormat.MessagePack:
                            result = MessagePack.MessagePackSerializer.Deserialize<TMessage>(message, MessagePackSerializerFactory.Defaults.FormatterResolver);
                            break;

                        case SerializationFormat.ProtoBuf:
                            using (var memStream = new MemoryStream())
                            {
                                memStream.Write(message, 0, message.Length);
                                memStream.Seek(0, SeekOrigin.Begin);
                                result = Serializer.Deserialize<TMessage>(memStream);
                            }
                            break;

                        // TODO: add each newly-enabled message format handler here.

                        // This should not ever happen. But who knows.
                        default:
                            throw new BinaryDeserializationException($"Attempt to deserialize a message with unsupported formatter: {fmt}.");
                    }

                    lastCompatibleFormat = fmt;

                    break;
                }
                catch (BinaryDeserializationException)
                {
                    throw;
                }
                catch (Exception ex) when (fmt != _formatPriorityList.Last())
                {
                    _logger.LogWarning(
                        ex,
                        "Unable to deserialize the message using {Format} formatter. Will try other(s).",
                        fmt);
                }
                // Otherwise, we give up and propagate the exception higher.
            }

            if (lastCompatibleFormat != _formatPriorityList.First())
            {
                // Will start just from the last successfull format next time.
                _formatPriorityList.Remove(lastCompatibleFormat);
                _formatPriorityList.Insert(0, lastCompatibleFormat);
            }

            return result;
        }

        #endregion
    }
}
