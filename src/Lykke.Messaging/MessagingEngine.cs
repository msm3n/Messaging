﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Threading;
using JetBrains.Annotations;
using Microsoft.Extensions.Logging;
using Lykke.Core.Utils;
using Lykke.Messaging.Contract;
using Lykke.Messaging.Serialization;
using Lykke.Messaging.Transports;

namespace Lykke.Messaging
{
    [PublicAPI]
    public class MessagingEngine : IMessagingEngine, IDisposable
    {
        private const int DEFAULT_UNACK_DELAY = 60000;
        private const int MESSAGE_DEFAULT_LIFESPAN = 0; // forever // 1800000; // milliseconds (30 minutes)

        private static readonly ILogger<MessagingEngine> _logger = Log.For<MessagingEngine>();

        private readonly ManualResetEvent m_Disposing = new ManualResetEvent(false);
        private readonly CountingTracker m_RequestsTracker = new CountingTracker();
        private readonly ISerializationManager m_SerializationManager;
        private readonly List<IDisposable> m_MessagingHandles = new List<IDisposable>();
        private readonly TransportManager m_TransportManager;
        private readonly ConcurrentDictionary<Type, string> m_MessageTypeMapping = new ConcurrentDictionary<Type, string>();
        private readonly SchedulingBackgroundWorker m_RequestTimeoutManager;
        private readonly Dictionary<RequestHandle, Action<Exception>> m_ActualRequests = new Dictionary<RequestHandle, Action<Exception>>();
        private readonly ProcessingGroupManager m_ProcessingGroupManager;

        public MessagingEngine(
            ITransportResolver transportResolver,
            IDictionary<string, ProcessingGroupInfo> processingGroups = null,
            params ITransportFactory[] transportFactories)
        {
            if (transportResolver == null)
                throw new ArgumentNullException(nameof(transportResolver));
            m_TransportManager = new TransportManager(transportResolver, transportFactories);
            m_ProcessingGroupManager = new ProcessingGroupManager(m_TransportManager, processingGroups);
            m_SerializationManager = new SerializationManager();
            m_RequestTimeoutManager = new SchedulingBackgroundWorker("RequestTimeoutManager", () => StopTimeoutedRequests());
            CreateMessagingHandle(() => StopTimeoutedRequests(true));
        }


        public MessagingEngine(
            ITransportResolver transportResolver,
            params ITransportFactory[] transportFactories)
            : this(transportResolver, null, transportFactories)
        {
        }

        public int ResubscriptionTimeout
        {
            get => m_ProcessingGroupManager.ResubscriptionTimeout;
            set => m_ProcessingGroupManager.ResubscriptionTimeout = value;
        }

        public void AddProcessingGroup(string name, ProcessingGroupInfo info)
        {
            m_ProcessingGroupManager.AddProcessingGroup(name, info);
        }

        public bool GetProcessingGroupInfo(string name, out ProcessingGroupInfo groupInfo)
        {
            return m_ProcessingGroupManager.GetProcessingGroupInfo(name, out groupInfo);
        }

        public string GetStatistics()
        {
            return m_ProcessingGroupManager.GetStatistics();
        }

        internal TransportManager TransportManager => m_TransportManager;

        public ISerializationManager SerializationManager => m_SerializationManager;

        #region IMessagingEngine Members

        public bool VerifyEndpoint(
            Endpoint endpoint,
            EndpointUsage usage,
            bool configureIfRequired,
            out string error)
        {
            return m_TransportManager.VerifyDestination(
                endpoint.TransportId,
                endpoint.Destination,
                usage,
                configureIfRequired,
                out error);
        }

        public Dictionary<Endpoint, string> VerifyEndpoints(
            EndpointUsage usage,
            IEnumerable<Endpoint> endpoints,
            bool configureIfRequired)
        {
            var result = new Dictionary<Endpoint, string>();

            var byTransport = endpoints.GroupBy(e => e.TransportId);

            foreach (var transportEndpoints in byTransport)
            {
                var transportEndpointsErrors = m_TransportManager.VerifyDestinations(
                    transportEndpoints.Key,
                    transportEndpoints,
                    usage,
                    configureIfRequired);
                foreach (var transportVerification in transportEndpointsErrors)
                {
                    result[transportVerification.Key] = transportVerification.Value;
                }
            }

            return result;
        }

        public Destination CreateTemporaryDestination(string transportId, string processingGroup)
        {
            return m_TransportManager
                .GetMessagingSession(new Endpoint { TransportId = transportId }, processingGroup ?? "default")
                .CreateTemporaryDestination();
        }

        public IDisposable SubscribeOnTransportEvents(TransportEventHandler handler)
        {
            TransportEventHandler safeHandler = (transportId, @event) =>
                {
                    try
                    {
                        handler(transportId, @event);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Transport events handler failed");
                    }
                };
            m_TransportManager.TransportEvents += safeHandler;
            return Disposable.Create(() => m_TransportManager.TransportEvents -= safeHandler);
        }

        public void Send<TMessage>(
            TMessage message,
            Endpoint endpoint,
            string processingGroup = null,
            Dictionary<string, string> headers = null)
        {
            Send(
                message,
                endpoint,
                MESSAGE_DEFAULT_LIFESPAN,
                processingGroup, headers);
        }

        private static string GetProcessingGroup(Endpoint endpoint, string processingGroup)
        {
            //by default on processing group per destination
            return processingGroup ?? endpoint.Destination.ToString();
        }

        public void Send<TMessage>(
            TMessage message,
            Endpoint endpoint,
            int ttl,
            string processingGroup = null,
            Dictionary<string, string> headers = null)
        {
            var serializedMessage = SerializeMessage(endpoint.SerializationFormat, message);
            if (headers != null)
            {
                foreach (var header in headers)
                {
                    serializedMessage.Headers[header.Key] = header.Value;
                }
            }
            Send(
                serializedMessage,
                endpoint,
                ttl,
                processingGroup);
        }

        public void Send(
            object message,
            Endpoint endpoint,
            string processingGroup = null,
            Dictionary<string, string> headers = null)
        {
            var type = GetMessageType(message.GetType());
            var bytes = m_SerializationManager.SerializeObject(endpoint.SerializationFormat, message);
            var serializedMessage = new BinaryMessage
            {
                Bytes = bytes,
                Type = type,
            };
            if (headers != null)
            {
                foreach (var header in headers)
                {
                    serializedMessage.Headers[header.Key] = header.Value;
                }
            }
            Send(
                serializedMessage,
                endpoint,
                MESSAGE_DEFAULT_LIFESPAN,
                processingGroup);
        }

        private void Send(
            BinaryMessage message,
            Endpoint endpoint,
            int ttl,
            string processingGroup)
        {
            if (endpoint.Destination == null) throw new ArgumentException("Destination can not be null");
            if (m_Disposing.WaitOne(0))
                throw new InvalidOperationException("Engine is disposing");

            using (m_RequestsTracker.Track())
            {
                try
                {
                    m_ProcessingGroupManager.Send(
                        endpoint,
                        message,
                        ttl,
                        GetProcessingGroup(endpoint, processingGroup));
                }
                catch (Exception e)
                {
                    _logger.LogError(e, "Failed to send message. Transport: {TransportId}, Queue: {Destination}", endpoint.TransportId, endpoint.Destination);
                    throw;
                }
            }
        }

        public IDisposable Subscribe<TMessage>(Endpoint endpoint, Action<TMessage> callback)
        {
            return Subscribe(
                endpoint,
                (TMessage message, AcknowledgeDelegate acknowledge, Dictionary<string, string> headers) =>
                    {
                        callback(message);
                        acknowledge(0, true);
                    });
        }

        public IDisposable Subscribe<TMessage>(
            Endpoint endpoint,
            CallbackDelegate<TMessage> callback,
            string processingGroup = null,
            int priority = 0)
        {
            if (endpoint.Destination == null) throw new ArgumentException("Destination can not be null");
            if (m_Disposing.WaitOne(0))
                throw new InvalidOperationException("Engine is disposing");

            using (m_RequestsTracker.Track())
            {
                try
                {
                    return Subscribe(
                        endpoint,
                        (m, ack) =>
                            ProcessMessage(
                                m,
                                typeof(TMessage),
                                (message, headers) => callback((TMessage)message, ack, headers),
                                ack,
                                endpoint),
                        endpoint.SharedDestination ? GetMessageType(typeof(TMessage)) : null,
                        processingGroup,
                        priority);
                }
                catch (Exception e)
                {
                    _logger.LogError(e, "Failed to subscribe. Transport: {TransportId}, Queue: {Destination}", endpoint.TransportId, endpoint.Destination);
                    throw;
                }
            }
        }

        public IDisposable Subscribe(
            Endpoint endpoint,
            Action<object> callback,
            Action<string> unknownTypeCallback,
            params Type[] knownTypes)
        {
            return Subscribe(
                endpoint,
                callback,
                unknownTypeCallback,
                null,
                0,
                knownTypes);
        }

        public IDisposable Subscribe(
            Endpoint endpoint,
            Action<object> callback,
            Action<string> unknownTypeCallback,
            string processingGroup,
            int priority,
            params Type[] knownTypes)
        {
            return Subscribe(
                endpoint,
                (message, acknowledge, headers) =>
                    {
                        callback(message);
                        acknowledge(0, true);
                    },
                (type, acknowledge) =>
                    {
                        unknownTypeCallback(type);
                        acknowledge(0, true);
                    },
                processingGroup,
                priority,
                knownTypes);
        }

        public IDisposable Subscribe(
            Endpoint endpoint,
            CallbackDelegate<object> callback,
            Action<string, AcknowledgeDelegate> unknownTypeCallback,
            params Type[] knownTypes)
        {
            return Subscribe(endpoint, callback, unknownTypeCallback, null, 0, knownTypes);
        }

        public IDisposable Subscribe(
            Endpoint endpoint,
            CallbackDelegate<object> callback,
            Action<string, AcknowledgeDelegate> unknownTypeCallback,
            string processingGroup,
            int priority = 0,
            params Type[] knownTypes)
        {
            if (endpoint.Destination == null)
                throw new ArgumentException("Destination can not be null");
            if (m_Disposing.WaitOne(0))
                throw new InvalidOperationException("Engine is disposing");

            using (m_RequestsTracker.Track())
            {
                try
                {
                    var dictionary = knownTypes.ToDictionary(GetMessageType);

                    return Subscribe(
                        endpoint,
                        (m, ack) =>
                            {
                                if (!dictionary.TryGetValue(m.Type ?? "", out var messageType))
                                {
                                    try
                                    {
                                        unknownTypeCallback(m.Type, ack);
                                    }
                                    catch (Exception e)
                                    {
                                        _logger.LogError(
                                            e,
                                            "Failed to handle message of unknown type. Transport: {TransportId}, Queue {Destination}, Message Type: {MessageType}",
                                            endpoint.TransportId, endpoint.Destination, m.Type);
                                    }
                                    return;
                                }
                                ProcessMessage(
                                    m,
                                    messageType,
                                    (message, headers) => callback(message, ack, headers),
                                    ack,
                                    endpoint);
                            },
                        null,
                        processingGroup,
                        priority);
                }
                catch (Exception e)
                {
                    _logger.LogError(e, "Failed to subscribe. Transport: {TransportId}, Queue: {Destination}", endpoint.TransportId, endpoint.Destination);
                    throw;
                }
            }
        }

        //NOTE: send via topic waits only first response.
        public TResponse SendRequest<TRequest, TResponse>(TRequest request, Endpoint endpoint, long timeout)
        {
            if (m_Disposing.WaitOne(0))
                throw new InvalidOperationException("Engine is disposing");

            using (m_RequestsTracker.Track())
            {
                var responseReceived = new ManualResetEvent(false);
                TResponse response = default(TResponse);
                Exception exception = null;

                using (SendRequestAsync<TRequest, TResponse>(
                    request,
                    endpoint,
                    r =>
                        {
                            response = r;
                            responseReceived.Set();
                        },
                    ex =>
                        {
                            exception = ex;
                            responseReceived.Set();
                        },
                    timeout))
                {
                    int waitResult = WaitHandle.WaitAny(new WaitHandle[] { m_Disposing, responseReceived });
                    switch (waitResult)
                    {
                        case 1:
                            if (exception == null)
                                return response;
                            if (exception is TimeoutException)
                                throw exception;//StackTrace is replaced bat it is ok here.
                            throw new ProcessingException("Failed to process response", exception);
                        case 0:
                            throw new ProcessingException("Request was canceled due to engine dispose", exception);

                        default:
                            throw new InvalidOperationException();
                    }
                }
            }
        }

        private void StopTimeoutedRequests(bool stopAll = false)
        {
            lock (m_ActualRequests)
            {
                var timeouted = stopAll
                    ? m_ActualRequests.ToArray()
                    : m_ActualRequests.Where(r => r.Key.DueDate <= DateTime.UtcNow || r.Key.IsComplete).ToArray();

                Array.ForEach(timeouted, r =>
                {
                    r.Key.Dispose();
                    if (!r.Key.IsComplete)
                    {
                        r.Value(new TimeoutException("Request has timed out"));
                    }
                    m_ActualRequests.Remove(r.Key);
                });
            }
        }

        public IDisposable SendRequestAsync<TRequest, TResponse>(
            TRequest request,
            Endpoint endpoint,
            Action<TResponse> callback,
            Action<Exception> onFailure,
            long timeout,
            string processingGroup = null)
        {
            if (m_Disposing.WaitOne(0))
                throw new InvalidOperationException("Engine is disposing");

            using (m_RequestsTracker.Track())
            {
                try
                {
                    var session = m_TransportManager.GetMessagingSession(endpoint, GetProcessingGroup(endpoint, processingGroup));
                    RequestHandle requestHandle = session.SendRequest(
                        endpoint.Destination.Publish,
                        SerializeMessage(endpoint.SerializationFormat, request),
                        message =>
                        {
                            try
                            {
                                var responseMessage = m_SerializationManager.Deserialize<TResponse>(endpoint.SerializationFormat, message.Bytes);
                                callback(responseMessage);
                            }
                            catch (Exception e)
                            {
                                onFailure(e);
                            }
                            finally
                            {
                                m_RequestTimeoutManager.Schedule(1);
                            }
                        });

                    lock (m_ActualRequests)
                    {
                        requestHandle.DueDate = DateTime.UtcNow.AddMilliseconds(timeout);
                        m_ActualRequests.Add(requestHandle, onFailure);
                        m_RequestTimeoutManager.Schedule(timeout);
                    }
                    return requestHandle;
                }
                catch (Exception e)
                {
                    _logger.LogError(e, "Failed to register handler. Transport: {TransportId}, Destination: {Destination}", endpoint.TransportId, endpoint.Destination);
                    throw;
                }
            }
        }

        public IDisposable RegisterHandler<TRequest, TResponse>(Func<TRequest, TResponse> handler, Endpoint endpoint)
            where TResponse : class
        {
            var handle = new SerialDisposable();
            IDisposable transportWatcher = SubscribeOnTransportEvents(
                (transportId, @event) =>
                {
                    if (transportId == endpoint.TransportId || @event != TransportEvents.Failure)
                        return;
                    RegisterHandlerWithRetry(handler, endpoint, handle);
                });

            RegisterHandlerWithRetry(handler, endpoint, handle);

            return new CompositeDisposable(transportWatcher, handle);
        }

        public void Dispose()
        {
            m_Disposing.Set();
            m_RequestsTracker.WaitAll();
            lock (m_MessagingHandles)
            {
                while (m_MessagingHandles.Any())
                {
                    m_MessagingHandles.First().Dispose();
                }
            }
            m_RequestTimeoutManager.Dispose();
            m_ProcessingGroupManager.Dispose();
            m_TransportManager.Dispose();
        }

        #endregion

        private void RegisterHandlerWithRetry<TRequest, TResponse>(Func<TRequest, TResponse> handler, Endpoint endpoint, SerialDisposable handle)
            where TResponse : class
        {
            lock (handle)
            {
                try
                {
                    handle.Disposable = RegisterHandler(handler, endpoint);
                }
                catch
                {
                    _logger.LogInformation(
                        "Scheduling register handler attempt in 1 minute. Transport: {TransportId}, Queue: {Destination}",
                        endpoint.TransportId, endpoint.Destination);

                    handle.Disposable = Scheduler.Default.Schedule(
                        DateTimeOffset.Now.AddMinutes(1),
                        () =>
                        {
                            lock (handle)
                            {
                                RegisterHandlerWithRetry(handler, endpoint, handle);
                            }
                        });
                }
            }
        }

        private IDisposable RegisterHandler<TRequest, TResponse>(Func<TRequest, TResponse> handler, Endpoint endpoint, string processingGroup = null)
            where TResponse : class
        {
            //BUG: registering handler while disposing causes endless loop
            if (m_Disposing.WaitOne(0))
                throw new InvalidOperationException("Engine is disposing");

            using (m_RequestsTracker.Track())
            {
                try
                {
                    var session = m_TransportManager.GetMessagingSession(endpoint, GetProcessingGroup(endpoint, processingGroup));
                    var subscription = session.RegisterHandler(
                        endpoint.Destination.Subscribe,
                        requestMessage =>
                        {
                            var message = m_SerializationManager.Deserialize<TRequest>(endpoint.SerializationFormat, requestMessage.Bytes);
                            TResponse response = handler(message);
                            return SerializeMessage(endpoint.SerializationFormat, response);
                        },
                        endpoint.SharedDestination ? GetMessageType(typeof(TRequest)) : null
                        );
                    var messagingHandle = CreateMessagingHandle(() =>
                        {
                            try
                            {
                                subscription.Dispose();
                                Disposable.Create(() => _logger.LogInformation(
                                    "Handler was unregistered. Transport: {TransportId}, Queue: {Destination}",
                                    endpoint.TransportId, endpoint.Destination));
                            }
                            catch (Exception e)
                            {
                                _logger.LogError(
                                    e,
                                    "Failed to unregister handler. Transport: {TransportId}, Queue: {Destination}",
                                    endpoint.TransportId, endpoint.Destination);
                            }
                        });

                    _logger.LogInformation(
                        "Handler was successfully registered. Transport: {TransportId}, Queue: {Destination}",
                        endpoint.TransportId, endpoint.Destination);

                    return messagingHandle;
                }
                catch (Exception e)
                {
                    _logger.LogError(
                        e,
                        "Failed to register handler. Transport: {TransportId}, Queue: {Destination}",
                        endpoint.TransportId, endpoint.Destination);
                    throw;
                }
            }
        }

        private BinaryMessage SerializeMessage<TMessage>(SerializationFormat format, TMessage message)
        {
            var type = GetMessageType(typeof(TMessage));
            var bytes = m_SerializationManager.Serialize(format, message);
            return new BinaryMessage { Bytes = bytes, Type = type };
        }

        private string GetMessageType(Type type)
        {
            return m_MessageTypeMapping.GetOrAdd(
                type,
                clrType =>
                    {
                        //TODO: type should be determined by serializer
                        var typeName = clrType.GetCustomAttributes(false)
                            .Select(a => a as ProtoBuf.ProtoContractAttribute)
                            .Where(a => a != null)
                            .Select(a => a.Name)
                            .FirstOrDefault();
                        return typeName ?? clrType.Name;
                    });
        }

        private IDisposable Subscribe(
            Endpoint endpoint,
            Action<BinaryMessage, AcknowledgeDelegate> callback,
            string messageType,
            string processingGroup,
            int priority)
        {
            var subscription = m_ProcessingGroupManager.Subscribe(
                endpoint,
                callback,
                messageType,
                GetProcessingGroup(endpoint, processingGroup),
                priority);

            return CreateMessagingHandle(() =>
            {
                subscription.Dispose();
                _logger.LogInformation(
                    "Unsubscribed from endpoint {Endpoint}",
                    endpoint);
            });
        }

        private IDisposable CreateMessagingHandle(Action destroy)
        {
            IDisposable handle = null;

            handle = Disposable.Create(() =>
                {
                    destroy();
                    lock (m_MessagingHandles)
                    {
                        // ReSharper disable AccessToModifiedClosure
                        m_MessagingHandles.Remove(handle);
                        // ReSharper restore AccessToModifiedClosure
                    }
                });
            lock (m_MessagingHandles)
            {
                m_MessagingHandles.Add(handle);
            }
            return handle;
        }

        private void ProcessMessage(
            BinaryMessage binaryMessage,
            Type type,
            Action<object, Dictionary<string, string>> callback,
            AcknowledgeDelegate ack,
            Endpoint endpoint)
        {
            object message = null;
            try
            {
                message = m_SerializationManager.Deserialize(endpoint.SerializationFormat, binaryMessage.Bytes, type);
            }
            catch (Exception e)
            {
                _logger.LogError(
                    e,
                    "Failed to deserialize message. Transport: {TransportId}, Destination: {Destination}, Message Type: {MessageType}",
                    endpoint.TransportId, endpoint.Destination, type.Name);

                ack(DEFAULT_UNACK_DELAY, false);

                return;
            }

            try
            {
                callback(message, binaryMessage.Headers);
            }
            catch (Exception e)
            {
                _logger.LogError(
                    e,
                    "Failed to handle message. Transport: {TransportId}, Destination: {Destination}, Message Type: {MessageType}",
                    endpoint.TransportId, endpoint.Destination, type.Name);

                ack(DEFAULT_UNACK_DELAY, false);
            }
        }
    }
}
