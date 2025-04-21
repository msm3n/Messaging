﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Disposables;
using System.Text;
using Microsoft.Extensions.Logging;
using Lykke.Core.Utils;
using Lykke.Messaging.Contract;
using Lykke.Messaging.Transports;
using Lykke.Messaging.Utils;

namespace Lykke.Messaging
{
    internal class ProcessingGroupManager : IDisposable
    {
        private readonly ITransportManager m_TransportManager;
        private static readonly ILogger<ProcessingGroupManager> _logger = Log.For<ProcessingGroupManager>();

        private readonly List<Tuple<DateTime, Action>> m_DeferredAcknowledgements = new List<Tuple<DateTime, Action>>();
        private readonly List<Tuple<DateTime, Action>> m_ResubscriptionSchedule = new List<Tuple<DateTime, Action>>();
        private readonly SchedulingBackgroundWorker m_DeferredAcknowledger;
        private readonly SchedulingBackgroundWorker m_Resubscriber;
        private readonly ConcurrentDictionary<string, ProcessingGroup> m_ProcessingGroups = new ConcurrentDictionary<string, ProcessingGroup>();
        private readonly ConcurrentDictionary<string, ProcessingGroupInfo> m_ProcessingGroupInfos;

        private volatile bool m_IsDisposing;

        public int ResubscriptionTimeout { get; set; }

        public ProcessingGroupManager(
            ITransportManager transportManager,
            IDictionary<string, ProcessingGroupInfo> processingGroups = null,
            int resubscriptionTimeout = 60000)
        {
            m_ProcessingGroupInfos = new ConcurrentDictionary<string, ProcessingGroupInfo>(processingGroups ?? new Dictionary<string, ProcessingGroupInfo>());
            m_TransportManager = transportManager;
            ResubscriptionTimeout = resubscriptionTimeout;
            m_DeferredAcknowledger = new SchedulingBackgroundWorker("DeferredAcknowledgement", () => ProcessDeferredAcknowledgements());
            m_Resubscriber = new SchedulingBackgroundWorker("Resubscription", () => ProcessResubscription());
        }

        public void AddProcessingGroup(string name, ProcessingGroupInfo info)
        {
            if (!m_ProcessingGroupInfos.TryAdd(name, info))
                throw new InvalidOperationException($"Can not add processing group '{name}'. It already exists.");
        }

        public bool GetProcessingGroupInfo(string name, out ProcessingGroupInfo groupInfo)
        {
            if (m_ProcessingGroupInfos.TryGetValue(name, out var info))
            {
                groupInfo = new ProcessingGroupInfo(info);
                return true;
            }

            groupInfo = null;
            return false;
        }

        public IDisposable Subscribe(
            Endpoint endpoint,
            Action<BinaryMessage, AcknowledgeDelegate> callback,
            string messageType,
            string processingGroup,
            int priority)
        {
            if (string.IsNullOrEmpty(processingGroup))
                throw new ArgumentNullException(nameof(processingGroup), "should be not empty string");
            if (m_IsDisposing)
                throw new ObjectDisposedException(GetType().Name);

            var subscriptionHandler = new MultipleAssignmentDisposable();
            Action<int> doSubscribe = null;
            doSubscribe = attemptNumber =>
            {
                string processingGroupName = null;
                if (subscriptionHandler.IsDisposed)
                    return;

                try
                {
                    var group = GetProcessingGroup(processingGroup);
                    processingGroupName = group.Name;
                    _logger.LogInformation(
                        "Subscribing for endpoint {Endpoint} within processing group '{Group}'. Attempt#{Attempt}",
                        endpoint, processingGroupName, attemptNumber);

                    var sessionName = GetSessionName(group, priority);

                    var session = m_TransportManager.GetMessagingSession(endpoint, sessionName, Helper.CallOnlyOnce(() =>
                    {
                        _logger.LogInformation(
                            "Subscription for endpoint {Endpoint} within processing group '{Group}' failure detected. Attempting subscribe again.",
                            endpoint, processingGroupName);
                        doSubscribe(0);
                    }));

                    var subscription = group.Subscribe(
                        session,
                        endpoint.Destination.Subscribe,
                        (message, ack) => callback(message, CreateDeferredAcknowledge(ack)),
                        messageType,
                        priority);
                    var brokenSubscription = subscriptionHandler.Disposable;
                    subscriptionHandler.Disposable = subscription;
                    try
                    {
                        if (attemptNumber > 0)
                            brokenSubscription.Dispose();
                    }
                    catch
                    {
                    }
                    _logger.LogInformation(
                        "Subscribed for endpoint {Endpoint} in processingGroup '{Group}' using session {Session}",
                        endpoint, processingGroupName, sessionName);
                }
                catch (InvalidSubscriptionException e)
                {
                    _logger.LogError(
                        e,
                        "Failed to subscribe for endpoint {Endpoint} within processing group '{Group}'",
                        endpoint, processingGroupName);
                    throw;
                }
                catch (Exception e)
                {
                    _logger.LogError(
                        e,
                        "Error subscribing endpoint {Endpoint} in group '{Group}'. Attempt#{Attempt}, retry in {Timeout}ms",
                        endpoint, processingGroupName, attemptNumber, ResubscriptionTimeout);

                    ScheduleSubscription(doSubscribe, attemptNumber + 1);
                }
            };
            doSubscribe(0);
            return subscriptionHandler;
        }

        private string GetSessionName(ProcessingGroup processingGroup, int priority)
        {
            if (processingGroup.ConcurrencyLevel == 0)
                return processingGroup.Name;
            return $"{processingGroup.Name} priority{priority}";
        }

        private ProcessingGroup GetProcessingGroup(string processingGroup)
        {
            if (m_ProcessingGroups.TryGetValue(processingGroup, out var group))
                return group;

            var info = m_ProcessingGroupInfos.GetOrAdd(processingGroup, new ProcessingGroupInfo());
            group = m_ProcessingGroups.GetOrAdd(processingGroup, new ProcessingGroup(processingGroup, info));

            return group;
        }

        public void Send(Endpoint endpoint, BinaryMessage message, int ttl, string processingGroup)
        {
            var group = GetProcessingGroup(processingGroup);
            var session = m_TransportManager.GetMessagingSession(endpoint, GetSessionName(group, 0));

            group.Send(session, endpoint.Destination.Publish, message, ttl);
        }

        private void ScheduleSubscription(Action<int> subscribe, int attemptCount)
        {
            lock (m_ResubscriptionSchedule)
            {
                m_ResubscriptionSchedule.Add(
                    Tuple.Create<DateTime, Action>(
                        DateTime.UtcNow.AddMilliseconds(attemptCount == 0 ? 100 : ResubscriptionTimeout), () => subscribe(attemptCount)));
                m_Resubscriber.Schedule(ResubscriptionTimeout);
            }
        }

        private void ProcessDeferredAcknowledgements(bool all = false)
        {
            Tuple<DateTime, Action>[] ready;
            var succeeded = new List<Tuple<DateTime, Action>>();
            lock (m_DeferredAcknowledgements)
            {
                ready = all
                    ? m_DeferredAcknowledgements.ToArray()
                    : m_DeferredAcknowledgements.Where(r => r.Item1 <= DateTime.UtcNow).ToArray();
            }

            foreach (var t in ready)
            {
                try
                {
                    t.Item2();
                    succeeded.Add(t);
                }
                catch (Exception e)
                {
                    _logger.LogError(
                        e,
                        "Deferred acknowledge failed. Will retry later.");
                }
            }

            lock (m_DeferredAcknowledgements)
            {
                Array.ForEach(succeeded.ToArray(), r => m_DeferredAcknowledgements.Remove(r));
            }
        }

        private void ProcessResubscription(bool all = false)
        {
            Tuple<DateTime, Action>[] ready;
            var succeeded = new List<Tuple<DateTime, Action>>();
            lock (m_ResubscriptionSchedule)
            {
                ready = all
                    ? m_ResubscriptionSchedule.ToArray()
                    : m_ResubscriptionSchedule.Where(r => r.Item1 <= DateTime.UtcNow).ToArray();
            }

            foreach (var t in ready)
            {
                try
                {
                    t.Item2();
                    succeeded.Add(t);
                }
                catch (Exception e)
                {
                    _logger.LogInformation(
                        "Resubscription failed: {Error}. Will retry later.",
                        e.Message);
                }
            }

            lock (m_ResubscriptionSchedule)
            {
                Array.ForEach(succeeded.ToArray(), r => m_ResubscriptionSchedule.Remove(r));
            }
        }

        private AcknowledgeDelegate CreateDeferredAcknowledge(Action<bool> ack)
        {
            return (l, b) =>
            {
                if (l == 0)
                {
                    ack(b);
                    return;
                }

                lock (m_DeferredAcknowledgements)
                {
                    m_DeferredAcknowledgements.Add(Tuple.Create<DateTime, Action>(DateTime.UtcNow.AddMilliseconds(l), () => ack(b)));
                    m_DeferredAcknowledger.Schedule(l);
                }
            };
        }

        public string GetStatistics()
        {
            var stats = new StringBuilder();
            int length = m_ProcessingGroups.Keys.Max(k => k.Length);
            m_ProcessingGroups.Aggregate(stats,
                (builder, pair) => builder.AppendFormat(
                    "{0,-" + length + "}\tConcurrencyLevel:{1:-10}\tSent:{2}\tReceived:{3}\tProcessed:{4}" + Environment.NewLine,
                    pair.Key,
                    pair.Value.ConcurrencyLevel == 0 ? "[current thread]" : pair.Value.ConcurrencyLevel.ToString(),
                    pair.Value.SentMessages,
                    pair.Value.ReceivedMessages,
                    pair.Value.ProcessedMessages));
            return stats.ToString();
        }

        public void Dispose()
        {
            m_IsDisposing = true;
            m_DeferredAcknowledger.Dispose();
            m_Resubscriber.Dispose();

            foreach (var processingGroup in m_ProcessingGroups.Values)
            {
                processingGroup.Dispose();
            }
            ProcessDeferredAcknowledgements(true);
        }
    }
}