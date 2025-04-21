﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Lykke.Messaging.Contract;
using Lykke.Messaging.Serialization;
using Lykke.Messaging.Transports;
using Moq;
using NUnit.Framework;
using Assert = NUnit.Framework.Assert;

namespace Lykke.Messaging.Tests
{
    [TestFixture]
    public class ProcessingGroupManagerTests : IDisposable
    {
        public void Dispose()
        {

        }

        [Test]
        public void ProcessingGroupWithZeroConcurrencyDoesNotAcceptPriority()
        {
            using (var processingGroup = new ProcessingGroup("test", new ProcessingGroupInfo()))
            {
                Assert.That(
                    () => processingGroup.Subscribe(
                        new Mock<IMessagingSession>().Object,
                        "dest",
                        (message, action) => { },
                        null,
                        1),
                    Throws.TypeOf<InvalidSubscriptionException>());
            }
        }

        [Test]
        public void SameThreadSubscriptionTest()
        {
            var transportManager = new TransportManager(
                new TransportResolver(new Dictionary<string, TransportInfo>
                    {
                        {"transport-1", new TransportInfo("transport-1", "login1", "pwd1", "None", "InMemory")}
                    }));
            var processingGroupManager = new ProcessingGroupManager(transportManager);
            var endpoint = new Endpoint { Destination = "queue", TransportId = "transport-1" };
            var session = transportManager.GetMessagingSession(endpoint, "pg");
            var usedThreads = new List<int>();
            var subscription = processingGroupManager.Subscribe(endpoint,
                (message, action) =>
                {
                    lock (usedThreads)
                    {
                        usedThreads.Add(Thread.CurrentThread.ManagedThreadId);
                    }
                    Thread.Sleep(50);
                }, null, "pg", 0);
            using (subscription)
            {
                Enumerable.Range(1, 20).ToList().ForEach(i => session.Send("queue", new BinaryMessage(), 0));
                Thread.Sleep(2000);
            }
            Assert.That(usedThreads.Count(), Is.EqualTo(20), "not all messages were processed");
            Assert.That(usedThreads.Distinct().Count(), Is.EqualTo(1), "more then one thread was used for message processing");
        }

        [Test]
        public void MultiThreadThreadSubscriptionTest()
        {
            var transportManager = new TransportManager(
                new TransportResolver(new Dictionary<string, TransportInfo>
                    {
                        {"transport-1", new TransportInfo("transport-1", "login1", "pwd1", "None", "InMemory")}
                    }));
            var processingGroupManager = new ProcessingGroupManager(transportManager, new Dictionary<string, ProcessingGroupInfo>()
            {
                {
                    "pg", new ProcessingGroupInfo() {ConcurrencyLevel = 3}
                }
            });
            var endpoint = new Endpoint { Destination = "queue", TransportId = "transport-1" };
            var processingGroup = transportManager.GetMessagingSession(endpoint, "pg");
            var usedThreads = new List<int>();
            var subscription = processingGroupManager.Subscribe(
                endpoint,
                (message, action) =>
                {
                    lock (usedThreads)
                    {
                        usedThreads.Add(Thread.CurrentThread.ManagedThreadId);
                    }
                    Thread.Sleep(50);
                },
                null,
                "pg",
                0);

            using (subscription)
            {
                Enumerable.Range(1, 20)
                    .ToList()
                    .ForEach(i => processingGroup.Send(
                        "queue",
                        new BinaryMessage { Bytes = Encoding.UTF8.GetBytes((i % 3).ToString()) },
                        0));
                Thread.Sleep(1200);
            }
            Assert.That(usedThreads.Count, Is.EqualTo(20), "not all messages were processed");
            Assert.That(usedThreads.Distinct().Count(), Is.EqualTo(3), "wrong number of threads was used for message processing");
        }

        [Test]
        public void QueuedTaskSchedulerIsHiddenTest()
        {
            var transportManager = new TransportManager(
                new TransportResolver(new Dictionary<string, TransportInfo>
                    {
                        {"transport-1", new TransportInfo("transport-1", "login1", "pwd1", "None", "InMemory")}
                    }));
            var processingGroupManager = new ProcessingGroupManager(transportManager, new Dictionary<string, ProcessingGroupInfo>()
            {
                {
                    "pg", new ProcessingGroupInfo() {ConcurrencyLevel = 1,QueueCapacity = 1000}
                }
            });

            var e = new ManualResetEvent(false);
            var endpoint = new Endpoint { Destination = "queue", TransportId = "transport-1" };
            var processingGroup = transportManager.GetMessagingSession(endpoint, "pg");
            var childTaskFinishedBeforeHandler = false;
            var subscription = processingGroupManager.Subscribe(
                endpoint,
                (message, action) =>
                {
                    if (Task.Factory.StartNew(() => { }).Wait(500))
                        childTaskFinishedBeforeHandler = true;
                    e.Set();
                },
                null,
                "pg",
                0);

            using (subscription)
            {
                processingGroup.Send("queue", new BinaryMessage { Bytes = Encoding.UTF8.GetBytes((100).ToString()) }, 0);
                e.WaitOne(1000);
                Assert.That(childTaskFinishedBeforeHandler, "Child task used scheduler from QueuedTaskScheduler");
            }
        }

        [Test]
        public void MultiThreadPrioritizedThreadSubscriptionTest()
        {
            var transportManager = new TransportManager(
                new TransportResolver(new Dictionary<string, TransportInfo>
                    {
                        {"transport-1", new TransportInfo("transport-1", "login1", "pwd1", "None", "InMemory")}
                    }));
            var processingGroupManager = new ProcessingGroupManager(
                transportManager,
                new Dictionary<string, ProcessingGroupInfo>()
                {
                    {
                        "pg", new ProcessingGroupInfo {ConcurrencyLevel = 3}
                    }
                });

            var usedThreads = new List<int>();
            var processedMessages = new List<int>();
            Action<BinaryMessage, AcknowledgeDelegate> callback = (message, action) =>
            {
                lock (usedThreads)
                {
                    usedThreads.Add(Thread.CurrentThread.ManagedThreadId);
                    processedMessages.Add(int.Parse(Encoding.UTF8.GetString(message.Bytes)));
                }
                Thread.Sleep(50);
            };

            var subscription0 = processingGroupManager.Subscribe(new Endpoint { Destination = "queue0", TransportId = "transport-1" }, callback, null, "pg", 0);
            var subscription1 = processingGroupManager.Subscribe(new Endpoint { Destination = "queue1", TransportId = "transport-1" }, callback, null, "pg", 1);
            var subscription2 = processingGroupManager.Subscribe(new Endpoint { Destination = "queue2", TransportId = "transport-1" }, callback, null, "pg", 2);

            using (subscription0)
            using (subscription1)
            using (subscription2)
            {
                Enumerable.Range(1, 20)
                    .ToList()
                    .ForEach(i => processingGroupManager.Send(
                        new Endpoint { Destination = "queue" + i % 3, TransportId = "transport-1" },
                        new BinaryMessage { Bytes = Encoding.UTF8.GetBytes((i % 3).ToString()) },
                        0,
                        "pg"));

                Thread.Sleep(1200);
                ResolvedTransport transport = transportManager.ResolveTransport("transport-1");
                Assert.That(transport.Sessions.Select(s => s.Name).OrderBy(s => s), Is.EqualTo(new[] { "pg priority0", "pg priority1", "pg priority2" }), "Wrong sessions were created. Expectation: one session per [processingGroup,priority] pair ");
            }
            Assert.That(usedThreads.Count, Is.EqualTo(20), "not all messages were processed");
            Assert.That(usedThreads.Distinct().Count(), Is.EqualTo(3), "wrong number of threads was used for message processing");

            double averageOrder0 = processedMessages.Select((i, index) => new { message = i, index }).Where(arg => arg.message == 0).Average(arg => arg.index);
            double averageOrder1 = processedMessages.Select((i, index) => new { message = i, index }).Where(arg => arg.message == 1).Average(arg => arg.index);
            double averageOrder2 = processedMessages.Select((i, index) => new { message = i, index }).Where(arg => arg.message == 2).Average(arg => arg.index);
            Assert.That(averageOrder0, Is.LessThan(averageOrder1), "priority was not respected");
            Assert.That(averageOrder1, Is.LessThan(averageOrder2), "priority was not respected");
        }

        [Test]
        public void DeferredAcknowledgementTest()
        {
            Action<BinaryMessage, Action<bool>> callback = null;
            using (var processingGroupManager = CreateProcessingGroupManagerWithMockedDependencies(action => callback = action))
            {
                DateTime processed = default(DateTime);
                DateTime acked = default(DateTime);
                processingGroupManager.Subscribe(
                    new Endpoint("test", "test", false, SerializationFormat.Json),
                    (message, acknowledge) =>
                    {
                        processed = DateTime.UtcNow;
                        acknowledge(1000, true);
                    },
                    null,
                    "ProcessingGroup",
                    0);
                var acknowledged = new ManualResetEvent(false);
                callback(new BinaryMessage { Bytes = new byte[0], Type = typeof(string).Name }, b =>
                {
                    acked = DateTime.UtcNow;
                    acknowledged.Set();
                });
                Assert.That(acknowledged.WaitOne(1300), Is.True, "Message was not acknowledged");
                Assert.That((acked - processed).TotalMilliseconds, Is.GreaterThan(1000), "Message was acknowledged earlier than scheduled time ");
            }
        }

        [Test]
        public void MessagesStuckedInProcessingGroupAfterUnsubscriptionShouldBeUnacked()
        {
            bool? normallyProcessedMessageAck = null;
            bool? stuckedInQueueMessageAck = null;
            var finishProcessing = new ManualResetEvent(false);
            Action<BinaryMessage, Action<bool>> callback = null;
            using (var processingGroupManager = CreateProcessingGroupManagerWithMockedDependencies(action => callback = action))
            {
                IDisposable subscription = null;
                subscription = processingGroupManager.Subscribe(
                    new Endpoint("test", "test", false, SerializationFormat.Json),
                    (message, acknowledge) =>
                    {
                        acknowledge(0, true);
                        finishProcessing.WaitOne();

                        //dispose subscription in first message processing to ensure first message processing starts before unsubscription 
                        subscription.Dispose();
                    },
                    null,
                    "SingleThread",
                    0);

                callback(new BinaryMessage { Bytes = new byte[] { 1 }, Type = typeof(string).Name }, b => normallyProcessedMessageAck = b);
                finishProcessing.Set();
                callback(new BinaryMessage { Bytes = new byte[] { 2 }, Type = typeof(string).Name }, b => stuckedInQueueMessageAck = b);
            }
            Assert.That(normallyProcessedMessageAck, Is.True, "Normally processed message was not acked");
            Assert.That(stuckedInQueueMessageAck, Is.False, "Stucked message was not unacked");
        }

        [Test]
        public void DeferredAcknowledgementShouldBePerformedOnDisposeTest()
        {
            Action<BinaryMessage, Action<bool>> callback = null;
            bool acknowledged = false;

            using (var processingGroupManager = CreateProcessingGroupManagerWithMockedDependencies(action => callback = action))
            {
                processingGroupManager.Subscribe(
                    new Endpoint("test", "test", false, SerializationFormat.Json),
                    (message, acknowledge) =>
                    {
                        acknowledge(60000, true);
                    },
                    null,
                    "ProcessingGroup",
                    0);
                callback(
                    new BinaryMessage { Bytes = new byte[0], Type = typeof(string).Name },
                    b => { acknowledged = true; });
            }

            Assert.That(acknowledged, Is.True, "Message was not acknowledged on engine dispose");
        }

        [Test]
        public void DuplicateSubscriptionFailuresShoudNotCauseDoubleResubscriptionTest()
        {
            var subscribed = new AutoResetEvent(false);
            int subscriptionsCounter = 0;
            Action onSubscribe = () =>
            {
                Interlocked.Increment(ref subscriptionsCounter);
                subscribed.Set();
            };
            Action emulateFail = null;
            using (var processingGroupManager = CreateProcessingGroupManagerWithMockedDependencies(
                action => { },
                action => emulateFail = emulateFail ?? action,
                onSubscribe))
            {
                using (processingGroupManager.Subscribe(new Endpoint("test", "test", false, SerializationFormat.Json), (message, acknowledge) =>
                {
                    acknowledge(0, true);
                }, null, "ProcessingGroup", 0))
                {
                    subscribed.WaitOne();
                    emulateFail();
                    Thread.Sleep(100);
                    emulateFail();
                }
                Assert.That(subscriptionsCounter, Is.EqualTo(2), "Duplicate subscription failure report leaded to double resubscription");
            }
        }

        [Test]
        public void ResubscriptionTest()
        {
            Action<BinaryMessage, Action<bool>> callback = null;
            Action emulateFail = () => { };
            int subscriptionsCounter = 0;
            var subscribed = new AutoResetEvent(false);
            Action onSubscribe = () =>
            {
                subscriptionsCounter++;
                if (subscriptionsCounter == 1 || subscriptionsCounter == 3 || subscriptionsCounter == 5)
                    throw new Exception("Fail #" + subscriptionsCounter);
                subscribed.Set();
            };

            using (var processingGroupManager = CreateProcessingGroupManagerWithMockedDependencies(
                action => callback = action,
                action => emulateFail = action,
                onSubscribe))
            {
                var subscription = processingGroupManager.Subscribe(
                    new Endpoint("test", "test", false, SerializationFormat.Json),
                    (message, acknowledge) =>
                    {
                        acknowledge(0, true);
                    },
                    null,
                    "ProcessingGroup",
                    0);

                bool eventRes = subscribed.WaitOne(1200);
                //First attempt fails next one happends in 1000ms and should be successfull
                Assert.True(eventRes, "Has not resubscribed after first subscription fail");
                callback(
                    new BinaryMessage { Bytes = new byte[0], Type = typeof(string).Name },
                    b => { });
                Thread.Sleep(300);

                emulateFail();

                //First attempt is taken right after failure, but it fails next one happends in 1000ms and should be successfull
                Assert.That(subscribed.WaitOne(1500), Is.True, "Resubscription has not happened within resubscription timeout");
                Assert.That(subscriptionsCounter, Is.EqualTo(4));
                Thread.Sleep(300);

                emulateFail();

                subscription.Dispose();
                //First attempt is taken right after failure, but it fails next one should not be taken since subscription is disposed
                Assert.That(subscribed.WaitOne(3000), Is.False, "Resubscription happened after subscription was disposed");
            }
        }

        [Test]
        [Ignore("PerformanceTest")]
        public void DeferredAcknowledgementPerformanceTest()
        {
            var rnd = new Random();
            const int messageCount = 100;
            var delays = new Dictionary<long, string>
            {
                { 0, "immediate acknowledge" },
                { 1, "deferred acknowledge" },
                { 100, "deferred acknowledge" },
                { 1000, "deferred acknowledge" },
            };
            foreach (var delay in delays)
            {
                Action<BinaryMessage, Action<bool>> callback = null;
                long counter = messageCount;
                var complete = new ManualResetEvent(false);
                var processingGroupManager = CreateProcessingGroupManagerWithMockedDependencies(action => callback = action);
                Stopwatch sw = Stopwatch.StartNew();
                processingGroupManager.Subscribe(new Endpoint("test", "test", false, SerializationFormat.Json), (message, acknowledge) =>
                    {
                        Thread.Sleep(rnd.Next(1, 10));
                        acknowledge(delay.Key, true);
                    }, null, "ProcessingGroup", 0);
                for (int i = 0; i < messageCount; i++)
                    callback(new BinaryMessage { Bytes = new byte[0], Type = typeof(string).Name }, b =>
                        {
                            if (Interlocked.Decrement(ref counter) == 0)
                                complete.Set();
                        });
                complete.WaitOne();
            }
        }

        private ProcessingGroupManager CreateProcessingGroupManagerWithMockedDependencies(
            Action<Action<BinaryMessage, Action<bool>>> setCallback,
            Action<Action> setOnFail = null,
            Action onSubscribe = null)
        {
            if (setOnFail == null)
                setOnFail = action => { };
            if (onSubscribe == null)
                onSubscribe = () => { };
            var transportManager = new Mock<ITransportManager>();
            var session = new Mock<IMessagingSession>();
            session
                .Setup(p => p.Subscribe(It.IsAny<string>(), It.IsAny<Action<BinaryMessage, Action<bool>>>(), It.IsAny<string>()))
                .Callback<string, Action<BinaryMessage, Action<bool>>, string>((dst, invocation, mt) =>
                {
                    setCallback(invocation);
                    onSubscribe();
                })
                .Returns(new Mock<IDisposable>().Object);
            transportManager
                .Setup(t => t.GetMessagingSession(It.IsAny<Endpoint>(), It.IsAny<string>(), It.IsAny<Action>()))
                .Callback<Endpoint, string, Action>((endpoint, name, invocation) => setOnFail(invocation))
                .Returns(session.Object);
            return new ProcessingGroupManager(
                transportManager.Object,
                new Dictionary<string, ProcessingGroupInfo>
                {
                    {"SingleThread", new ProcessingGroupInfo{ConcurrencyLevel = 1}},
                    {"MultiThread", new ProcessingGroupInfo{ConcurrencyLevel = 3}}
                },
                1000);
        }
    }
}
