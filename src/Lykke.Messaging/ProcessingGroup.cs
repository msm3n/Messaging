using System;
using System.Reactive.Disposables;
using System.Threading;
using System.Threading.Tasks;
using Lykke.Messaging.Transports;

namespace Lykke.Messaging
{
    internal class ProcessingGroup : IDisposable
    {
        private readonly ISchedulingStrategy m_SchedulingStrategy;
        private readonly uint m_ConcurrencyLevel;

        private volatile bool m_IsDisposing;

        private long m_TasksInProgress = 0;
        private long m_ReceivedMessages = 0;
        private long m_ProcessedMessages = 0;
        private long m_SentMessages = 0;

        public string Name { get; private set; }

        public ProcessingGroup(string name, ProcessingGroupInfo processingGroupInfo)
        {
            Name = name;
            m_ConcurrencyLevel = Math.Max(processingGroupInfo.ConcurrencyLevel, 0);

            m_SchedulingStrategy = (m_ConcurrencyLevel == 0) 
                ? (ISchedulingStrategy) new CurrentThreadSchedulingStrategy()
                : (ISchedulingStrategy) new QueuedSchedulingStrategy(m_ConcurrencyLevel, processingGroupInfo.QueueCapacity, $"ProcessingGroup '{Name}' thread");
        }

        public uint ConcurrencyLevel
        {
            get { return m_ConcurrencyLevel; }
        }

        public long ReceivedMessages
        {
            get { return Interlocked.Read(ref m_ReceivedMessages); }
        }

        public long ProcessedMessages
        {
            get { return Interlocked.Read(ref m_ProcessedMessages); }
        }

        public long SentMessages
        {
            get { return Interlocked.Read(ref m_SentMessages); }
        }

        public IDisposable Subscribe(
            IMessagingSession messagingSession,
            string destination,
            Action<BinaryMessage, Action<bool>> callback,
            string messageType,
            int priority)
        {
            if(m_IsDisposing)
                throw new ObjectDisposedException("ProcessingGroup "+Name);
            var taskFactory = m_SchedulingStrategy.GetTaskFactory(priority);
            var subscription = new SingleAssignmentDisposable();
            subscription.Disposable = messagingSession.Subscribe(destination, (message, ack) =>
            {
                Interlocked.Increment(ref m_TasksInProgress);
                taskFactory.StartNew(() =>
                {
                    Interlocked.Increment(ref m_ReceivedMessages);
                    //if subscription is disposed unack message immediately
                    if (subscription.IsDisposed)
                        ack(false);
                    else
                    {
                        callback(message, ack);
                        Interlocked.Increment(ref m_ProcessedMessages);
                    }
                    Interlocked.Decrement(ref m_TasksInProgress);
                },TaskCreationOptions.HideScheduler);
            }, messageType);
            return subscription;
        }

        public void Dispose()
        {
            m_IsDisposing = true;
            while (Interlocked.Read(ref m_TasksInProgress)>0)
                Thread.Sleep(100);
            m_SchedulingStrategy.Dispose();
        }

        public void Send(
            IMessagingSession messagingSession,
            string publish,
            BinaryMessage message,
            int ttl)
        {
            messagingSession.Send(publish, message, ttl);
            Interlocked.Increment(ref m_SentMessages);
        }
    }
}