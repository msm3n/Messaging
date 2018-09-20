using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Lykke.Messaging.Utils;

namespace Lykke.Messaging
{
    internal class QueuedSchedulingStrategy : ISchedulingStrategy
    {
        private readonly QueuedTaskScheduler m_TaskScheduler;
        private readonly Dictionary<int, TaskFactory> m_TaskFactories = new Dictionary<int, TaskFactory>();

        public QueuedSchedulingStrategy(uint threadCount,uint capacity,string name)
        {
            m_TaskScheduler = new QueuedTaskScheduler((int)threadCount,(int)capacity,name);
            m_TaskFactories = new Dictionary<int, TaskFactory>();
        }

        public TaskFactory GetTaskFactory(int priority)
        {
            if (priority < 0)
                throw new ArgumentException("Priority should be > 0", "priority");
            lock (m_TaskFactories)
            {
                TaskFactory factory;
                if (!m_TaskFactories.TryGetValue(priority, out factory))
                {
                    var scheduler = m_TaskScheduler.ActivateNewQueue(priority);
                    factory = new TaskFactory(scheduler);
                    m_TaskFactories.Add(priority, factory);
                }
                return factory;
            }
        }

        public void Dispose()
        {
            m_TaskScheduler.Dispose();
        }
    }
}