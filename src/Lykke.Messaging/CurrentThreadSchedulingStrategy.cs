using System.Threading.Tasks;
using Lykke.Messaging.Utils;

namespace Lykke.Messaging
{
    internal class CurrentThreadSchedulingStrategy : ISchedulingStrategy
    {
        public void Dispose()
        {
        }

        public TaskFactory GetTaskFactory(int priority)
        {
            if (priority != 0)
                throw new InvalidSubscriptionException(
                    "Priority other then 0 is not applicable for processing group with zero concurrencyLevel (messages are processed on consuming thread)");

            return new TaskFactory(new CurrentThreadTaskScheduler());
        }
    }
}