using System;
using System.Threading.Tasks;

namespace Lykke.Messaging
{
    interface ISchedulingStrategy :IDisposable
    {
        TaskFactory GetTaskFactory(int priority);
    }
}