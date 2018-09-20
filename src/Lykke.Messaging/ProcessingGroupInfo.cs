namespace Lykke.Messaging
{
    public class ProcessingGroupInfo
    {
        public ProcessingGroupInfo()
        {
            ConcurrencyLevel = 0;
            QueueCapacity = 1024;
        }

        public ProcessingGroupInfo(ProcessingGroupInfo info)
        {
            ConcurrencyLevel = info.ConcurrencyLevel;
            QueueCapacity = info.QueueCapacity;
        }

        public uint ConcurrencyLevel { get; set; }
        public uint QueueCapacity { get; set; }
    }
}
