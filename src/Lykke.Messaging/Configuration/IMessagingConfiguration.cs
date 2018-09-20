using System.Collections.Generic;
using Lykke.Messaging.Contract;

namespace Lykke.Messaging.Configuration
{
    public interface IMessagingConfiguration
    {
        IDictionary<string, TransportInfo> GetTransports();
        IDictionary<string, Endpoint> GetEndpoints();
        IDictionary<string, ProcessingGroupInfo> GetProcessingGroups();
    }
}