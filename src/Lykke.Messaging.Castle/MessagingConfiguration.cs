using System.Collections.Generic;
using Lykke.Messaging.Configuration;
using Lykke.Messaging.Contract;

namespace Lykke.Messaging.Castle
{
    public class MessagingConfiguration : IMessagingConfiguration
    {
        public MessagingConfiguration()
        {
            Transports = new Dictionary<string, TransportInfo>();
            Endpoints = new Dictionary<string, Endpoint>();
            ProcessingGroups=new Dictionary<string, ProcessingGroupInfo>();
        }

        public IDictionary<string, TransportInfo> Transports { get; set; }

        public IDictionary<string, Endpoint> Endpoints { get; set; }

        public IDictionary<string, ProcessingGroupInfo> ProcessingGroups{ get; set; }

        public IDictionary<string, TransportInfo> GetTransports()
        {
            return Transports;
        }

        public IDictionary<string, Endpoint> GetEndpoints()
        {
            return Endpoints;
        }

        public IDictionary<string, ProcessingGroupInfo> GetProcessingGroups()
        {
            return ProcessingGroups;
        }
    }
}