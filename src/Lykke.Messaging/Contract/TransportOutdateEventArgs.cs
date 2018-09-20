using System;

namespace Lykke.Messaging.Contract
{
    public class TransportOutdateEventArgs : EventArgs
    {
        public string Message { get; private set; }
        public string TransportName { get; private set; }
//        internal TransportResolutionTag TransportTag { get; private set; }

        internal TransportOutdateEventArgs(string message, string transportName)
        {
            TransportName = transportName ?? throw new ArgumentNullException("transportName");
            Message = message??"";
        }

/*
        internal TransportOutdateEventArgs(string message, string transportName, TransportResolutionTag transportTag)
        {
            Message = message ?? "";
            TransportName = transportName;
            TransportTag = transportTag;
        }
*/
    }
}