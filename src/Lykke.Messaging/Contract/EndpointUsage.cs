using System;

namespace Lykke.Messaging.Contract
{
    [Flags]
    public enum EndpointUsage
    {
        None,
        Publish,
        Subscribe,
    }
}
