using Lykke.Messaging.Contract;

namespace Lykke.Messaging.Configuration
{
    public interface IEndpointProvider
    {
        bool Contains(string endpointName);
        Endpoint Get(string endpointName);
    }
}