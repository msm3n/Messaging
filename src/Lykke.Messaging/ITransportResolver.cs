namespace Lykke.Messaging
{
    public interface ITransportResolver
    {
        TransportInfo GetTransport(string transportId);
    }
}
