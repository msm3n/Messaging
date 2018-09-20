namespace Lykke.Messaging
{
    public interface IMessageSerializer<TMessage>
    {
        byte[] Serialize(TMessage message);
        TMessage Deserialize(byte[] message);
    }
}
