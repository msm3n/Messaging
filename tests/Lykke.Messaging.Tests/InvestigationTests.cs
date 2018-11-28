using Lykke.Messaging.Serialization;
using Moq;
using NUnit.Framework;

namespace Lykke.Messaging.Tests
{
    [TestFixture]
    public class SerializationManagerExtensionsTests
    {
        [Test]
        public void DeserializeTest()
        {
            var bytes = new byte[] {0x1};
            var manager = new Mock<ISerializationManager>();
            manager.Setup(m => m.Deserialize<string>(SerializationFormat.Json, bytes)).Returns("test");
            var deserialized = manager.Object.Deserialize(SerializationFormat.Json, bytes, typeof (string));
            Assert.That(deserialized, Is.EqualTo("test"));
        }

        [Test]
        public void SerializeTest()
        {
            var bytes = new byte[] {0x1};
            var manager = new Mock<ISerializationManager>();
            manager.Setup(m => m.Serialize(SerializationFormat.Json, "test")).Returns(bytes);
            var serialized = manager.Object.SerializeObject(SerializationFormat.Json, "test");
            Assert.That(serialized, Is.EqualTo(bytes));
        }
    }
}