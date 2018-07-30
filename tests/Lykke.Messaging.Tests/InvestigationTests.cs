using Lykke.Messaging.Serialization;
using NUnit.Framework;
using Rhino.Mocks;

namespace Lykke.Messaging.Tests
{
    [TestFixture]
    public class SerializationManagerExtensionsTests
    {
        [Test]
        public void DeserializeTest()
        {
            var bytes = new byte[] {0x1};
            var manager = MockRepository.GenerateMock<ISerializationManager>();
            manager.Expect(m => m.Deserialize<string>(SerializationFormat.Json, bytes)).Return("test");
            var deserialized = manager.Deserialize(SerializationFormat.Json, bytes, typeof (string));
            Assert.That(deserialized, Is.EqualTo("test"));
        }

        [Test]
        public void SerializeTest()
        {
            var bytes = new byte[] {0x1};
            var manager = MockRepository.GenerateMock<ISerializationManager>();
            manager.Expect(m => m.Serialize(SerializationFormat.Json, "test")).Return(bytes);
            var serialized = manager.SerializeObject(SerializationFormat.Json, "test");
            Assert.That(serialized, Is.EqualTo(bytes));
        }
    }
}