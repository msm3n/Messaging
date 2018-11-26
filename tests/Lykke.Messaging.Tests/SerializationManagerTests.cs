using System.Threading;
using System.Threading.Tasks;
using Lykke.Common.Log;
using Lykke.Logs;
using Lykke.Logs.Loggers.LykkeConsole;
using Lykke.Messaging.Serialization;
using Moq;
using NUnit.Framework;

namespace Lykke.Messaging.Tests
{
    [TestFixture]
    public class SerializationManagerTests
    {
        private readonly ILogFactory _logFactory;

        public SerializationManagerTests()
        {
            _logFactory = LogFactory.Create().AddUnbufferedConsole();
        }

        [Test]
        public void JsonSerializerIsPresentByDefaultTest()
        {
            var serializationManager = new SerializationManager(_logFactory);

            Assert.NotNull(serializationManager.ExtractSerializer<int>(SerializationFormat.Json));
        }

        [Test]
        public void MessagePackSerializerIsPresentByDefaultTest()
        {
            var serializationManager = new SerializationManager(_logFactory);

            Assert.NotNull(serializationManager.ExtractSerializer<int>(SerializationFormat.MessagePack));
        }

        [Test]
        public void ProtoBufSerializerIsPresentByDefaultTest()
        {
            var serializationManager = new SerializationManager(_logFactory);

            Assert.NotNull(serializationManager.ExtractSerializer<int>(SerializationFormat.ProtoBuf));
        }

        [Test]
        public void AnotherJsonSerializerRegistrationFailureTest()
        {
            var serializationManager = new SerializationManager(_logFactory);
            var serializer = new Mock<IMessageSerializer<int>>();
            var factory = new Mock<ISerializerFactory>();
            factory.Setup(f => f.SerializationFormat).Returns(SerializationFormat.Json);
            factory.Setup(f => f.Create<int>()).Returns(serializer.Object);
            serializationManager.RegisterSerializerFactory(factory.Object);

            Assert.Throws<ProcessingException>(() => serializationManager.ExtractSerializer<int>(SerializationFormat.Json));
        }

        [Test]
        public void SerialiezerShouldBeCreatedOnlyOnceTest()
        {
            var serializationManager = new SerializationManager(_logFactory);
            var mre = new ManualResetEvent(false);

            IMessageSerializer<string> serializer1 = null;
            IMessageSerializer<string> serializer2 = null;

            var t1 = Task.Factory.StartNew(() =>
            {
                mre.WaitOne();
                serializer1 = serializationManager.ExtractSerializer<string>(SerializationFormat.Json);
            });
            var t2 = Task.Factory.StartNew(() =>
            {
                mre.WaitOne();
                serializer2 = serializationManager.ExtractSerializer<string>(SerializationFormat.Json);
            });
            mre.Set();

            Task.WaitAll(new[] { t1, t2 }, 10000);
            Assert.That(serializer1, Is.SameAs(serializer2), "Previousely created serializer was not reused");
        }
    }
}