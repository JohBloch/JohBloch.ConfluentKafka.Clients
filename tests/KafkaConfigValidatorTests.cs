using JohBloch.ConfluentKafka.Clients.Services;
using Xunit;

namespace JohBloch.ConfluentKafka.Clients.Tests
{
    /// <summary>
    /// Unit tests for <see cref="KafkaConfigValidator"/>.
    /// Covers both valid and invalid input paths for global and consumer config.
    /// </summary>
    public class KafkaConfigValidatorTests
    {
        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData(" ")]
        public void ValidateGlobal_ReturnsError_WhenBootstrapServersMissing(string? bootstrapServers)
        {
            // Act
            var result = KafkaConfigValidator.ValidateGlobal(bootstrapServers);

            // Assert
            Assert.Equal("Missing Kafka.Global.Config required key: bootstrap.servers", result);
        }

        [Fact]
        public void ValidateGlobal_ReturnsNull_WhenBootstrapServersProvided()
        {
            // Act
            var result = KafkaConfigValidator.ValidateGlobal("broker1:9092");

            // Assert
            Assert.Null(result);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData(" ")]
        public void ValidateConsumer_ReturnsError_WhenGroupIdMissing(string? groupId)
        {
            // Arrange
            var consumerName = "orders-consumer";

            // Act
            var result = KafkaConfigValidator.ValidateConsumer(consumerName, groupId);

            // Assert
            Assert.Equal($"Missing required consumer setting group.id for {consumerName}", result);
        }

        [Fact]
        public void ValidateConsumer_ReturnsNull_WhenGroupIdProvided()
        {
            // Act
            var result = KafkaConfigValidator.ValidateConsumer("orders-consumer", "orders-group");

            // Assert
            Assert.Null(result);
        }
    }
}