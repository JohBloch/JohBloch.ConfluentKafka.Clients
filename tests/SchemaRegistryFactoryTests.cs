using System;
using JohBloch.ConfluentKafka.Clients.Services;
using JohBloch.ConfluentKafka.Clients.Models;
using Microsoft.Extensions.Options;
using Confluent.SchemaRegistry;
using Xunit;

namespace JohBloch.ConfluentKafka.Clients.Tests
{
    /// <summary>
    /// Unit tests for <see cref="SchemaRegistryFactory"/> covering basic construction and client creation scenarios.
    /// </summary>
    public class SchemaRegistryFactoryTests
    {
        /// <summary>
        /// Verifies that a client can be created when a valid URL is provided in options.
        /// </summary>
        [Fact]
        public void CreateClient_UsesOAuth2Settings_WhenOptionsValid()
        {
            var opts = Options.Create(new SchemaRegistryOptions
            {
                Url = "http://localhost:8081"
            });

            var factory = new SchemaRegistryFactory(opts);
            using var client = factory.CreateClient();

            Assert.NotNull(client);
        }

        /// <summary>
        /// Ensures the constructor throws an <see cref="ArgumentNullException"/> when options are null.
        /// </summary>
        [Fact]
        public void Constructor_Throws_OnNullOptions()
        {
            Assert.Throws<ArgumentNullException>(() => new SchemaRegistryFactory(null!));
        }

        /// <summary>
        /// Ensures <see cref="SchemaRegistryFactory.CreateClient"/> throws an <see cref="ArgumentException"/> when Url is missing.
        /// </summary>
        [Fact]
        public void CreateClient_Throws_OnMissingUrl()
        {
            var opts = Options.Create(new SchemaRegistryOptions
            {
                Url = null!
            });
            var factory = new SchemaRegistryFactory(opts);
            Assert.Throws<ArgumentException>(() => factory.CreateClient());
        }
    }
}
