using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using JohBloch.ConfluentKafka.Clients.Configuration;
using JohBloch.ConfluentKafka.Clients.Models;
using Xunit;

namespace JohBloch.ConfluentKafka.Clients.Tests
{
    public class SchemaRegistryExtRegistrationTests
    {
        [Fact]
        public void AddKafkaClients_Registers_ExtClient()
        {
            var services = new ServiceCollection();

            services.AddKafkaClients(opts =>
            {
                opts.SchemaRegistryUrl = "http://localhost:8081";
            });

            var sp = services.BuildServiceProvider();

            var ext = sp.GetService<JohBloch.ConfluentKafka.SchemaRegistryExtClient.Interfaces.ISchemaRegistryExtClient>();
            var client = sp.GetService<Confluent.SchemaRegistry.ISchemaRegistryClient>();

            Assert.NotNull(ext);
            // We no longer register a Confluent.ISchemaRegistryClient - public API changed to use ISchemaRegistryExtClient
            Assert.Null(client);
        }
    }
}