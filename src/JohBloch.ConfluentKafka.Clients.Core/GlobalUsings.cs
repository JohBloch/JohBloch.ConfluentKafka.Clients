// System namespaces
global using System;
global using System.Collections.Concurrent;
global using System.Collections.Generic;
global using System.IO;
global using System.Linq;
global using System.Numerics;
global using System.Reflection;
global using System.Text;
global using System.Text.Json;
global using System.Text.RegularExpressions;
global using System.Threading;
global using System.Threading.Tasks;

// Confluent namespaces
global using Confluent.Kafka;
global using Confluent.Kafka.SyncOverAsync;
global using Confluent.SchemaRegistry;

// Microsoft Extensions namespaces
global using Microsoft.Extensions.Logging;
global using Microsoft.Extensions.Options;

// Schema Registry Ext Client namespaces
global using JohBloch.ConfluentKafka.SchemaRegistryExtClient.Models;

// Internal project namespaces
global using JohBloch.ConfluentKafka.Clients.Interfaces;
global using JohBloch.ConfluentKafka.Clients.Models;
global using JohBloch.ConfluentKafka.Clients.Services;

global using JohBloch.ConfluentKafka.Clients.Security;