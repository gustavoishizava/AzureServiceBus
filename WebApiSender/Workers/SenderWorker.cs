using System.Text.Json;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Bogus;
using WebApiSender.Models;

namespace WebApiSender.Workers
{
    public sealed class SenderWorker : BackgroundService
    {
        private readonly ILogger<SenderWorker> _logger;
        private readonly ServiceBusAdministrationClient _serviceBusAdminClient;
        private readonly ServiceBusClient _serviceBusClient;
        private readonly ServiceBusSender _serviceBusSender;

        private readonly string _topicName;

        public SenderWorker(ILogger<SenderWorker> logger, ServiceBusAdministrationClient serviceBusAdminClient, IConfiguration configuration, ServiceBusClient serviceBusClient)
        {
            _logger = logger;
            _serviceBusAdminClient = serviceBusAdminClient;
            _topicName = configuration["ServiceBusConfiguration:TopicName"];
            _serviceBusClient = serviceBusClient;

            _serviceBusSender = _serviceBusClient.CreateSender(_topicName);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await SetupTopicAsync(stoppingToken);

            while (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation("Publish message.");
                await SendMessageAsync(stoppingToken);
            }
        }

        private async Task SetupTopicAsync(CancellationToken stoppingToken)
        {
            if (!await _serviceBusAdminClient.TopicExistsAsync(_topicName, stoppingToken))
            {
                _logger.LogInformation($"Creating topic '{_topicName}'.");

                await _serviceBusAdminClient.CreateTopicAsync(_topicName, stoppingToken);

                return;
            }

            _logger.LogInformation($"Topic '{_topicName}' already exists.");
        }

        private async Task SendMessageAsync(CancellationToken stoppingToken)
        {
            var client = CreateClient();
            var message = new ServiceBusMessage(JsonSerializer.Serialize(client));

            var subject = "created";
            if (client.Name.Length % 2 == 0)
                subject = "updated";

            message.Subject = subject;

            message.ApplicationProperties.TryAdd("trace_id", Guid.NewGuid().ToString());
            message.ApplicationProperties.TryAdd("origin", "sender");

            await _serviceBusSender.SendMessageAsync(message, stoppingToken);
        }

        private Client CreateClient()
        {
            var faker = new Faker();
            return new Client(faker.Person.FullName, faker.Internet.Email(), faker.Person.Phone);
        }
    }
}