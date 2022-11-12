using System.Text.Json;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using WebApiConsumer.Models;

namespace WebApiConsumer.Workers
{
    public class ConsumerDeletedWorker : ConsumerBase<Client>
    {
        private readonly IConfiguration _configuration;

        public ConsumerDeletedWorker(ILogger<ConsumerDeletedWorker> logger, ServiceBusAdministrationClient serviceBusAdministrationClient, ServiceBusClient serviceBusClient, IConfiguration configuration) : base(logger, serviceBusAdministrationClient, serviceBusClient)
        {
            _configuration = configuration;
        }

        protected override async Task ConfigureAsync(CancellationToken stoppingToken)
        {
            var topicName = _configuration["ServiceBusConfiguration:TopicName"];
            var subscriptionName = "consumer.deleted";

            await CreateSubscriptionIfNotExistsAsync(topicName, subscriptionName, stoppingToken);
        }

        protected override Task ProcessMessageAsync(ProcessMessageEventArgs eventArgs)
        {
            var client = eventArgs.Message.Body.ToObjectFromJson<Client>();
            eventArgs.Message.ApplicationProperties.TryGetValue("trace_id", out var traceId);
            eventArgs.Message.ApplicationProperties.TryGetValue("origin", out var origin);

            Logger.LogInformation($"Trace ID: {traceId} | Origin: {origin} | Message: {JsonSerializer.Serialize(client)}");

            return Task.CompletedTask;
        }

        protected override CreateRuleOptions GetRuleOptions()
        {
            return new CreateRuleOptions("only_deleted", new CorrelationRuleFilter
            {
                Subject = "deleted"
            });
        }
    }
}