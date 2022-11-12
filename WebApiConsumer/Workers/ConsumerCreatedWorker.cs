using System.Text.Json;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using WebApiConsumer.Models;

namespace WebApiConsumer.Workers
{
    public sealed class ConsumerCreatedWorker : BackgroundService
    {
        private readonly ILogger<ConsumerCreatedWorker> _logger;
        private readonly ServiceBusAdministrationClient _serviceBusAdminClient;
        private readonly ServiceBusClient _serviceBusClient;
        private ServiceBusProcessor _serviceBusProcessor;
        private readonly IConfiguration _configuration;

        private readonly string _topicName;
        private readonly string _subscriptionName;

        public ConsumerCreatedWorker(ILogger<ConsumerCreatedWorker> logger, ServiceBusAdministrationClient serviceBusAdminClient, ServiceBusClient serviceBusClient, IConfiguration configuration)
        {
            _logger = logger;
            _serviceBusAdminClient = serviceBusAdminClient;
            _serviceBusClient = serviceBusClient;
            _configuration = configuration;
            _topicName = _configuration["ServiceBusConfiguration:TopicName"];
            _subscriptionName = "consumer.created";
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await SetupSubscription(stoppingToken);
            await SetupProcessorAsync(stoppingToken);

            while (!stoppingToken.IsCancellationRequested)
                await Task.Delay(2000);

            await _serviceBusProcessor!.StopProcessingAsync();
            await _serviceBusProcessor.DisposeAsync();
        }

        private async Task SetupSubscription(CancellationToken stoppingToken)
        {
            if (!await _serviceBusAdminClient.SubscriptionExistsAsync(_topicName, _subscriptionName, stoppingToken))
            {
                _logger.LogInformation($"Creating subscription '{_subscriptionName}'.");

                await _serviceBusAdminClient.CreateSubscriptionAsync(
                    new CreateSubscriptionOptions(_topicName, _subscriptionName),
                    new CreateRuleOptions("only_subject_created", new CorrelationRuleFilter
                    {
                        Subject = "created"
                    }));

                return;
            }

            _logger.LogInformation($"Subscription '{_subscriptionName} already exists.'");
        }

        private async Task SetupProcessorAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Creating processor.");

            _serviceBusProcessor = _serviceBusClient.CreateProcessor(_topicName, _subscriptionName, new ServiceBusProcessorOptions
            {
                AutoCompleteMessages = false,
                MaxConcurrentCalls = 10,
                ReceiveMode = ServiceBusReceiveMode.PeekLock
            });

            _serviceBusProcessor.ProcessMessageAsync += ProcessMessageAsync;
            _serviceBusProcessor.ProcessErrorAsync += ProcessErrorAsync;

            await _serviceBusProcessor.StartProcessingAsync(stoppingToken);
        }

        private async Task ProcessMessageAsync(ProcessMessageEventArgs eventArgs)
        {
            var client = eventArgs.Message.Body.ToObjectFromJson<Client>();
            eventArgs.Message.ApplicationProperties.TryGetValue("trace_id", out var traceId);
            eventArgs.Message.ApplicationProperties.TryGetValue("origin", out var origin);

            _logger.LogInformation($"Trace ID: {traceId} | Origin: {origin} | Message: {JsonSerializer.Serialize(client)}");

            await eventArgs.CompleteMessageAsync(eventArgs.Message);
        }

        private Task ProcessErrorAsync(ProcessErrorEventArgs eventArgs)
        {
            _logger.LogError("Error on process message.");
            return Task.CompletedTask;
        }
    }
}