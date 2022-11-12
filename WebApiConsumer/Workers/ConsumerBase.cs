using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

namespace WebApiConsumer.Workers
{
    public abstract class ConsumerBase<TMessage> : BackgroundService
    {
        protected readonly ILogger<ConsumerBase<TMessage>> Logger;
        protected readonly ServiceBusAdministrationClient ServiceBusAdministrationClient;
        protected readonly ServiceBusClient ServiceBusClient;
        protected ServiceBusProcessor ServiceBusProcessor;

        private string _topicName;
        private string _subscriptionName;
        private string _queueName;
        private bool _isSubscription = false;

        private int _maxConcurrentCalls = 10;

        public ConsumerBase(ILogger<ConsumerBase<TMessage>> logger, ServiceBusAdministrationClient serviceBusAdministrationClient, ServiceBusClient serviceBusClient)
        {
            Logger = logger;
            ServiceBusAdministrationClient = serviceBusAdministrationClient;
            ServiceBusClient = serviceBusClient;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await ConfigureAsync(stoppingToken);
            CreateProcessor(stoppingToken);
            await ServiceBusProcessor.StartProcessingAsync(stoppingToken);

            while (!stoppingToken.IsCancellationRequested)
                await Task.Delay(1000);

            await ServiceBusProcessor.StopProcessingAsync(stoppingToken);
            await ServiceBusProcessor.DisposeAsync();
        }

        protected abstract Task ConfigureAsync(CancellationToken stoppingToken);

        protected async Task CreateSubscriptionIfNotExistsAsync(string topic, string subscription, CancellationToken stoppingToken)
        {
            _topicName = topic;
            _subscriptionName = subscription;
            _isSubscription = true;

            if (!await ServiceBusAdministrationClient.SubscriptionExistsAsync(_topicName, _subscriptionName, stoppingToken))
            {
                Logger.LogInformation($"Creating subscription '{_subscriptionName}'.");

                var options = new CreateSubscriptionOptions(_topicName, _subscriptionName);
                var rule = GetRuleOptions();

                await ServiceBusAdministrationClient.CreateSubscriptionAsync(options, rule, stoppingToken);
            }
        }

        private async Task CreateQueueIfNotExistsAsync(string queueName, CancellationToken stoppingToken)
        {
            _queueName = queueName;

            if (!await ServiceBusAdministrationClient.QueueExistsAsync(_queueName, stoppingToken))
            {
                Logger.LogInformation($"Creating queue '{_queueName}'.");

                var options = new CreateQueueOptions(_queueName);

                await ServiceBusAdministrationClient.CreateQueueAsync(options, stoppingToken);
            }
        }

        private void CreateProcessor(CancellationToken stoppingToken)
        {
            var options = new ServiceBusProcessorOptions
            {
                AutoCompleteMessages = false,
                ReceiveMode = ServiceBusReceiveMode.PeekLock,
                MaxConcurrentCalls = _maxConcurrentCalls
            };

            if (_isSubscription)
                ServiceBusProcessor = ServiceBusClient.CreateProcessor(_topicName, _subscriptionName, options);
            else
                ServiceBusProcessor = ServiceBusClient.CreateProcessor(_queueName, options);

            ServiceBusProcessor.ProcessMessageAsync += Event_ProcessMessageAsync;
            ServiceBusProcessor.ProcessErrorAsync += Event_ProcessErrorAsync;
        }

        // Ref: https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-filter-examples
        // https://learn.microsoft.com/en-us/azure/service-bus-messaging/topic-filters
        protected virtual CreateRuleOptions GetRuleOptions()
        {
            return new CreateRuleOptions("Default");
        }

        private async Task Event_ProcessMessageAsync(ProcessMessageEventArgs eventArgs)
        {
            await ProcessMessageAsync(eventArgs);
            await eventArgs.CompleteMessageAsync(eventArgs.Message);
        }

        protected abstract Task ProcessMessageAsync(ProcessMessageEventArgs eventArgs);

        protected virtual Task Event_ProcessErrorAsync(ProcessErrorEventArgs eventArgs)
        {
            Logger.LogError(eventArgs.Exception.StackTrace);
            return Task.CompletedTask;
        }
    }
}