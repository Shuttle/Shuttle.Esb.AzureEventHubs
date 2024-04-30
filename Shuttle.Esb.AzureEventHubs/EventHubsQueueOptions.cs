using System;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;
using Azure.Messaging.EventHubs.Producer;
using Azure.Storage.Blobs;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.AzureEventHubs
{
    public class EventHubQueueOptions
    {

        public const string SectionName = "Shuttle:AzureEventHubs";

        public string ConnectionString { get; set; }

        public string BlobStorageConnectionString { get; set; }
        public string BlobContainerName { get; set; }
        public string ConsumerGroup { get; set; } = EventHubConsumerClient.DefaultConsumerGroupName;
        public bool ProcessEvents { get; set; }
        public TimeSpan OperationTimeout { get; set; } = TimeSpan.FromSeconds(30);
        public TimeSpan ConsumeTimeout { get; set; } = TimeSpan.FromSeconds(30);
        public EventPosition DefaultStartingPosition { get; set; } = EventPosition.Latest;
        public int CheckpointInterval { get; set; } = 1;

        public event EventHandler<ConfigureEventArgs<EventHubProducerClientOptions>> ConfigureProducer;
        public event EventHandler<ConfigureEventArgs<BlobClientOptions>> ConfigureBlobStorage;
        public event EventHandler<ConfigureEventArgs<EventProcessorClientOptions>> ConfigureProcessor;
        public event EventHandler<ProcessErrorEventArgs> ProcessError;

        public void OnConfigureProducer(object sender, ConfigureEventArgs<EventHubProducerClientOptions> args)
        {
            Guard.AgainstNull(sender, nameof(sender));
            Guard.AgainstNull(args, nameof(args));

            ConfigureProducer?.Invoke(sender, args);
        }

        public void OnConfigureBlobStorage(object sender, ConfigureEventArgs<BlobClientOptions> args)
        {
            Guard.AgainstNull(sender, nameof(sender));
            Guard.AgainstNull(args, nameof(args));

            ConfigureBlobStorage?.Invoke(sender, args);
        }

        public void OnConfigureProcessor(object sender, ConfigureEventArgs<EventProcessorClientOptions> args)
        {
            Guard.AgainstNull(sender, nameof(sender));
            Guard.AgainstNull(args, nameof(args));

            ConfigureProcessor?.Invoke(sender, args);
        }

        public void OnProcessError(object sender, ProcessErrorEventArgs args)
        {
            Guard.AgainstNull(sender, nameof(sender));
            Guard.AgainstNull(args, nameof(args));

            ProcessError?.Invoke(sender, args);
        }
    }
}