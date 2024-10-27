using System;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;
using Azure.Messaging.EventHubs.Producer;
using Azure.Storage.Blobs;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.AzureEventHubs;

public class EventHubQueueOptions
{
    public const string SectionName = "Shuttle:AzureEventHubs";
    public string BlobContainerName { get; set; } = string.Empty;

    public string BlobStorageConnectionString { get; set; } = string.Empty;
    public int CheckpointInterval { get; set; } = 1;

    public string ConnectionString { get; set; } = string.Empty;
    public string ConsumerGroup { get; set; } = EventHubConsumerClient.DefaultConsumerGroupName;
    public TimeSpan ConsumeTimeout { get; set; } = TimeSpan.FromSeconds(30);
    public EventPosition DefaultStartingPosition { get; set; } = EventPosition.Latest;
    public TimeSpan OperationTimeout { get; set; } = TimeSpan.FromSeconds(30);
    public bool ProcessEvents { get; set; }

    public event EventHandler<ConfigureEventArgs<BlobClientOptions>>? ConfigureBlobStorage;
    public event EventHandler<ConfigureEventArgs<EventProcessorClientOptions>>? ConfigureProcessor;
    public event EventHandler<ConfigureEventArgs<EventHubProducerClientOptions>>? ConfigureProducer;
    public event EventHandler<ProcessErrorEventArgs>? ProcessError;

    public void OnConfigureBlobStorage(object? sender, ConfigureEventArgs<BlobClientOptions> args)
    {
        ConfigureBlobStorage?.Invoke(Guard.AgainstNull(sender), Guard.AgainstNull(args));
    }

    public void OnConfigureProcessor(object? sender, ConfigureEventArgs<EventProcessorClientOptions> args)
    {
        ConfigureProcessor?.Invoke(Guard.AgainstNull(sender), Guard.AgainstNull(args));
    }

    public void OnConfigureProducer(object? sender, ConfigureEventArgs<EventHubProducerClientOptions> args)
    {
        ConfigureProducer?.Invoke(Guard.AgainstNull(sender), Guard.AgainstNull(args));
    }

    public void OnProcessError(object? sender, ProcessErrorEventArgs args)
    {
        ProcessError?.Invoke(Guard.AgainstNull(sender), Guard.AgainstNull(args));
    }
}