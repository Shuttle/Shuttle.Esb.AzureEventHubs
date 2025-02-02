using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.AzureEventHubs;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddAzureEventHubs(this IServiceCollection services, Action<EventHubQueueBuilder>? builder = null)
    {
        var azureStorageQueuesBuilder = new EventHubQueueBuilder(Guard.AgainstNull(services));

        builder?.Invoke(azureStorageQueuesBuilder);

        services.AddSingleton<IValidateOptions<EventHubQueueOptions>, EventHubQueueOptionsValidator>();

        foreach (var pair in azureStorageQueuesBuilder.EventHubQueueOptions)
        {
            services.AddOptions<EventHubQueueOptions>(pair.Key).Configure(options =>
            {
                options.ConnectionString = pair.Value.ConnectionString;
                options.ProcessEvents = pair.Value.ProcessEvents;
                options.BlobStorageConnectionString = pair.Value.BlobStorageConnectionString;
                options.BlobContainerName = pair.Value.BlobContainerName;
                options.ConsumerGroup = pair.Value.ConsumerGroup;
                options.OperationTimeout = pair.Value.OperationTimeout;
                options.ConsumeTimeout = pair.Value.ConsumeTimeout;
                options.DefaultStartingPosition = pair.Value.DefaultStartingPosition;
                options.CheckpointInterval = pair.Value.CheckpointInterval;

                options.ConfigureProducer += (sender, args) =>
                {
                    pair.Value.OnConfigureProducer(sender, args);
                };

                options.ConfigureBlobStorage += (sender, args) =>
                {
                    pair.Value.OnConfigureBlobStorage(sender, args);
                };

                options.ConfigureProcessor += (sender, args) =>
                {
                    pair.Value.OnConfigureProcessor(sender, args);
                };
            });
        }

        services.TryAddSingleton<IQueueFactory, EventHubQueueFactory>();

        return services;
    }
}