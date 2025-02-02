using System;
using Azure.Messaging.EventHubs.Consumer;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.AzureEventHubs.Tests;

public class AzureEventHubsConfiguration
{
    public static IServiceCollection GetServiceCollection()
    {
        var services = new ServiceCollection();

        var configuration = new ConfigurationBuilder().AddUserSecrets<AzureEventHubsConfiguration>().Build();

        services.AddSingleton<IConfiguration>(configuration);

        services.AddAzureEventHubs(builder =>
        {
            // connection strings in `secrets.json`
            var eventHubQueueOptions = new EventHubQueueOptions
            {
                ProcessEvents = true,
                ConsumerGroup = "$Default",
                BlobContainerName = "eh-shuttle-esb",
                OperationTimeout = TimeSpan.FromSeconds(5),
                ConsumeTimeout = TimeSpan.FromSeconds(15),
                DefaultStartingPosition = EventPosition.Latest,
                CheckpointInterval = 5
            };

            configuration.GetSection($"{EventHubQueueOptions.SectionName}:azure").Bind(eventHubQueueOptions);

            eventHubQueueOptions.ConfigureProducer += (sender, _) =>
            {
                Console.WriteLine($@"[event] : ConfigureProducer / Uri = '{Guard.AgainstNull(sender as IQueue).Uri}'");
            };

            eventHubQueueOptions.ConfigureBlobStorage += (sender, _) =>
            {
                Console.WriteLine($@"[event] : ConfigureBlobStorage / Uri = '{Guard.AgainstNull(sender as IQueue).Uri}'");
            };

            eventHubQueueOptions.ConfigureProcessor += (sender, args) =>
            {
                args.Options.PrefetchCount = 100;
                
                Console.WriteLine($@"[event] : ConfigureProcessor / Uri = '{Guard.AgainstNull(sender as IQueue).Uri}'");
            };

            eventHubQueueOptions.ProcessError += (_, args) =>
            {
                Console.WriteLine($@"[event] : ProcessError / message = '{args.Exception.Message}'");
            };

            builder.AddOptions("azure", eventHubQueueOptions);
        });

        return services;
    }
}