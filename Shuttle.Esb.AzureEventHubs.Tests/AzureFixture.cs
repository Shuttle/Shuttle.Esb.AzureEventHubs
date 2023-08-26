using System;
using Azure.Messaging.EventHubs.Consumer;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Shuttle.Esb.AzureEventHubs.Tests;

public class AzureFixture
{
    public static IServiceCollection GetServiceCollection()
    {
        var services = new ServiceCollection();

        var configuration = new ConfigurationBuilder().AddUserSecrets<AzureFixture>().Build();

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
                ConsumeTimeout = TimeSpan.FromSeconds(5),
                DefaultStartingPosition = EventPosition.Latest
            };

            configuration.GetSection($"{EventHubQueueOptions.SectionName}:azure").Bind(eventHubQueueOptions);

            eventHubQueueOptions.ConfigureProducer += (sender, args) =>
            {
                Console.WriteLine($"[event] : ConfigureProducer / Uri = '{((IQueue)sender).Uri}'");
            };

            eventHubQueueOptions.ConfigureBlobStorage += (sender, args) =>
            {
                Console.WriteLine($"[event] : ConfigureBlobStorage / Uri = '{((IQueue)sender).Uri}'");
            };

            eventHubQueueOptions.ConfigureProcessor += (sender, args) =>
            {
                Console.WriteLine($"[event] : ConfigureProcessor / Uri = '{((IQueue)sender).Uri}'");
            };

            eventHubQueueOptions.ProcessError += (sender, args) =>
            {
                Console.WriteLine($"[event] : ProcessError / message = '{args.Exception.Message}'");
            };

            builder.AddOptions("azure", eventHubQueueOptions);
        });

        return services;
    }
}