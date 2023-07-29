﻿using System;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Shuttle.Core.Pipelines;
using Shuttle.Esb.Logging;

namespace Shuttle.Esb.AzureEventHubs.Tests
{
    public class AzureFixture
    {
        public static IServiceCollection GetServiceCollection()
        {
            var services = new ServiceCollection();

            var configuration = new ConfigurationBuilder().AddUserSecrets<AzureFixture>().Build();

            services.AddSingleton<IConfiguration>(configuration);

            services.AddAzureEventHubs(builder =>
            {
                var eventHubQueueOptions = new EventHubQueueOptions();

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
}