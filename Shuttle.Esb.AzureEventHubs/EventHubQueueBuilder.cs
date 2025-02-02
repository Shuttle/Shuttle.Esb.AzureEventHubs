using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.AzureEventHubs;

public class EventHubQueueBuilder
{
    internal readonly Dictionary<string, EventHubQueueOptions> EventHubQueueOptions = new();

    public EventHubQueueBuilder(IServiceCollection services)
    {
        Services = Guard.AgainstNull(services);
    }

    public IServiceCollection Services { get; }

    public EventHubQueueBuilder AddOptions(string name, EventHubQueueOptions eventHubQueueOptions)
    {
        Guard.AgainstNullOrEmptyString(name);
        Guard.AgainstNull(eventHubQueueOptions);

        EventHubQueueOptions.Remove(name);

        EventHubQueueOptions.Add(name, eventHubQueueOptions);

        return this;
    }
}