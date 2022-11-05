using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.AzureEventHubs
{
    public class EventHubQueueBuilder
    {
        internal readonly Dictionary<string, EventHubQueueOptions> EventHubQueueOptions = new Dictionary<string, EventHubQueueOptions>();
        public IServiceCollection Services { get; }

        public EventHubQueueBuilder(IServiceCollection services)
        {
            Guard.AgainstNull(services, nameof(services));
            
            Services = services;
        }

        public EventHubQueueBuilder AddOptions(string name, EventHubQueueOptions eventHubQueueOptions)
        {
            Guard.AgainstNullOrEmptyString(name, nameof(name));
            Guard.AgainstNull(eventHubQueueOptions, nameof(eventHubQueueOptions));

            EventHubQueueOptions.Remove(name);

            EventHubQueueOptions.Add(name, eventHubQueueOptions);

            return this;
        }
    }
}