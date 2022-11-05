using System;
using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;
using Shuttle.Core.Threading;

namespace Shuttle.Esb.AzureEventHubs
{
    public class EventHubQueueFactory : IQueueFactory
    {
        private readonly IOptionsMonitor<EventHubQueueOptions> _eventHubQueueOptions;
        private readonly ICancellationTokenSource _cancellationTokenSource;
        public string Scheme => "azureeh";

        public EventHubQueueFactory(IOptionsMonitor<EventHubQueueOptions> eventHubQueueOptions, ICancellationTokenSource cancellationTokenSource)
        {
            Guard.AgainstNull(eventHubQueueOptions, nameof(eventHubQueueOptions));
            Guard.AgainstNull(cancellationTokenSource, nameof(cancellationTokenSource));

            _eventHubQueueOptions = eventHubQueueOptions;
            _cancellationTokenSource = cancellationTokenSource;
        }

        public IQueue Create(Uri uri)
        {
            Guard.AgainstNull(uri, "uri");

            var queueUri = new QueueUri(uri).SchemeInvariant(Scheme);
            var eventHubQueueOptions = _eventHubQueueOptions.Get(queueUri.ConfigurationName);

            if (eventHubQueueOptions == null)
            {
                throw new InvalidOperationException(string.Format(Esb.Resources.QueueConfigurationNameException, queueUri.ConfigurationName));
            }

            return new EventHubQueue(queueUri, eventHubQueueOptions, _cancellationTokenSource.Get().Token);
        }
    }
}