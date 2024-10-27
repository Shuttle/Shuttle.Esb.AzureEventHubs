using System;
using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;
using Shuttle.Core.Threading;

namespace Shuttle.Esb.AzureEventHubs;

public class EventHubQueueFactory : IQueueFactory
{
    private readonly ICancellationTokenSource _cancellationTokenSource;
    private readonly IOptionsMonitor<EventHubQueueOptions> _eventHubQueueOptions;

    public EventHubQueueFactory(IOptionsMonitor<EventHubQueueOptions> eventHubQueueOptions, ICancellationTokenSource cancellationTokenSource)
    {
        _eventHubQueueOptions = Guard.AgainstNull(eventHubQueueOptions);
        _cancellationTokenSource = Guard.AgainstNull(cancellationTokenSource);
    }

    public string Scheme => "azureeh";

    public IQueue Create(Uri uri)
    {
        var queueUri = new QueueUri(Guard.AgainstNull(uri)).SchemeInvariant(Scheme);
        var eventHubQueueOptions = _eventHubQueueOptions.Get(queueUri.ConfigurationName);

        if (eventHubQueueOptions == null)
        {
            throw new InvalidOperationException(string.Format(Esb.Resources.QueueConfigurationNameException, queueUri.ConfigurationName));
        }

        return new EventHubQueue(queueUri, eventHubQueueOptions, _cancellationTokenSource.Get().Token);
    }
}