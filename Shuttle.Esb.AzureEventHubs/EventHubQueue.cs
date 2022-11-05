using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Processor;
using Azure.Messaging.EventHubs.Producer;
using Azure.Storage.Blobs;
using Shuttle.Core.Contract;
using Shuttle.Core.Reflection;
using Shuttle.Core.Streams;

namespace Shuttle.Esb.AzureEventHubs
{
    public class EventHubQueue : IQueue, IDisposable
    {
        private readonly CancellationToken _cancellationToken;
        private readonly EventHubQueueOptions _eventHubQueueOptions;
        private readonly object _lock = new object();
        private readonly EventProcessorClient _processorClient;
        private readonly EventHubProducerClient _producerClient;

        private readonly Queue<ReceivedMessage> _receivedMessages = new Queue<ReceivedMessage>();
        private bool _disposed;

        public EventHubQueue(QueueUri uri, EventHubQueueOptions eventHubQueueOptions, CancellationToken cancellationToken)
        {
            Guard.AgainstNull(uri, nameof(uri));
            Guard.AgainstNull(eventHubQueueOptions, nameof(eventHubQueueOptions));

            _cancellationToken = cancellationToken;

            Uri = uri;

            _eventHubQueueOptions = eventHubQueueOptions;

            var eventHubProducerClientOptions = new EventHubProducerClientOptions();

            _eventHubQueueOptions.OnConfigureProducer(this, new ConfigureEventArgs<EventHubProducerClientOptions>(eventHubProducerClientOptions));

            _producerClient = new EventHubProducerClient(_eventHubQueueOptions.ConnectionString, Uri.QueueName, eventHubProducerClientOptions);

            if (!_eventHubQueueOptions.ProcessEvents)
            {
                return;
            }

            var blobClientOptions = new BlobClientOptions();
            var eventProcessorClientOptions = new EventProcessorClientOptions();

            _eventHubQueueOptions.OnConfigureBlobStorage(this, new ConfigureEventArgs<BlobClientOptions>(blobClientOptions));
            _eventHubQueueOptions.OnConfigureProcessor(this, new ConfigureEventArgs<EventProcessorClientOptions>(eventProcessorClientOptions));

            _processorClient = new EventProcessorClient(new BlobContainerClient(_eventHubQueueOptions.BlobStorageConnectionString, _eventHubQueueOptions.BlobContainerName, blobClientOptions), _eventHubQueueOptions.ConsumerGroup, _eventHubQueueOptions.ConnectionString, uri.QueueName, eventProcessorClientOptions);

            _processorClient.ProcessEventAsync += ProcessEventHandler;
            _processorClient.PartitionInitializingAsync += InitializeEventHandler;

            _processorClient.StartProcessing(_cancellationToken);
        }

        public void Dispose()
        {
            _producerClient.DisposeAsync().AsTask().Wait(_eventHubQueueOptions.OperationTimeout);

            if (_eventHubQueueOptions.ProcessEvents)
            {
                try
                {
                    _processorClient.StopProcessing(_cancellationToken);
                }
                catch (TaskCanceledException)
                {
                    // ignore
                }

                _processorClient.PartitionInitializingAsync -= InitializeEventHandler;
                _processorClient.ProcessEventAsync -= ProcessEventHandler;

                _processorClient.ProcessEventAsync -= ProcessEventHandler;
            }

            _disposed = true;
        }

        public bool IsEmpty()
        {
            lock (_lock)
            {
                return _receivedMessages.Count == 0;
            }
        }

        public void Enqueue(TransportMessage message, Stream stream)
        {
            Guard.AgainstNull(message, nameof(message));
            Guard.AgainstNull(stream, nameof(stream));

            lock (_lock)
            {
                if (_disposed)
                {
                    return;
                }

                try
                {
                    using (var batch = _producerClient.CreateBatchAsync(_cancellationToken).Result)
                    {
                        batch.TryAdd(new EventData(Convert.ToBase64String(stream.ToBytes())));

                        _producerClient.SendAsync(batch, _cancellationToken).Wait(_cancellationToken);
                    }
                }
                catch (TaskCanceledException)
                {
                    // ignore
                }
            }
        }

        public ReceivedMessage GetMessage()
        {
            lock (_lock)
            {
                if (_disposed)
                {
                    return null;
                }

                return _receivedMessages.Count > 0 ? _receivedMessages.Dequeue() : null;
            }
        }

        public void Acknowledge(object acknowledgementToken)
        {
            Guard.AgainstNull(acknowledgementToken, nameof(acknowledgementToken));

            var args = (ProcessEventArgs)acknowledgementToken;

            try
            {
                args.UpdateCheckpointAsync(_cancellationToken).Wait(_cancellationToken);
            }
            catch (TaskCanceledException)
            {
                // ignore
            }
        }

        public void Release(object acknowledgementToken)
        {
            lock (_lock)
            {
                if (_disposed)
                {
                    return;
                }

                var args = (ProcessEventArgs)acknowledgementToken;

                _receivedMessages.Enqueue(new ReceivedMessage(new MemoryStream(args.Data.EventBody.ToArray()), args));
            }
        }

        public QueueUri Uri { get; }
        public bool IsStream { get; } = true;

        private Task InitializeEventHandler(PartitionInitializingEventArgs args)
        {
            if (args.CancellationToken.IsCancellationRequested)
            {
                return Task.CompletedTask;
            }

            args.DefaultStartingPosition = _eventHubQueueOptions.DefaultStartingPosition;

            return Task.CompletedTask;
        }

        private Task ProcessEventHandler(ProcessEventArgs args)
        {
            if (args.HasEvent)
            {
                _receivedMessages.Enqueue(new ReceivedMessage(new MemoryStream(args.Data.EventBody.ToArray()), args));
            }

            return Task.CompletedTask;
        }
    }
}