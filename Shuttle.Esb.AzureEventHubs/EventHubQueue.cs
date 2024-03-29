﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Primitives;
using Azure.Messaging.EventHubs.Processor;
using Azure.Messaging.EventHubs.Producer;
using Azure.Storage.Blobs;
using Shuttle.Core.Contract;
using Shuttle.Core.Streams;

namespace Shuttle.Esb.AzureEventHubs
{
    public class EventHubQueue : IQueue, IPurgeQueue, IDisposable
    {
        private readonly BlobContainerClient _blobContainerClient;
        private readonly CancellationToken _cancellationToken;
        private readonly EventHubQueueOptions _eventHubQueueOptions;
        private readonly object _lock = new object();
        private readonly EventProcessorClient _processorClient;
        private readonly EventHubProducerClient _producerClient;

        private readonly Queue<ReceivedMessage> _receivedMessages = new Queue<ReceivedMessage>();
        private CancellationToken _consumeCancellationToken;
        private CancellationTokenSource _consumeCancellationTokenSource;
        private bool _disposed;
        private bool _started;

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

            _blobContainerClient = new BlobContainerClient(_eventHubQueueOptions.BlobStorageConnectionString, _eventHubQueueOptions.BlobContainerName, blobClientOptions);
            _processorClient = new EventProcessorClient(_blobContainerClient, _eventHubQueueOptions.ConsumerGroup, _eventHubQueueOptions.ConnectionString, uri.QueueName, eventProcessorClientOptions);

            _processorClient.ProcessEventAsync += ProcessEventHandler;
            _processorClient.ProcessErrorAsync += ProcessErrorHandler;
            _processorClient.PartitionInitializingAsync += InitializeEventHandler;
        }

        public void Dispose()
        {
            lock (_lock)
            {
                if (_disposed)
                {
                    return;
                }

                _producerClient.DisposeAsync().AsTask().Wait(_eventHubQueueOptions.OperationTimeout);

                if (_eventHubQueueOptions.ProcessEvents)
                {
                    try
                    {
                        _processorClient.StopProcessing(CancellationToken.None);
                    }
                    catch (OperationCanceledException)
                    {
                        // ignore
                    }

                    _processorClient.PartitionInitializingAsync -= InitializeEventHandler;
                    _processorClient.ProcessEventAsync -= ProcessEventHandler;
                    _processorClient.ProcessErrorAsync -= ProcessErrorHandler;
                }

                _disposed = true;
            }
        }

        public void Purge()
        {
            if (!_eventHubQueueOptions.ProcessEvents)
            {
                return;
            }

            if (_eventHubQueueOptions.DefaultStartingPosition != EventPosition.Latest)
            {
                throw new ApplicationException(string.Format(Resources.UnsupportedPurgeException, Uri.Uri));
            }

            var checkpointStore = new BlobCheckpointStore(_blobContainerClient);

            foreach (var partitionId in _producerClient.GetPartitionIdsAsync(_cancellationToken).Result)
            {
                var partitionProperties = _producerClient.GetPartitionPropertiesAsync(partitionId, _cancellationToken).Result;

                checkpointStore.UpdateCheckpointAsync(_producerClient.FullyQualifiedNamespace, Uri.QueueName, _eventHubQueueOptions.ConsumerGroup, partitionId, partitionProperties.LastEnqueuedOffset + 1, partitionProperties.LastEnqueuedSequenceNumber + 1, _cancellationToken).Wait(_eventHubQueueOptions.OperationTimeout);
            }
        }

        public bool IsEmpty()
        {
            if (!_eventHubQueueOptions.ProcessEvents)
            {
                return true;
            }

            lock (_lock)
            {
                Buffer();

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

                        _producerClient.SendAsync(batch, _cancellationToken).Wait(_eventHubQueueOptions.OperationTimeout);
                    }
                }
                catch (OperationCanceledException)
                {
                    // ignore
                }
            }
        }

        public ReceivedMessage GetMessage()
        {
            if (!_eventHubQueueOptions.ProcessEvents)
            {
                return null;
            }

            lock (_lock)
            {
                Buffer();

                return _receivedMessages.Count > 0 && !_disposed ? _receivedMessages.Dequeue() : null;
            }
        }

        private void Buffer()
        {
            if (!_started)
            {
                try
                {
                    _processorClient.StartProcessing(_cancellationToken);
                    _started = true;
                }
                catch (OperationCanceledException)
                {
                    // ignore
                }
            }

            if (_receivedMessages.Count == 0 && _eventHubQueueOptions.ConsumeTimeout > TimeSpan.Zero)
            {
                _consumeCancellationTokenSource = new CancellationTokenSource();
                _consumeCancellationToken = _consumeCancellationTokenSource.Token;

                try
                {
                    Task.Delay(_eventHubQueueOptions.ConsumeTimeout, _consumeCancellationToken).Wait(_cancellationToken);
                }
                catch (AggregateException)
                {
                    // ignore
                }
                catch (OperationCanceledException)
                {
                    // ignore
                }
            }
        }

        public void Acknowledge(object acknowledgementToken)
        {
            Guard.AgainstNull(acknowledgementToken, nameof(acknowledgementToken));

            var args = (ProcessEventArgs)acknowledgementToken;

            try
            {
                args.UpdateCheckpointAsync(_cancellationToken).Wait(_eventHubQueueOptions.OperationTimeout);
            }
            catch (OperationCanceledException)
            {
                // ignore
            }
        }

        public void Release(object acknowledgementToken)
        {
            Guard.AgainstNull(acknowledgementToken, nameof(acknowledgementToken));

            lock (_lock)
            {
                var args = (ProcessEventArgs)acknowledgementToken;

                _receivedMessages.Enqueue(new ReceivedMessage(new MemoryStream(Convert.FromBase64String(Encoding.UTF8.GetString(args.Data.Body.ToArray()))), args));
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

        private Task ProcessErrorHandler(ProcessErrorEventArgs args)
        {
            _eventHubQueueOptions.OnProcessError(this, args);

            return Task.CompletedTask;
        }

        private Task ProcessEventHandler(ProcessEventArgs args)
        {
            if (args.HasEvent)
            {
                _consumeCancellationTokenSource?.Cancel();

                _receivedMessages.Enqueue(new ReceivedMessage(new MemoryStream(Convert.FromBase64String(Encoding.UTF8.GetString(args.Data.Body.ToArray()))), args));
            }

            return Task.CompletedTask;
        }
    }
}