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
        private readonly OperationEventArgs _bufferOperationEventArgs = new OperationEventArgs("Buffer");
        private readonly OperationEventArgs _processEventHandlerOperationEventArgs = new OperationEventArgs("ProcessEventHandler");
        private readonly SemaphoreSlim _lock = new SemaphoreSlim(1, 1);

        private readonly BlobContainerClient _blobContainerClient;
        private readonly CancellationToken _cancellationToken;
        private readonly EventHubQueueOptions _eventHubQueueOptions;
        private readonly EventProcessorClient _processorClient;
        private readonly EventHubProducerClient _producerClient;

        private readonly Queue<ReceivedMessage> _receivedMessages = new Queue<ReceivedMessage>();
        private bool _disposed;
        private bool _started;

        public event EventHandler<MessageEnqueuedEventArgs> MessageEnqueued = delegate
        {
        };

        public event EventHandler<MessageAcknowledgedEventArgs> MessageAcknowledged = delegate
        {
        };

        public event EventHandler<MessageReleasedEventArgs> MessageReleased = delegate
        {
        };

        public event EventHandler<MessageReceivedEventArgs> MessageReceived = delegate
        {
        };

        public event EventHandler<OperationEventArgs> OperationStarting = delegate
        {
        };

        public event EventHandler<OperationEventArgs> OperationCompleted = delegate
        {
        };

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
            _lock.Wait(CancellationToken.None);

            try
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
                        OperationStarting.Invoke(this, new OperationEventArgs("StopProcessing"));

                        _processorClient.StopProcessing(CancellationToken.None);
                    }
                    catch (OperationCanceledException)
                    {
                        // ignore
                    }
                    finally
                    {
                        OperationCompleted.Invoke(this, new OperationEventArgs("StopProcessing"));
                    }

                    _processorClient.PartitionInitializingAsync -= InitializeEventHandler;
                    _processorClient.ProcessEventAsync -= ProcessEventHandler;
                    _processorClient.ProcessErrorAsync -= ProcessErrorHandler;
                }

                _disposed = true;
            }
            finally
            {
                _lock.Release();
            }
        }

        public async ValueTask<bool> IsEmpty()
        {
            OperationStarting.Invoke(this, new OperationEventArgs("IsEmpty"));

            if (!_eventHubQueueOptions.ProcessEvents)
            {
                OperationCompleted.Invoke(this, new OperationEventArgs("IsEmpty", true));
                
                return true;
            }

            await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

            try
            {
                await Buffer();

                var result = _receivedMessages.Count == 0;

                OperationCompleted.Invoke(this, new OperationEventArgs("IsEmpty", result));

                return result;
            }
            finally
            {
                _lock.Release();
            }
        }

        public async Task Purge()
        {
            OperationStarting.Invoke(this, new OperationEventArgs("Purge"));

            await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

            try
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

                    await checkpointStore.UpdateCheckpointAsync(_producerClient.FullyQualifiedNamespace, Uri.QueueName, _eventHubQueueOptions.ConsumerGroup, partitionId, partitionProperties.LastEnqueuedOffset + 1, partitionProperties.LastEnqueuedSequenceNumber + 1, _cancellationToken).ConfigureAwait(false);
                }

                OperationCompleted.Invoke(this, new OperationEventArgs("Purge"));
            }
            finally
            {
                _lock.Release();
            }
        }

        public async Task Enqueue(TransportMessage message, Stream stream)
        {
            Guard.AgainstNull(message, nameof(message));
            Guard.AgainstNull(stream, nameof(stream));

            await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

            try
            {
                if (_disposed)
                {
                    return;
                }

                try
                {
                    using (var batch = _producerClient.CreateBatchAsync(_cancellationToken).Result)
                    {
                        batch.TryAdd(new EventData(Convert.ToBase64String(await stream.ToBytesAsync().ConfigureAwait(false))));

                        await _producerClient.SendAsync(batch, _cancellationToken).ConfigureAwait(false);

                        MessageEnqueued.Invoke(this, new MessageEnqueuedEventArgs(message, stream));
                    }
                }
                catch (OperationCanceledException)
                {
                    // ignore
                }
            }
            finally
            {
                _lock.Release();
            }
        }

        public async Task<ReceivedMessage> GetMessage()
        {
            if (!_eventHubQueueOptions.ProcessEvents)
            {
                return null;
            }

            await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

            try
            {
                await Buffer();

                var receivedMessage = _receivedMessages.Count > 0 && !_disposed ? _receivedMessages.Dequeue() : null;

                if (receivedMessage != null)
                {
                    MessageReceived.Invoke(this, new MessageReceivedEventArgs(receivedMessage));
                }

                return receivedMessage;
            }
            finally
            {
                _lock.Release();
            }
        }

        private async Task Buffer()
        {
            if (!_started)
            {
                try
                {
                    OperationStarting.Invoke(this, new OperationEventArgs("StartProcessing"));

                    await _processorClient.StartProcessingAsync(_cancellationToken);
                    _started = true;
                }
                catch (OperationCanceledException)
                {
                    // ignore
                }
                finally
                {
                    OperationCompleted.Invoke(this, new OperationEventArgs("StartProcessing"));
                }
            }

            if (_eventHubQueueOptions.ConsumeTimeout <= TimeSpan.Zero)
            {
                return;
            }

            OperationStarting.Invoke(this, _bufferOperationEventArgs);

            var timeout = DateTime.Now.Add(_eventHubQueueOptions.ConsumeTimeout);

            while (_receivedMessages.Count == 0 && timeout > DateTime.Now && !_cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(250, _cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    // ignore
                }
            }

            OperationCompleted.Invoke(this, _bufferOperationEventArgs);
        }

        public async Task Acknowledge(object acknowledgementToken)
        {
            Guard.AgainstNull(acknowledgementToken, nameof(acknowledgementToken));

            var args = (ProcessEventArgs)acknowledgementToken;

            await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

            try
            {
                await args.UpdateCheckpointAsync(_cancellationToken).ConfigureAwait(false);

                MessageAcknowledged.Invoke(this, new MessageAcknowledgedEventArgs(acknowledgementToken));
            }
            catch (OperationCanceledException)
            {
                // ignore
            }
            finally
            {
                _lock.Release();
            }
        }

        public async Task Release(object acknowledgementToken)
        {
            Guard.AgainstNull(acknowledgementToken, nameof(acknowledgementToken));

            await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

            try
            {
                var args = (ProcessEventArgs)acknowledgementToken;

                _receivedMessages.Enqueue(new ReceivedMessage(new MemoryStream(Convert.FromBase64String(Encoding.UTF8.GetString(args.Data.Body.ToArray()))), args));

                MessageReleased.Invoke(this, new MessageReleasedEventArgs(acknowledgementToken));
            }
            finally
            {
                _lock.Release();
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
            OperationStarting.Invoke(this, _processEventHandlerOperationEventArgs);

            if (args.HasEvent)
            {
                _receivedMessages.Enqueue(new ReceivedMessage(new MemoryStream(Convert.FromBase64String(Encoding.UTF8.GetString(args.Data.Body.ToArray()))), args));
            }

            OperationCompleted.Invoke(this, _processEventHandlerOperationEventArgs);

            return Task.CompletedTask;
        }
    }
}