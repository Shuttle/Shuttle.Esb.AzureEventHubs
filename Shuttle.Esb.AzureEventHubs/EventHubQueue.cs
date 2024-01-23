using System;
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
        private readonly OperationEventArgs _bufferOperationStartingEventArgs = new OperationEventArgs("[buffer/starting]");
        private readonly OperationEventArgs _bufferOperationCompletedEventArgs = new OperationEventArgs("[buffer/starting]");
        private readonly OperationEventArgs _processEventHandlerOperationMessageReceivedEventArgs = new OperationEventArgs("[process-event-handler/message-received]");
        private readonly OperationEventArgs _processEventHandlerOperationNoMessageReceivedEventArgs = new OperationEventArgs("[process-event-handler/no-message-received]");
        private readonly OperationEventArgs _acknowledgeStartingEventArgs = new OperationEventArgs("[acknowledge/starting]");
        private readonly SemaphoreSlim _lock = new SemaphoreSlim(1, 1);

        private readonly BlobContainerClient _blobContainerClient;
        private readonly CancellationToken _cancellationToken;
        private readonly EventHubQueueOptions _eventHubQueueOptions;
        private readonly EventProcessorClient _processorClient;
        private readonly EventHubProducerClient _producerClient;

        private readonly Queue<ReceivedMessage> _receivedMessages = new Queue<ReceivedMessage>();
        private bool _disposed;
        private bool _started;

        public event EventHandler<MessageEnqueuedEventArgs> MessageEnqueued;
        public event EventHandler<MessageAcknowledgedEventArgs> MessageAcknowledged;
        public event EventHandler<MessageReleasedEventArgs> MessageReleased;
        public event EventHandler<MessageReceivedEventArgs> MessageReceived;
        public event EventHandler<OperationEventArgs> Operation;

        private int _checkpointItem = 1;
        private ProcessEventArgs? _acknowledgeProcessEventArgs;

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

                _acknowledgeProcessEventArgs?.UpdateCheckpointAsync(CancellationToken.None).GetAwaiter().GetResult();

                if (_eventHubQueueOptions.ProcessEvents)
                {
                    try
                    {
                        Operation?.Invoke(this, new OperationEventArgs("[dispose/stop-processing/starting]"));

                        _processorClient.StopProcessing(CancellationToken.None);
                    }
                    catch (OperationCanceledException)
                    {
                        // ignore - shouldn't happen
                    }
                    finally
                    {
                        Operation?.Invoke(this, new OperationEventArgs("[dispose/stop-processing/completed]"));
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

        public bool IsEmpty()
        {
            return IsEmptyAsync().GetAwaiter().GetResult();
        }

        public async ValueTask<bool> IsEmptyAsync()
        {
            if (_cancellationToken.IsCancellationRequested)
            {
                Operation?.Invoke(this, new OperationEventArgs("[is-empty/cancelled]", true));
                return true;
            }

            if (!_eventHubQueueOptions.ProcessEvents)
            {
                Operation?.Invoke(this, new OperationEventArgs("[is-empty]", true));

                return true;
            }

            Operation?.Invoke(this, new OperationEventArgs("[is-empty/starting]"));

            await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

            try
            {
                await BufferAsync();

                var result = _receivedMessages.Count == 0;

                Operation?.Invoke(this, new OperationEventArgs("[is-empty]", result));

                return result;
            }
            finally
            {
                _lock.Release();
            }
        }

        public void Enqueue(TransportMessage transportMessage, Stream stream)
        {
            EnqueueAsync(transportMessage, stream).GetAwaiter().GetResult();
        }

        public void Purge()
        {
            PurgeAsync().GetAwaiter().GetResult();
        }

        public async Task PurgeAsync()
        {
            if (_cancellationToken.IsCancellationRequested)
            {
                Operation?.Invoke(this, new OperationEventArgs("[purge/cancelled]"));
                return;
            }

            Operation?.Invoke(this, new OperationEventArgs("[purge/starting]"));

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
            }
            catch (OperationCanceledException)
            {
                Operation?.Invoke(this, new OperationEventArgs("[purge/cancelled]"));
            }
            finally
            {
                _lock.Release();
            }

            Operation?.Invoke(this, new OperationEventArgs("[purge/completed]"));
        }

        public async Task EnqueueAsync(TransportMessage message, Stream stream)
        {
            Guard.AgainstNull(message, nameof(message));
            Guard.AgainstNull(stream, nameof(stream));

            if (_cancellationToken.IsCancellationRequested)
            {
                Operation?.Invoke(this, new OperationEventArgs("[enqueue/cancelled]"));
                return;
            }

            await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

            if (_disposed)
            {
                return;
            }

            try
            {
                using (var batch = await _producerClient.CreateBatchAsync(_cancellationToken))
                {
                    batch.TryAdd(new EventData(Convert.ToBase64String(await stream.ToBytesAsync().ConfigureAwait(false))));

                    await _producerClient.SendAsync(batch, _cancellationToken).ConfigureAwait(false);

                    MessageEnqueued?.Invoke(this, new MessageEnqueuedEventArgs(message, stream));
                }
            }
            catch (OperationCanceledException)
            {
                Operation?.Invoke(this, new OperationEventArgs("[enqueue/cancelled]"));
            }
            finally
            {
                _lock.Release();
            }
        }

        public ReceivedMessage GetMessage()
        {
            return GetMessageAsync().GetAwaiter().GetResult();
        }

        public async Task<ReceivedMessage> GetMessageAsync()
        {
            if (_cancellationToken.IsCancellationRequested)
            {
                Operation?.Invoke(this, new OperationEventArgs("[get-message/cancelled]"));
                return null;
            }

            if (!_eventHubQueueOptions.ProcessEvents)
            {
                return null;
            }

            await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

            try
            {
                await BufferAsync();

                var receivedMessage = _receivedMessages.Count > 0 && !_disposed ? _receivedMessages.Dequeue() : null;

                if (receivedMessage != null)
                {
                    MessageReceived?.Invoke(this, new MessageReceivedEventArgs(receivedMessage));
                }

                return receivedMessage;
            }
            finally
            {
                _lock.Release();
            }
        }

        public void Acknowledge(object acknowledgementToken)
        {
            AcknowledgeAsync(acknowledgementToken).GetAwaiter().GetResult();
        }

        private async Task BufferAsync()
        {
            if (!_started)
            {
                try
                {
                    Operation?.Invoke(this, new OperationEventArgs("[start-processing/starting]"));

                    await _processorClient.StartProcessingAsync(_cancellationToken);
                    _started = true;
                }
                catch (OperationCanceledException)
                {
                    Operation?.Invoke(this, new OperationEventArgs("[start-processing/cancelled]"));
                }
                finally
                {
                    Operation?.Invoke(this, new OperationEventArgs("[start-processing/completed]"));
                }
            }

            if (_eventHubQueueOptions.ConsumeTimeout <= TimeSpan.Zero)
            {
                return;
            }

            Operation?.Invoke(this, _bufferOperationStartingEventArgs);

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

            Operation?.Invoke(this, _bufferOperationCompletedEventArgs);
        }

        public async Task AcknowledgeAsync(object acknowledgementToken)
        {
            Guard.AgainstNull(acknowledgementToken, nameof(acknowledgementToken));

            if (!(acknowledgementToken is ProcessEventArgs args))
            {
                return;
            }

            if (_cancellationToken.IsCancellationRequested)
            {
                Operation?.Invoke(this, new OperationEventArgs("[acknowledge/cancelled]"));
                return;
            }

            await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

            try
            {
                if (_checkpointItem == _eventHubQueueOptions.CheckpointInterval)
                {
                    Operation?.Invoke(this, _acknowledgeStartingEventArgs);

                    await args.UpdateCheckpointAsync(_cancellationToken).ConfigureAwait(false);

                    _acknowledgeProcessEventArgs = null;
                    _checkpointItem = 1;

                    MessageAcknowledged?.Invoke(this, new MessageAcknowledgedEventArgs(acknowledgementToken));
                }
                else
                {
                    _acknowledgeProcessEventArgs = args;
                    _checkpointItem++;
                }
            }
            catch (OperationCanceledException)
            {
                Operation?.Invoke(this, new OperationEventArgs("[acknowledge/cancelled]"));
            }
            finally
            {
                _lock.Release();
            }
        }

        public void Release(object acknowledgementToken)
        {
            ReleaseAsync(acknowledgementToken).GetAwaiter().GetResult();
        }

        public async Task ReleaseAsync(object acknowledgementToken)
        {
            Guard.AgainstNull(acknowledgementToken, nameof(acknowledgementToken));

            if (!(acknowledgementToken is ProcessEventArgs args))
            {
                return;
            }

            if (_cancellationToken.IsCancellationRequested)
            {
                Operation?.Invoke(this, new OperationEventArgs("[release/cancelled]"));
                return;
            }

            
            await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

            try
            {
                _receivedMessages.Enqueue(new ReceivedMessage(new MemoryStream(Convert.FromBase64String(Encoding.UTF8.GetString(args.Data.Body.ToArray()))), args));

                MessageReleased?.Invoke(this, new MessageReleasedEventArgs(acknowledgementToken));
            }
            finally
            {
                _lock.Release();
            }
        }

        public QueueUri Uri { get; }
        public bool IsStream { get; } = true;

        private async Task InitializeEventHandler(PartitionInitializingEventArgs args)
        {
            if (args.CancellationToken.IsCancellationRequested)
            {
                return;
            }

            args.DefaultStartingPosition = _eventHubQueueOptions.DefaultStartingPosition;

            await Task.CompletedTask;
        }

        private async Task ProcessErrorHandler(ProcessErrorEventArgs args)
        {
            _eventHubQueueOptions.OnProcessError(this, args);

            await Task.CompletedTask;
        }

        private async Task ProcessEventHandler(ProcessEventArgs args)
        {
            if (args.HasEvent)
            {
                _receivedMessages.Enqueue(new ReceivedMessage(new MemoryStream(Convert.FromBase64String(Encoding.UTF8.GetString(args.Data.Body.ToArray()))), args));
                Operation?.Invoke(this, _processEventHandlerOperationMessageReceivedEventArgs);
            }
            else
            {
                Operation?.Invoke(this, _processEventHandlerOperationNoMessageReceivedEventArgs);
            }

            await Task.CompletedTask;
        }
    }
}