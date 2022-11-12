# AzureEventHubs

```
PM> Install-Package Shuttle.Esb.AzureEventHubs
```

## Configuration

The URI structure is `azureeh://configuration-name/queue-name`.

```c#
services.AddAzureEventHubs(builder =>
{
    var eventHubQueueOptions = new EventHubQueueOptions
    {
        ConnectionString = "UseDevelopmentStorage=true",
        ProcessEvents = false,
        ConsumerGroup = EventHubConsumerClient.DefaultConsumerGroupName,
        BlobStorageConnectionString = "{BlobStorageConnectionString}",
        BlobContainerName = "{BlobContainerName}",
        OperationTimeout = TimeSpan.FromSeconds(30),
        ConsumeTimeout = TimeSpan.FromSeconds(30),
        DefaultStartingPosition = EventPosition.Latest
    };

    eventHubQueueOptions.ConfigureProducer += (sender, args) =>
    {
        Console.WriteLine($"[event] : ConfigureProducer / Uri = '{((IQueue)sender).Uri}'");
    };

    eventHubQueueOptions.ConfigureBlobStorage += (sender, args) =>
    {
        Console.WriteLine($"[event] : ConfigureBlobStorage / Uri = '{((IQueue)sender).Uri}'");
    };

    eventHubQueueOptions.ConfigureConsumer += (sender, args) =>
    {
        Console.WriteLine($"[event] : ConfigureConsumer / Uri = '{((IQueue)sender).Uri}'");
    };

    builder.AddOptions("azure", eventHubQueueOptions);
});
```

In the `Configure` events the `args` arugment exposes the relevant client options directly should you need to set an values explicitly.

The default JSON settings structure is as follows:

```json
{
  "Shuttle": {
    "AzureEventHubs": {
      "azure": {
        "ConnectionString": "UseDevelopmentStorage=true",
        "ProcessEvents": false,
        "ConsumerGroup": "$Default",
        "BlobStorageConnectionString": "{BlobStorageConnectionString}",
        "BlobContainerName": "{BlobContainerName}",
        "OperationTimeout": "00:00:30",
        "ConsumeTimeout": "00:00:30",
        "DefaultStartingPosition": "Latest"
      }
    }
  }
}
```

## Options

| Segment / Argument | Default | Description |
| --- | --- | --- | 
| `ConnectionString` | | The Azure Event Hubs endpoint to connect to. |
| `ProcessEvents` | `false` | Indicates whether the endpoint will process messages.  If `true`, an `EventProcessorClient` is instanced and configured. |
| `ConsumerGroup` | "$Default" | The consumer group to use when processing events. |
| `BlobStorageConnectionString` | | The Azure Storage Account endpoint to connect to in order to perform checkpoints. |
| `BlobContainerName` | | The blob container name where checkpoints will be stored. |
| `OperationTimeout` | "00:00:30" | The duration to wait for relevant `async` methods to complete before timing out. |
| `ConsumeTimeout` | "00:00:30" | The duration to poll for messages before returning `null`. |
| `DefaultStartingPosition` | | The default starting position to use when no checkpoint exists. |

