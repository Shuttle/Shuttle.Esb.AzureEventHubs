# Fixture configuration

You will need to create an Event Hubs [namespace](https://portal.azure.com/#view/HubsExtension/BrowseResource/resourceType/Microsoft.EventHub%2Fnamespaces) with a [shared access policy](https://portal.azure.com/#@bscglobal.com/resource/subscriptions/020717a5-b2a3-4630-915f-ca09a8df7c75/resourceGroups/rg-shuttle/providers/Microsoft.EventHub/namespaces/shuttle/saskey)] with `Send` and `Listen` claims.

The fixtures will need the following event hubs to be preent:

- `test-error`
- `test-inbox-work`

In addition, you need to create a `Container` with name `eh-shuttle-esb` in a `Storage Account` to store the checkpoints.

Right-click on the `Shuttle.Esb.AzureEventHubs.Tests` project and select `Manage User Secrets` and add the following configuration:

```json
{
    "Shuttle": {
        "AzureEventHubs": {
          "azure": {
            "ConnectionString": "displayed when you click on the shared access policy",
            "BlobStorageConnectionString": ""
          }
        }
    }
}
```