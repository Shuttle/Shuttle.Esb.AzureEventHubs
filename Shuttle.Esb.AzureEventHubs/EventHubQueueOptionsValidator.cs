using Microsoft.Extensions.Options;

namespace Shuttle.Esb.AzureEventHubs
{
    public class EventHubQueueOptionsValidator : IValidateOptions<EventHubQueueOptions>
    {
        public ValidateOptionsResult Validate(string name, EventHubQueueOptions options)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                return ValidateOptionsResult.Fail(Esb.Resources.QueueConfigurationNameException);
            }

            if (string.IsNullOrWhiteSpace(options.ConnectionString))
            {
                return ValidateOptionsResult.Fail(string.Format(Esb.Resources.QueueConfigurationItemException, name, nameof(options.ConnectionString)));
            }

            if (options.ProcessEvents)
            {
                if (string.IsNullOrWhiteSpace(options.BlobStorageConnectionString))
                {
                    return ValidateOptionsResult.Fail(string.Format(Esb.Resources.QueueConfigurationItemException, name, nameof(options.BlobStorageConnectionString)));
                }

                if (string.IsNullOrWhiteSpace(options.BlobContainerName))
                {
                    return ValidateOptionsResult.Fail(string.Format(Esb.Resources.QueueConfigurationItemException, name, nameof(options.BlobContainerName)));
                }

                if (string.IsNullOrWhiteSpace(options.ConsumerGroup))
                {
                    return ValidateOptionsResult.Fail(string.Format(Esb.Resources.QueueConfigurationItemException, name, nameof(options.ConsumerGroup)));
                }
            }

            return ValidateOptionsResult.Success;
        }
    }
}