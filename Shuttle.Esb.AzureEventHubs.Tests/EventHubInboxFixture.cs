using NUnit.Framework;
using Shuttle.Esb.Tests;
using System.Threading.Tasks;

namespace Shuttle.Esb.AzureEventHubs.Tests
{
    public class EventHubInboxFixture : InboxFixture
    {
        [TestCase(true, true)]
        [TestCase(true, false)]
        [TestCase(false, true)]
        [TestCase(false, false)]
        public void Should_be_able_handle_errors(bool hasErrorQueue, bool isTransactionalEndpoint)
        {
            TestInboxError(AzureEventHubsConfiguration.GetServiceCollection(), "azureeh://azure/{0}", hasErrorQueue, isTransactionalEndpoint);
        }

        [TestCase(true, true)]
        [TestCase(true, false)]
        [TestCase(false, true)]
        [TestCase(false, false)]
        public async Task Should_be_able_handle_errors_async(bool hasErrorQueue, bool isTransactionalEndpoint)
        {
            await TestInboxErrorAsync(AzureEventHubsConfiguration.GetServiceCollection(), "azureeh://azure/{0}", hasErrorQueue, isTransactionalEndpoint);
        }

        [TestCase(100, true)]
        [TestCase(100, false)]
        public void Should_be_able_to_process_queue_timeously(int count, bool isTransactionalEndpoint)
        {
            TestInboxThroughput(AzureEventHubsConfiguration.GetServiceCollection(), "azureeh://azure/{0}", 1000, count, 1, isTransactionalEndpoint);
        }

        [TestCase(100, true)]
        [TestCase(100, false)]
        public async Task Should_be_able_to_process_queue_timeously_async(int count, bool isTransactionalEndpoint)
        {
            await TestInboxThroughputAsync(AzureEventHubsConfiguration.GetServiceCollection(), "azureeh://azure/{0}", 1000, count, 1, isTransactionalEndpoint);
        }
    }
}