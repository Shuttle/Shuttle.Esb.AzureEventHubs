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
        public async Task Should_be_able_handle_errors(bool hasErrorQueue, bool isTransactionalEndpoint)
        {
            await TestInboxError(AzureFixture.GetServiceCollection(), "azureeh://azure/{0}", hasErrorQueue, isTransactionalEndpoint);
        }

        [TestCase(250, false)]
        [TestCase(250, true)]
        public async Task Should_be_able_to_process_messages_concurrently(int msToComplete, bool isTransactionalEndpoint)
        {
            await TestInboxConcurrency(AzureFixture.GetServiceCollection(), "azureeh://azure/{0}", msToComplete, isTransactionalEndpoint);
        }

        [TestCase(100, true)]
        [TestCase(100, false)]
        public async Task Should_be_able_to_process_queue_timeously(int count, bool isTransactionalEndpoint)
        {
            await TestInboxThroughput(AzureFixture.GetServiceCollection(), "azureeh://azure/{0}", 1000, count, 1, isTransactionalEndpoint);
        }
    }
}