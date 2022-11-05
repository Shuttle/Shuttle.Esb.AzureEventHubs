using NUnit.Framework;
using Shuttle.Esb.Tests;

namespace Shuttle.Esb.AzureEventHubs.Tests
{
    public class EventHubOutboxFixture : OutboxFixture
    {
        [TestCase(true)]
        [TestCase(false)]
        public void Should_be_able_to_use_outbox(bool isTransactionalEndpoint)
        {
            TestOutboxSending(AzureFixture.GetServiceCollection(), "azureeh://azure/{0}", 1, isTransactionalEndpoint);
        }
    }
}