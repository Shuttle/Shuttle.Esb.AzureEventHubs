using NUnit.Framework;
using Shuttle.Esb.Tests;

namespace Shuttle.Esb.AzureEventHubs.Tests
{
    public class EventHubDistributorFixture : DistributorFixture
    {
        [Test]
        [TestCase(false)]
        [TestCase(true)]
        public void Should_be_able_to_distribute_messages(bool isTransactionalEndpoint)
        {
            TestDistributor(AzureFixture.GetServiceCollection(), 
                AzureFixture.GetServiceCollection(), @"azureeh://azure/{0}", isTransactionalEndpoint, 30);
        }
    }
}