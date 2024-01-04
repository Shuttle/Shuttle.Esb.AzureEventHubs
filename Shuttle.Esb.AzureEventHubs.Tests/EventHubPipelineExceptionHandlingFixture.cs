using NUnit.Framework;
using Shuttle.Esb.Tests;
using System.Threading.Tasks;

namespace Shuttle.Esb.AzureEventHubs.Tests
{
    public class EventHubPipelineExceptionHandlingFixture : PipelineExceptionFixture
    {
        [Test]
        public void Should_be_able_to_handle_exceptions_in_receive_stage_of_receive_pipeline()
        {
            TestExceptionHandling(AzureEventHubsConfiguration.GetServiceCollection(), "azureeh://azure/{0}");
        }

        [Test]
        public async Task Should_be_able_to_handle_exceptions_in_receive_stage_of_receive_pipeline_async()
        {
            await TestExceptionHandlingAsync(AzureEventHubsConfiguration.GetServiceCollection(), "azureeh://azure/{0}");
        }
    }
}