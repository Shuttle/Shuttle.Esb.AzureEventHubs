using NUnit.Framework;
using Shuttle.Esb.Tests;
using System.Threading.Tasks;

namespace Shuttle.Esb.AzureEventHubs.Tests
{
    public class EventHubPipelineExceptionHandlingFixture : PipelineExceptionFixture
    {
        [Test]
        public async Task Should_be_able_to_handle_exceptions_in_receive_stage_of_receive_pipeline()
        {
            await TestExceptionHandling(AzureFixture.GetServiceCollection(), "azureeh://azure/{0}");
        }
    }
}