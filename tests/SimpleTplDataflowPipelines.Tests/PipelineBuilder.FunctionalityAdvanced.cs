using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace SimpleTplDataflowPipelines.Tests
{
    public partial class PipelineBuilderTests
    {
        [TestMethod]
        public async Task PostCompletionAction()
        {
            long[] timestamps = new long[6];

            var pipeline = PipelineBuilder
                .BeginWith(new TransformBlock<int, long>(_ => timestamps[0] = Stopwatch.GetTimestamp()))
                .WithPostCompletionAction(_ => timestamps[1] = Stopwatch.GetTimestamp())
                .LinkTo(new TransformBlock<long, long>(_ => timestamps[2] = Stopwatch.GetTimestamp()))
                .WithPostCompletionAction(_ => timestamps[3] = Stopwatch.GetTimestamp())
                .LinkTo(new ActionBlock<long>(_ => timestamps[4] = Stopwatch.GetTimestamp()))
                .WithPostCompletionAction(_ => timestamps[5] = Stopwatch.GetTimestamp())
                .ToPipeline();

            pipeline.Post(0);
            pipeline.Complete();
            await pipeline.Completion.WithTimeout(1000);
            Console.WriteLine(String.Join(", ", timestamps));
            Assert.IsTrue(timestamps.Zip(timestamps.Skip(1)).All(e => e.First <= e.Second));
        }
    }
}
