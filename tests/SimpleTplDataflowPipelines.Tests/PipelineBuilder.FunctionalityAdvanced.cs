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
            long[] timestamps = new long[5];

            var pipeline = PipelineBuilder
                .BeginWith(new TransformBlock<int, long>(_ => timestamps[0] = Stopwatch.GetTimestamp()))
                .WithPostCompletionAction(_ => timestamps[1] = Stopwatch.GetTimestamp())
                .LinkTo(new TransformBlock<long, long>(x => x))
                .WithPostCompletionAction(_ => timestamps[2] = Stopwatch.GetTimestamp())
                .LinkTo(new ActionBlock<long>(_ => { }))
                .WithPostCompletionAction(_ => timestamps[3] = Stopwatch.GetTimestamp())
                .AddUnlinked(new TransformBlock<long, long>(x => x))
                .WithPostCompletionAction(_ => timestamps[4] = Stopwatch.GetTimestamp())
                .ToPipeline();

            pipeline.Post(0);
            pipeline.Complete();
            await pipeline.Completion.WithTimeout(1000);
            Console.WriteLine(String.Join(", ", timestamps));
            Assert.IsTrue(timestamps.Zip(timestamps.Skip(1)).All(e => e.First <= e.Second));
        }

        [TestMethod]
        public async Task UnlinkedBlocks()
        {
            var block3 = new TransformBlock<int, int>(x => x);
            var block2 = new ActionBlock<int>(async x => await block3.SendAsync(x));
            var block1 = new ActionBlock<int>(async x => await block2.SendAsync(x));

            var pipeline = PipelineBuilder
                .BeginWith(block1)
                .WithPostCompletionAction(async t => await block2.SendAsync(100))
                .AddUnlinked(block2)
                .WithPostCompletionAction(async t => await block3.SendAsync(200))
                .AddUnlinked(block3)
                .ToPipeline();

            var source = Enumerable.Range(1, 10);
            foreach (var item in source) pipeline.Post(item);
            pipeline.Complete();
            var list = await ((IReceivableSourceBlock<int>)pipeline).ToListAsync(new CancellationTokenSource(1000).Token);
            Assert.IsTrue(list.SequenceEqual(source.Append(100).Append(200)));
        }
    }
}
