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
            Assert.IsTrue(timestamps.All(x => x > 0L));
            Assert.IsTrue(timestamps.Zip(timestamps.Skip(1)).All(e => e.First <= e.Second));
        }

        [TestMethod]
        public void FailedPostCompletionAction()
        {
            var block1 = new TransformBlock<int, int>(x => x,
                new ExecutionDataflowBlockOptions() { BoundedCapacity = 1 });
            var block2 = new ActionBlock<int>(x => Thread.Sleep(20));

            var pipeline = PipelineBuilder
                .BeginWith(block1)
                .WithPostCompletionAction(t => throw new ApplicationException(t.Status.ToString()))
                .LinkTo(block2)
                .WithPostCompletionAction(t => throw new ApplicationException(t.Status.ToString()))
                .ToPipeline();

            pipeline.Post(13);
            pipeline.Post(42);
            pipeline.Complete();

            var aex = Assert.ThrowsException<AggregateException>(
                () => pipeline.Completion.Wait(100));
            Console.WriteLine(String.Join(", ", aex.InnerExceptions.Select(ex => ex.Message)));
            Assert.IsTrue(aex.InnerExceptions.Count == 2);
            Assert.IsTrue(aex.InnerExceptions.All(ex => ex is ApplicationException));
            Assert.IsTrue(aex.InnerExceptions.Select(ex => ex.Message).SequenceEqual(new[] { "RanToCompletion", "Faulted" }));
            Assert.IsTrue(block1.Completion.IsCompletedSuccessfully());
            Assert.IsTrue(block2.Completion.IsFaulted);
            Assert.IsTrue(block2.Completion.Exception.InnerExceptions.Count == 1);
            Assert.IsTrue(block2.Completion.Exception.InnerException is PipelineException);
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

        [TestMethod]
        public async Task FailedUnlinkedBlock()
        {
            var block2 = new ActionBlock<int>(async x => { await Task.Delay(20); throw new ApplicationException(x.ToString()); },
                new ExecutionDataflowBlockOptions() { MaxDegreeOfParallelism = 2, BoundedCapacity = 2 });
            var block1 = new ActionBlock<int>(async x => await block2.SendAsync(x),
                new ExecutionDataflowBlockOptions() { BoundedCapacity = 2 });

            var pipeline = PipelineBuilder
                .BeginWith(block1)
                .AddUnlinked(block2)
                .ToPipeline();

            var source = Enumerable.Range(1, 10);
            await Task.Run(async () =>
            {
                foreach (var item in source) await pipeline.SendAsync(item);
            }).WithTimeout(500);

            pipeline.Complete();
            var aex = Assert.ThrowsException<AggregateException>(
                () => pipeline.Completion.Wait(100));
            Console.WriteLine(String.Join(", ", aex.InnerExceptions.Select(ex => ex.Message)));
            Assert.IsTrue(aex.InnerExceptions.Count == 2);
            Assert.IsTrue(aex.InnerExceptions.All(ex => ex is ApplicationException));
            Assert.IsTrue(aex.InnerExceptions.Select(ex => ex.Message).OrderBy(x => x).SequenceEqual(new[] { "1", "2" }));
            Assert.IsTrue(block1.Completion.IsFaulted);
            Assert.IsTrue(block1.Completion.Exception.InnerExceptions.Count == 1);
            Assert.IsTrue(block1.Completion.Exception.InnerException is PipelineException);
            Assert.IsTrue(block2.Completion.IsFaulted);
            Assert.IsTrue(block2.Completion.Exception.InnerExceptions.Count == 2);
            Assert.IsTrue(block2.Completion.Exception.InnerExceptions.All(ex => ex is ApplicationException));
        }
    }
}
