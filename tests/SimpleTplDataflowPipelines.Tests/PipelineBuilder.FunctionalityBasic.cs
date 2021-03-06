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
        public async Task CreateTargetPipeline()
        {
            bool done = false;
            var block1 = new TransformBlock<int, string>(_ => "");
            var block2 = new TransformBlock<string, int>(_ => 0);
            var block3 = new ActionBlock<int>(_ => done = true);

            var pipeline = PipelineBuilder
                .BeginWith(block1)
                .LinkTo(block2)
                .LinkTo(block3)
                .ToPipeline();

            pipeline.Post(0);
            pipeline.Complete();
            await pipeline.Completion.WithTimeout(1000);
            Assert.IsTrue(done);
        }

        [TestMethod]
        public async Task CreatePropagatorPipeline()
        {
            var block1 = new TransformBlock<int, string>(n => n.ToString());
            var block2 = new TransformBlock<string, int>(s => Int32.Parse(s));
            var block3 = new BufferBlock<int>();

            var pipeline = PipelineBuilder
                .BeginWith(block1)
                .LinkTo(block2)
                .LinkTo(block3)
                .ToPipeline();

            pipeline.Post(13);
            pipeline.Post(42);
            pipeline.Complete();
            var receivable = (IReceivableSourceBlock<int>)pipeline;
            var list = await receivable.ToListAsync(new CancellationTokenSource(1000).Token);
            Assert.IsTrue(list.SequenceEqual(new int[] { 13, 42 }));
        }

        [TestMethod]
        public async Task CreatePipelineWithBuilderStepByStep()
        {
            bool done = false;
            var block1 = new TransformBlock<int, string>(_ => "");
            var block2 = new TransformBlock<string, int>(_ => 0);
            var block2b = new BufferBlock<int>();
            var block3 = new ActionBlock<int>(_ => done = true);

            var builder1 = PipelineBuilder.BeginWith(block1);
            var builder2 = builder1.LinkTo(block2);
            builder2 = builder2.LinkTo(block2b);
            var builder3 = builder2.LinkTo(block3);
            var pipeline = builder3.ToPipeline();

            pipeline.Post(0);
            pipeline.Complete();
            await pipeline.Completion.WithTimeout(1000);
            Assert.IsTrue(done);
        }

        [TestMethod]
        public async Task CreateSingleTargetPipeline()
        {
            bool done = false;
            var block = new ActionBlock<int>(_ => done = true);
            var pipeline = PipelineBuilder.BeginWith(block).ToPipeline();
            pipeline.Post(0);
            pipeline.Complete();
            await pipeline.Completion.WithTimeout(1000);
            Assert.IsTrue(done);
        }

        [TestMethod]
        public async Task CreateSinglePropagatorPipeline()
        {
            var block = new TransformBlock<int, string>(_ => "OK");
            var pipeline = PipelineBuilder.BeginWith(block).ToPipeline();
            pipeline.Post(13);
            pipeline.Complete();
            var receivable = (IReceivableSourceBlock<string>)pipeline;
            var list = await receivable.ToListAsync(new CancellationTokenSource(1000).Token);
            Assert.IsTrue(list.SequenceEqual(new string[] { "OK" }));
        }

        [TestMethod]
        public async Task BoundedCapacityNoDeadlock()
        {
            // https://stackoverflow.com/questions/21603428/tpl-dataflow-exception-in-transform-block-with-bounded-capacity
            var block1 = new BufferBlock<int>(new DataflowBlockOptions() { BoundedCapacity = 1 });

            var block2 = new ActionBlock<int>(async x => { await Task.Delay(50); throw new ApplicationException(); },
                new ExecutionDataflowBlockOptions() { BoundedCapacity = 2, MaxDegreeOfParallelism = 2 });

            var pipeline = PipelineBuilder
                .BeginWith(block1)
                .LinkTo(block2)
                .ToPipeline();

            await Task.Run(async () =>
            {
                foreach (var item in Enumerable.Range(1, 5))
                    if (!await pipeline.SendAsync(item)) break;
            }).WithTimeout(500);

            pipeline.Complete();

            await Assert.ThrowsExceptionAsync<ApplicationException>(
                async () => await block2.Completion.WithTimeout(1000));
            var aex = Assert.ThrowsException<AggregateException>(
                () => pipeline.Completion.Wait(100));
            Assert.IsTrue(aex.InnerExceptions.Count == 2, aex.InnerExceptions.Count.ToString());
            Assert.IsTrue(aex.InnerExceptions.All(ex => ex is ApplicationException));
            Assert.IsTrue(block1.Count == 0);
            Assert.IsTrue(block1.Completion.IsFaulted);
            Assert.IsTrue(block2.Completion.IsFaulted);
        }

        [TestMethod]
        public async Task DiscardOutputOfVeryLongPipeline()
        {
            var blocks = Enumerable.Range(1, 100)
                .Select(_ => new TransformBlock<int, int>(x => x, new ExecutionDataflowBlockOptions() { BoundedCapacity = 1 }))
                .ToArray();

            var finalBlock = new ActionBlock<int>(x => throw new ApplicationException(),
                new ExecutionDataflowBlockOptions() { BoundedCapacity = 1 });

            var builder = PipelineBuilder.BeginWith(blocks[0]);
            foreach (var block in blocks.Skip(1)) builder = builder.LinkTo(block);
            var pipeline = builder.LinkTo(finalBlock).ToPipeline();

            await Task.Run(async () =>
            {
                foreach (var item in Enumerable.Range(1, blocks.Length + 5))
                    if (!await pipeline.SendAsync(item)) break;
            }).WithTimeout(500);

            pipeline.Complete();

            await Assert.ThrowsExceptionAsync<ApplicationException>(
                async () => await finalBlock.Completion.WithTimeout(1000));
            var aex = Assert.ThrowsException<AggregateException>(
                () => pipeline.Completion.Wait(100));
            Assert.IsTrue(aex.InnerExceptions.Count == 1, aex.InnerExceptions.Count.ToString());
            Assert.IsTrue(aex.InnerException is ApplicationException);
            Assert.IsTrue(blocks.All(block => block.OutputCount == 0));
            Assert.IsTrue(blocks.All(block => block.Completion.IsFaulted));
            Assert.IsTrue(finalBlock.Completion.IsFaulted);
        }

        [TestMethod]
        public void ExceptionsFromMultipleBlocks()
        {
            var block1 = new TransformBlock<int, int>(
                async x => { if (x == 4) { await Task.Delay(50); throw new ApplicationException(x.ToString()); } return x; });
            var block2 = new TransformBlock<int, int>(
                async x => { if (x >= 3) { await Task.Delay(50); throw new ApplicationException(x.ToString()); } return x; });
            var block3 = new TransformBlock<int, int>(
                async x => { await Task.Delay(50); throw new ApplicationException(x.ToString()); },
                new ExecutionDataflowBlockOptions() { MaxDegreeOfParallelism = 2 });
            var block4 = new ActionBlock<int>(_ => { });

            var pipeline = PipelineBuilder
                .BeginWith(block1)
                .LinkTo(block2)
                .LinkTo(block3)
                .LinkTo(block4)
                .ToPipeline();

            pipeline.Post(1);
            pipeline.Post(2);
            pipeline.Post(3);
            pipeline.Post(4);
            pipeline.Complete();
            var aex = Assert.ThrowsException<AggregateException>(
                () => pipeline.Completion.WithTimeout(1000).Wait());
            Assert.IsTrue(aex.InnerExceptions.Count == 4, String.Join(", ", aex.InnerExceptions.Select(ex => ex.Message)));
            Assert.IsTrue(aex.InnerExceptions.All(ex => ex is ApplicationException));
            Assert.IsTrue(aex.InnerExceptions.Select(ex => ex.Message).OrderBy(x => x).SequenceEqual(new[] { "1", "2", "3", "4" }));
            Assert.IsTrue(block4.Completion.IsFaulted);
            Assert.IsTrue(block4.Completion.Exception.InnerExceptions.Count == 1);
            Assert.IsTrue(block4.Completion.Exception.InnerException is PipelineException);
        }

        [TestMethod]
        public void FailedMiddleBlock()
        {
            var block1 = new TransformBlock<int, int>(x => x);
            var block2 = new TransformBlock<int, int>(x => { if (x == 3) throw new ApplicationException(x.ToString()); return x; });
            var block3 = new ActionBlock<int>(x => { });

            var pipeline = PipelineBuilder
                .BeginWith(block1)
                .LinkTo(block2)
                .LinkTo(block3)
                .ToPipeline();

            foreach (var item in Enumerable.Range(1, 5)) pipeline.Post(item);
            pipeline.Complete();

            var aex = Assert.ThrowsException<AggregateException>(
                () => pipeline.Completion.Wait(100));
            Assert.IsTrue(aex.InnerExceptions.Count == 1, aex.InnerExceptions.Count.ToString());
            Assert.IsTrue(aex.InnerException is ApplicationException);
            Assert.IsTrue(aex.InnerException.Message == "3");
        }

        [TestMethod]
        public async Task PropagateResultsFromDiversePipeline()
        {
            var block1 = new BroadcastBlock<int>(x => x);
            var block2 = new BatchBlock<int>(10);
            var block3 = new TransformBlock<int[], int[]>(x => x);
            var block4 = new TransformManyBlock<int[], int>(x => x);
            var block5 = new BufferBlock<int>();

            var pipeline = PipelineBuilder
                .BeginWith(block1)
                .LinkTo(block2)
                .LinkTo(block3)
                .LinkTo(block4)
                .LinkTo(block5)
                .ToPipeline();

            var source = Enumerable.Range(1, 1000);
            foreach (var item in source) pipeline.Post(item);
            pipeline.Complete();
            var list = await ((IReceivableSourceBlock<int>)pipeline).ToListAsync(new CancellationTokenSource(1000).Token);
            Assert.IsTrue(list.SequenceEqual(source));
        }

        [TestMethod]
        public void LongChaoticPipeline()
        {
            var blocks = Enumerable.Range(1, 100).Reverse()
                .Select(n => new TransformBlock<int, int>(
                    x => { if (x == n) throw new ApplicationException($"{n}/{x}"); return x; }))
                .ToArray();

            var builder = PipelineBuilder.BeginWith(blocks[0]);
            foreach (var block in blocks.Skip(1)) builder = builder.LinkTo(block);
            var pipeline = builder.ToPipeline();

            foreach (var item in Enumerable.Range(1, 1000)) pipeline.Post(item);

            pipeline.Complete();

            var aex = Assert.ThrowsException<AggregateException>(
                () => pipeline.Completion.WithTimeout(1000).Wait());
            Assert.IsTrue(aex.InnerExceptions.All(ex => ex is ApplicationException));
            Console.WriteLine($"({aex.InnerExceptions.Count}) {String.Join(", ", aex.InnerExceptions.Select(ex => ex.Message))}");
        }

        [TestMethod]
        public async Task Cancellation()
        {
            var cts = new CancellationTokenSource();
            var options = new ExecutionDataflowBlockOptions()
            { BoundedCapacity = 1, CancellationToken = cts.Token };
            var blocks = Enumerable.Range(1, 100).Reverse()
                .Select(n => new TransformBlock<int, int>(
                    async x => { await Task.Yield(); return x; }, options))
                .ToArray();

            int count = 0;
            var finalBlock = new ActionBlock<int>(x => count++,
                new ExecutionDataflowBlockOptions() { BoundedCapacity = 1 });

            var builder = PipelineBuilder.BeginWith(blocks[0]);
            foreach (var block in blocks.Skip(1)) builder = builder.LinkTo(block);
            var pipeline = builder.LinkTo(finalBlock).ToPipeline();
            cts.CancelAfter(100);

            await Task.Run(async () =>
            {
                while (true) if (!await pipeline.SendAsync(0)) break;
            }).WithTimeout(500);

            pipeline.Complete();

            var ex = await Assert.ThrowsExceptionAsync<TaskCanceledException>(
                async () => await pipeline.Completion.WithTimeout(1000));
            Assert.IsTrue(ex.CancellationToken != cts.Token); // TPL Dataflow doesn't preserve the token
            Assert.IsTrue(pipeline.Completion.IsCanceled);
            Console.WriteLine($"Count: {count}");
        }

        [TestMethod]
        public void CreatePipelineWithBlocksAlreadyFaulted()
        {
            bool done = false;
            var block1 = new TransformBlock<int, string>(_ => "");
            var block2 = new TransformBlock<string, int>(_ => 0);
            var block3 = new ActionBlock<int>(_ => done = true);

            ((IDataflowBlock)block2).Fault(new ApplicationException());
            Assert.ThrowsException<AggregateException>(
                () => block2.Completion.Wait(100));

            var pipeline = PipelineBuilder
                .BeginWith(block1)
                .LinkTo(block2)
                .LinkTo(block3)
                .ToPipeline();

            pipeline.Post(0);
            pipeline.Complete();
            var aex = Assert.ThrowsException<AggregateException>(
                () => pipeline.Completion.WithTimeout(1000).Wait());
            Assert.IsTrue(aex.InnerExceptions.Count == 1);
            Assert.IsTrue(aex.InnerException is ApplicationException);
            Assert.IsTrue(!done);
        }

        [TestMethod]
        public void TimelyCompletionOfLinkedBlocks()
        {
            var block1 = new TransformBlock<int, int>(async x => { await Task.Delay(50); return x; });
            var block2 = new ActionBlock<int>(async _ => { await Task.Delay(50); throw new ApplicationException(); });

            var pipeline = PipelineBuilder
                .BeginWith(block1)
                .LinkTo(block2)
                .ToPipeline();

            foreach (var item in Enumerable.Range(1, 10)) pipeline.Post(item);
            pipeline.Complete();
            var aex = Assert.ThrowsException<AggregateException>(
                () => pipeline.Completion.Wait(400));
            Assert.IsTrue(aex.InnerExceptions.Count == 1);
            Assert.IsTrue(aex.InnerException is ApplicationException);
        }
    }
}
