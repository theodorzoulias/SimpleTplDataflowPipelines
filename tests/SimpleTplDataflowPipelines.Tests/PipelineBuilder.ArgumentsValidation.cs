using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace SimpleTplDataflowPipelines.Tests
{
    public partial class PipelineBuilderTests
    {
        [TestMethod]
        public void ArgumentsValidation_Static()
        {
            Assert.ThrowsException<ArgumentNullException>(() =>
            {
                PipelineBuilder.BeginWith((ITargetBlock<int>)null);
            });
            Assert.ThrowsException<ArgumentNullException>(() =>
            {
                PipelineBuilder.BeginWith((IPropagatorBlock<int, int>)null);
            });
        }

        [TestMethod]
        public void ArgumentsValidation_TInput()
        {
            var builder_1 = PipelineBuilder.BeginWith(new ActionBlock<int>(_ => { }));

            Assert.ThrowsException<ArgumentNullException>(() =>
            {
                builder_1.AddUnlinked((ITargetBlock<int>)null, true);
            });
            Assert.ThrowsException<ArgumentNullException>(() =>
            {
                builder_1.AddUnlinked((IPropagatorBlock<int, int>)null, true);
            });

            Assert.ThrowsException<ArgumentNullException>(() =>
            {
                builder_1.WithPostCompletionAction((Action<Task>)null);
            });
            Assert.ThrowsException<ArgumentNullException>(() =>
            {
                builder_1.WithPostCompletionAction((Func<Task, Task>)null);
            });
        }

        [TestMethod]
        public void ArgumentsValidation_TInput_TOutput()
        {
            var builder_2 = PipelineBuilder.BeginWith(new TransformBlock<int, int>(x => x));

            Assert.ThrowsException<ArgumentNullException>(() =>
            {
                builder_2.LinkTo((ITargetBlock<int>)null);
            });
            Assert.ThrowsException<ArgumentNullException>(() =>
            {
                builder_2.LinkTo((IPropagatorBlock<int, int>)null);
            });

            Assert.ThrowsException<ArgumentNullException>(() =>
            {
                builder_2.AddUnlinked((ITargetBlock<int>)null, true);
            });
            Assert.ThrowsException<ArgumentNullException>(() =>
            {
                builder_2.AddUnlinked((IPropagatorBlock<int, int>)null, true);
            });

            Assert.ThrowsException<ArgumentNullException>(() =>
            {
                builder_2.WithPostCompletionAction((Action<Task>)null);
            });
            Assert.ThrowsException<ArgumentNullException>(() =>
            {
                builder_2.WithPostCompletionAction((Func<Task, Task>)null);
            });
        }

        [TestMethod]
        public void ArgumentsValidation_EmptyBuilders()
        {
            Assert.ThrowsException<InvalidOperationException>(() =>
            {
                new PipelineBuilder<int>().ToPipeline();
            });
            Assert.ThrowsException<InvalidOperationException>(() =>
            {
                new PipelineBuilder<int, int>().ToPipeline();
            });
        }

        [TestMethod]
        public void UnexpectedPipelineException()
        {
            PipelineException pipelineException =
                (PipelineException)typeof(PipelineException).GetConstructor(
                BindingFlags.NonPublic | BindingFlags.Instance,
                null, Type.EmptyTypes, null).Invoke(null);

            var pipeline = PipelineBuilder
                .BeginWith(new ActionBlock<int>(_ => { throw pipelineException; }))
                .ToPipeline();

            pipeline.Post(13);
            pipeline.Complete();
            var aex = Assert.ThrowsException<AggregateException>(
                () => pipeline.Completion.WithTimeout(1000).Wait());
            Assert.IsTrue(aex.InnerExceptions.Count == 1);
            Assert.IsTrue(aex.InnerException is InvalidOperationException, aex.InnerException?.Message);
        }
    }
}
