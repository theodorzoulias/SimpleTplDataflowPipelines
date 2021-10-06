﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace SimpleTplDataflowPipelines
{
    /// <summary>
    /// Factory class for creating builders of simple TPL Dataflow pipelines.
    /// </summary>
    public static class PipelineBuilder
    {
        /// <summary>
        /// Creates a pipeline builder that holds the metadata for a propagator block.
        /// </summary>
        public static PipelineBuilder<TInput, TOutput> BeginWith<TInput, TOutput>(
            IPropagatorBlock<TInput, TOutput> block)
        {
            if (block == null) throw new ArgumentNullException(nameof(block));
            return new PipelineBuilder<TInput, TOutput>(block, block, null);
        }

        /// <summary>
        /// Creates a pipeline builder that holds the metadata for a target block.
        /// </summary>
        public static PipelineBuilder<TInput> BeginWith<TInput>(ITargetBlock<TInput> block)
        {
            if (block == null) throw new ArgumentNullException(nameof(block));
            return new PipelineBuilder<TInput>(block, null);
        }
    }

    internal delegate void LinkDelegate(List<Task> completions, List<Task> links);

    /// <summary>
    /// An immutable struct that holds the metadata for building a pipeline without output.
    /// </summary>
    public readonly struct PipelineBuilder<TInput>
    {
        private readonly ITargetBlock<TInput> _target;
        private readonly LinkDelegate[] _linkDelegates;

        internal PipelineBuilder(ITargetBlock<TInput> target, LinkDelegate[] linkDelegates)
        {
            Debug.Assert(target != null);
            _target = target;
            _linkDelegates = linkDelegates;
        }

        /// <summary>
        /// Materializes the metadata stored in this builder, by creating an encapsulating
        /// target block.
        /// </summary>
        /// <remarks>
        /// After calling this method, the blocks have been linked and are now owned by
        /// the pipeline for the rest of their existence.
        /// The pipeline represents the completion of all blocks, and propagates all of
        /// their errors. The pipeline completes when all the blocks have completed.
        /// If any block fails, the whole pipeline fails, and all non-completed blocks
        /// are forcefully completed and their output is discarded.
        /// </remarks>
        public ITargetBlock<TInput> ToPipeline()
        {
            if (_target == null) throw new InvalidOperationException();
            var completion = PipelineCommon.CreatePipeline(_target, _linkDelegates);
            return new Pipeline<TInput>(_target, completion);
        }
    }

    /// <summary>
    /// An immutable struct that holds the metadata for building a pipeline.
    /// </summary>
    public readonly struct PipelineBuilder<TInput, TOutput>
    {
        private readonly ITargetBlock<TInput> _target;
        private readonly ISourceBlock<TOutput> _source;
        private readonly LinkDelegate[] _linkDelegates;

        internal PipelineBuilder(ITargetBlock<TInput> target, ISourceBlock<TOutput> source, LinkDelegate[] linkDelegates)
        {
            _target = target;
            _source = source;
            _linkDelegates = linkDelegates;
        }

        /// <summary>
        /// Creates a new builder that holds all the metadata of the current builder,
        /// plus the metadata for the new propagator block.
        /// The current builder is not changed.
        /// </summary>
        public PipelineBuilder<TInput, TNewOutput> LinkTo<TNewOutput>(
            IPropagatorBlock<TOutput, TNewOutput> block)
        {
            if (block == null) throw new ArgumentNullException(nameof(block));
            var source = _source;
            var action = new LinkDelegate((completions, links)
                => PipelineCommon.LinkTo(source, block, completions, links));
            var newActions = PipelineUtilities.Append(_linkDelegates, action);
            return new PipelineBuilder<TInput, TNewOutput>(_target, block, newActions);
        }

        /// <summary>
        /// Creates a new builder that holds all the metadata of the current builder,
        /// plus the metadata for the new target block.
        /// The current builder is not changed.
        /// </summary>
        public PipelineBuilder<TInput> LinkTo(ITargetBlock<TOutput> block)
        {
            if (block == null) throw new ArgumentNullException(nameof(block));
            var source = _source;
            var action = new LinkDelegate((completions, links)
                => PipelineCommon.LinkTo(source, block, completions, links));
            var newActions = PipelineUtilities.Append(_linkDelegates, action);
            return new PipelineBuilder<TInput>(_target, newActions);
        }

        /// <summary>
        /// Materializes the metadata stored in this builder, by creating an encapsulating
        /// propagator block.
        /// </summary>
        /// <remarks>
        /// After calling this method, the blocks have been linked and are now owned by
        /// the pipeline for the rest of their existence.
        /// The pipeline represents the completion of all blocks, and propagates all of
        /// their errors. The pipeline completes when all the blocks have completed.
        /// If any block fails, the whole pipeline fails, and all non-completed blocks
        /// are forcefully completed and their output is discarded.
        /// </remarks>
        public IPropagatorBlock<TInput, TOutput> ToPipeline()
        {
            if (_target == null) throw new InvalidOperationException();
            Debug.Assert(_source != null);
            var completion = PipelineCommon.CreatePipeline(_target, _linkDelegates);
            return new Pipeline<TInput, TOutput>(_target, _source, completion);
        }
    }

    internal static class PipelineCommon
    {
        internal static void LinkTo<TOutput>(
            ISourceBlock<TOutput> source, ITargetBlock<TOutput> target,
            List<Task> completions, List<Task> links)
        {
            Debug.Assert(source != null);
            Debug.Assert(target != null);
            Debug.Assert(completions != null);
            Debug.Assert(links != null);
            // Storing the IDisposable and disposing it after the completion of the block
            // serves no purpose (all links are released automatically anyway when a block completes).
            // Allowing the dismantling of the pipeline before its completion, is a functionality
            // that is unlikely to be useful to anyone.
            _ = source.LinkTo(target);

            // https://github.com/dotnet/runtime/blob/8486eacfa31af0e28e8b819e7b36a32cf755a94f/src/libraries/System.Threading.Tasks.Dataflow/src/Internal/Common.cs#L558
            Task forwardLink = source.Completion.ContinueWith(t =>
            {
                // The completion of the source is propagated to the target without a check,
                // because this is the normal case.
                //throw new InvalidOperationException(); // @@
                target.Complete();
            }, TaskScheduler.Default);

            Task backwardLink = target.Completion.ContinueWith(t =>
            {
                // The completion of the target is propagated to the source after checking
                // that the source is not completed yet, because this case is exceptional.
                if (source.Completion.IsCompleted) return;
                source.Complete();
                _ = source.LinkTo(DataflowBlock.NullTarget<TOutput>()); // Discard output
            }, TaskScheduler.Default);

            completions.Add(target.Completion);
            links.Add(forwardLink);
            links.Add(backwardLink);
        }

        internal static Task CreatePipeline<TInput>(ITargetBlock<TInput> target,
            LinkDelegate[] linkDelegates)
        {
            Debug.Assert(target != null);
            var completions = new List<Task>();
            var links = new List<Task>();
            completions.Add(target.Completion); // The initial builder has null linkDelegates
            if (linkDelegates != null)
                foreach (var action in linkDelegates)
                    action(completions, links);

            // In the (extremely unlikely) scenario that any of the links fails, there is
            // no guarantee that the Completion's of all blocks will actually complete.
            // Propagating a link-error through the pipeline's Completion is not sufficient,
            // because the caller may consume the pipeline without observing directly the
            // Completion property (they may use the Receive method for example).
            // In that case the caller will most likely deadlock.

            // This leaves only one realistic option:
            // Rethrow the link-error on the ThreadPool, resulting in an application-crashing
            // unhandled exception. This sounds nasty, but we are talking about methods that
            // according to the documentation should never throw. So if they throw, the
            // current state of the process is most likely in deep trouble already.
            if (links.Count > 0)
            {
                var allLinks = PipelineUtilities.WhenAllFailFast(links);
                ThreadPool.QueueUserWorkItem(async _ => await allLinks);
            }
            return Task.WhenAll(completions);
        }
    }

    internal class Pipeline<TInput> : ITargetBlock<TInput>
    {
        private readonly ITargetBlock<TInput> _target;
        private readonly Task _completion;

        public Pipeline(ITargetBlock<TInput> target, Task completion)
        {
            Debug.Assert(target != null);
            Debug.Assert(completion != null);
            _target = target;
            _completion = completion;
        }

        public Task Completion => _completion;
        public void Complete() => _target.Complete();
        public void Fault(Exception exception) => _target.Fault(exception);

        public DataflowMessageStatus OfferMessage(DataflowMessageHeader messageHeader,
            TInput messageValue, ISourceBlock<TInput> source, bool consumeToAccept)
                => _target.OfferMessage(messageHeader, messageValue, source, consumeToAccept);
    }

    internal class Pipeline<TInput, TOutput> : IPropagatorBlock<TInput, TOutput>,
        IReceivableSourceBlock<TOutput>
    {
        private readonly ITargetBlock<TInput> _target;
        private readonly ISourceBlock<TOutput> _source;
        private readonly Task _completion;

        public Pipeline(ITargetBlock<TInput> target, ISourceBlock<TOutput> source,
            Task completion)
        {
            Debug.Assert(target != null);
            Debug.Assert(source != null);
            Debug.Assert(completion != null);
            _target = target;
            _source = source;
            _completion = completion;
        }

        public Task Completion => _completion;
        public void Complete() => _target.Complete();
        public void Fault(Exception exception) => _target.Fault(exception);

        public IDisposable LinkTo(ITargetBlock<TOutput> target, DataflowLinkOptions linkOptions)
            => _source.LinkTo(target, linkOptions);

        public DataflowMessageStatus OfferMessage(DataflowMessageHeader messageHeader,
            TInput messageValue, ISourceBlock<TInput> source, bool consumeToAccept)
                => _target.OfferMessage(messageHeader, messageValue, source, consumeToAccept);
        public TOutput ConsumeMessage(DataflowMessageHeader messageHeader, ITargetBlock<TOutput> target, out bool messageConsumed)
            => _source.ConsumeMessage(messageHeader, target, out messageConsumed);
        public bool ReserveMessage(DataflowMessageHeader messageHeader, ITargetBlock<TOutput> target)
            => _source.ReserveMessage(messageHeader, target);
        public void ReleaseReservation(DataflowMessageHeader messageHeader, ITargetBlock<TOutput> target)
            => _source.ReleaseReservation(messageHeader, target);

        public bool TryReceive(Predicate<TOutput> filter, out TOutput item)
        {
            if (_source is IReceivableSourceBlock<TOutput> receivable)
                return receivable.TryReceive(filter, out item);
            item = default(TOutput);
            return false;
        }

        public bool TryReceiveAll(out IList<TOutput> items)
        {
            if (_source is IReceivableSourceBlock<TOutput> receivable)
                return receivable.TryReceiveAll(out items);
            items = null;
            return false;
        }
    }

    internal static class PipelineUtilities
    {
        internal static T[] Append<T>(T[] array, T item)
        {
            return (array ?? Enumerable.Empty<T>()).Append(item).ToArray();
        }

        // https://stackoverflow.com/questions/57313252/how-can-i-await-an-array-of-tasks-and-stop-waiting-on-first-exception
        internal static Task WhenAllFailFast(IList<Task> tasks)
        {
            Debug.Assert(tasks != null);
            var cts = new CancellationTokenSource();
            Task failedTask = null;
            var continuationAction = new Action<Task>(task =>
            {
                if (task.Status != TaskStatus.RanToCompletion)
                    if (Interlocked.CompareExchange(ref failedTask, task, null) == null)
                        cts.Cancel();
            });
            var continuations = tasks.Select(task => task.ContinueWith(continuationAction,
                cts.Token, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default));

            return Task.WhenAll(continuations).ContinueWith(_ =>
            {
                cts.Dispose();
                if (failedTask != null) return failedTask;
                return Task.WhenAll(tasks);
            }, TaskScheduler.Default).Unwrap();
        }
    }
}
