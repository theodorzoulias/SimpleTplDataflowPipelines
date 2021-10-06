using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.ExceptionServices;
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

    internal delegate void LinkDelegate(List<Task> completions);

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
            var action = new LinkDelegate(
                completions => PipelineCommon.LinkTo(source, block, completions));
            var newActions = PipelineCommon.Append(_linkDelegates, action);
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
            var action = new LinkDelegate(
                completions => PipelineCommon.LinkTo(source, block, completions));
            var newActions = PipelineCommon.Append(_linkDelegates, action);
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
        internal static Task CreatePipeline<TInput>(ITargetBlock<TInput> target,
            LinkDelegate[] linkDelegates)
        {
            Debug.Assert(target != null);
            var completions = new List<Task>();
            // The initial builder has null linkDelegates, so the Completion of the first
            // block must be added manually in the list.
            completions.Add(target.Completion);
            // Invoking the linkDelegates links all the blocks together, and populates the
            // completions list with the `Completion`s of all blocks.
            if (linkDelegates != null)
                foreach (var action in linkDelegates) action(completions);
            return Task.WhenAll(completions);
        }

        internal static void LinkTo<TOutput>(
            ISourceBlock<TOutput> source, ITargetBlock<TOutput> target,
            List<Task> completions)
        {
            Debug.Assert(source != null);
            Debug.Assert(target != null);
            Debug.Assert(completions != null);
            completions.Add(target.Completion);

            // Storing the IDisposable returned by the LinkTo, and and disposing it after
            // the completion of the block, serves no purpose. All links are released
            // automatically anyway when a block completes.
            // Also allowing the dismantling of the pipeline before its completion, is a
            // functionality that is not likely to be useful in any scenario.
            _ = source.LinkTo(target);

            // Propagating the completion of the blocks follows the same pattern implemented
            // internally by the TPL Dataflow library. The ContinueWith method is used for
            // creating fire-and-forget continuations, with any error thown inside the
            // continuations propagated to the ThreadPool.
            // https://source.dot.net/System.Threading.Tasks.Dataflow/Internal/Common.cs.html#7160be0ba468d387
            // It's extremely unlikely that any of these continuations will ever fail since,
            // according to the documentation, the invoked APIs do not throw exceptions.

            _ = source.Completion.ContinueWith(t => OnErrorThrowOnThreadPool(() =>
            {
                // The completion of the source is propagated to the target without a check,
                // because this is the normal case.
                target.Complete();
            }), default, TaskContinuationOptions.DenyChildAttach, TaskScheduler.Default);

            _ = target.Completion.ContinueWith(t => OnErrorThrowOnThreadPool(() =>
            {
                // The completion of the target is propagated to the source after checking
                // that the source is not completed yet, because this case is exceptional.
                if (source.Completion.IsCompleted) return;
                source.Complete();
                _ = source.LinkTo(DataflowBlock.NullTarget<TOutput>()); // Discard output
            }), default, TaskContinuationOptions.DenyChildAttach, TaskScheduler.Default);
        }

        internal static void OnErrorThrowOnThreadPool(Action action)
        {
            try { action(); }
            catch (Exception error)
            {
                // Copy-pasted from here:
                // https://github.com/dotnet/runtime/blob/8486eacfa31af0e28e8b819e7b36a32cf755a94f/src/libraries/System.Threading.Tasks.Dataflow/src/Internal/Common.cs#L558
                ExceptionDispatchInfo edi = ExceptionDispatchInfo.Capture(error);
                ThreadPool.QueueUserWorkItem(state => { ((ExceptionDispatchInfo)state).Throw(); }, edi);
            }
        }

        internal static T[] Append<T>(T[] array, T item)
        {
            T[] newArray;
            if (array == null || array.Length == 0)
            {
                newArray = new T[1];
            }
            else
            {
                newArray = new T[array.Length + 1];
                Array.Copy(array, newArray, array.Length);
            }
            newArray[newArray.Length - 1] = item;
            return newArray;
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
}
