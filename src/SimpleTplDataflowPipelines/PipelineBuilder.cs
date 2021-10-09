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
            return new PipelineBuilder<TInput>(block, block, null);
        }
    }

    internal delegate Action LinkDelegate(
        List<Task> completions, List<Action> failureActions, Action onError);

    /// <summary>
    /// Represents an error that occurred in an other dataflow block, owned by the same pipeline.
    /// </summary>
    public class PipelineException : Exception
    {
        internal PipelineException() : base("An other dataflow block, owned by the same pipeline, failed.") { }
    }

    /// <summary>
    /// An immutable struct that holds the metadata for building a pipeline without output.
    /// </summary>
    public readonly struct PipelineBuilder<TInput>
    {
        private readonly ITargetBlock<TInput> _target;
        private readonly IDataflowBlock _lastBlock;
        private readonly LinkDelegate[] _linkDelegates;

        internal PipelineBuilder(ITargetBlock<TInput> target, IDataflowBlock lastBlock,
            LinkDelegate[] linkDelegates)
        {
            Debug.Assert(target != null);
            Debug.Assert(lastBlock != null);
            _target = target;
            _lastBlock = lastBlock;
            _linkDelegates = linkDelegates;
        }

        /// <summary>
        /// Materializes the metadata stored in this builder, by creating an encapsulating
        /// target block.
        /// </summary>
        /// <remarks>
        /// After calling this method, the blocks have been linked and are now owned by
        /// the pipeline for the rest of their existence.
        /// The pipeline represents the completion of its constituent blocks, and
        /// propagates all of their errors. The pipeline completes when all the blocks
        /// have completed.
        /// If any block fails, the whole pipeline fails, and all the non-completed blocks
        /// are forcefully completed and their output is discarded.
        /// </remarks>
        public ITargetBlock<TInput> ToPipeline()
        {
            if (_target == null) throw new InvalidOperationException();
            Debug.Assert(_lastBlock != null);
            // Add a dummy final link so that there is one link for each block
            var action = PipelineCommon.CreateLinkDelegate<object>(_lastBlock, null, null);
            var newActions = PipelineCommon.Append(_linkDelegates, action);
            var completion = PipelineCommon.CreatePipeline(_target, newActions);
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

        internal PipelineBuilder(ITargetBlock<TInput> target, ISourceBlock<TOutput> source,
            LinkDelegate[] linkDelegates)
        {
            Debug.Assert(target != null);
            Debug.Assert(source != null);
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
            var action = PipelineCommon.CreateLinkDelegate(source, source, block);
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
            var action = PipelineCommon.CreateLinkDelegate(source, source, block);
            var newActions = PipelineCommon.Append(_linkDelegates, action);
            return new PipelineBuilder<TInput>(_target, block, newActions);
        }

        /// <summary>
        /// Materializes the metadata stored in this builder, by creating an encapsulating
        /// propagator block.
        /// </summary>
        /// <remarks>
        /// After calling this method, the blocks have been linked and are now owned by
        /// the pipeline for the rest of their existence.
        /// The pipeline represents the completion of its constituent blocks, and
        /// propagates all of their errors. The pipeline completes when all the blocks
        /// have completed.
        /// If any block fails, the whole pipeline fails, and all the non-completed blocks
        /// are forcefully completed and their output is discarded.
        /// </remarks>
        public IPropagatorBlock<TInput, TOutput> ToPipeline()
        {
            if (_target == null) throw new InvalidOperationException();
            Debug.Assert(_source != null);
            // Add a dummy final link so that there is one link for each block
            var action = PipelineCommon.CreateLinkDelegate(_source, _source, null);
            var newActions = PipelineCommon.Append(_linkDelegates, action);
            var completion = PipelineCommon.CreatePipeline(_target, newActions);
            return new Pipeline<TInput, TOutput>(_target, _source, completion);
        }
    }

    internal static class PipelineCommon
    {
        private static readonly DataflowLinkOptions _nullTargetLinkOptions
            = new DataflowLinkOptions() { Append = false };

        internal static Task CreatePipeline<TInput>(ITargetBlock<TInput> target,
            LinkDelegate[] linkDelegates)
        {
            Debug.Assert(target != null);
            Debug.Assert(linkDelegates != null);

            var completions = new List<Task>();
            var failureActions = new List<Action>();
            Action onError = () =>
            {
                Action[] failureActionsLocal;
                lock (failureActions)
                {
                    failureActionsLocal = failureActions.ToArray();
                    failureActions.Clear();
                }
                foreach (var failureAction in failureActionsLocal) failureAction();
            };

            // Invoking the linkDelegates populates the completions and failureActions lists.
            var finalActions = new List<Action>();
            foreach (var linkDelegate in linkDelegates)
            {
                finalActions.Add(linkDelegate(completions, failureActions, onError));
            }

            // Invoking the finalActions links all the blocks together, and attaches a
            // continuation to each block.
            foreach (var finalAction in finalActions) finalAction();

            // Combine the completions of all blocks, excluding the sentinel exceptions.
            return Task.WhenAll(completions).ContinueWith(t =>
            {
                if (!t.IsFaulted) return t;
                var tcs = new TaskCompletionSource<object>();
                tcs.SetException(
                    t.Exception.InnerExceptions.Where(ex => !(ex is PipelineException)));
                return tcs.Task;
            }).Unwrap();
        }

        internal static LinkDelegate CreateLinkDelegate<TOutput>(IDataflowBlock block,
            ISourceBlock<TOutput> blockAsSource, ITargetBlock<TOutput> target)
        {
            Debug.Assert(block != null);
            return new LinkDelegate((completions, failureActions, onError)
                => PipelineCommon.LinkTo(
                    block, blockAsSource, target, completions, failureActions, onError));
        }

        internal static Action LinkTo<TOutput>(IDataflowBlock block,
            ISourceBlock<TOutput> blockAsSource, ITargetBlock<TOutput> target,
            List<Task> completions, List<Action> failureActions, Action onError)
        {
            Debug.Assert(block != null);
            Debug.Assert(!(target != null && blockAsSource == null));
            Debug.Assert(completions != null);
            Debug.Assert(failureActions != null);
            Debug.Assert(onError != null);

            completions.Add(block.Completion);

            failureActions.Add(() =>
            {
                if (block.Completion.IsCompleted) return;
                block.Fault(new PipelineException());
                // Discard the output of the block, if it's actually a source block.
                // The last block in the pipeline may not be a source block.
                if (blockAsSource != null)
                    _ = blockAsSource.LinkTo(
                        DataflowBlock.NullTarget<TOutput>(), _nullTargetLinkOptions);
            });

            // Propagating the completion of the blocks follows the same pattern implemented
            // internally by the TPL Dataflow library. The ContinueWith method is used for
            // creating fire-and-forget continuations, with any error thrown inside the
            // continuations propagated to the ThreadPool.
            // https://source.dot.net/System.Threading.Tasks.Dataflow/Internal/Common.cs.html#7160be0ba468d387
            // It's extremely unlikely that any of these continuations will ever fail since,
            // according to the documentation, the invoked APIs (Complete, LinkTo, NullTarget)
            // do not throw exceptions.

            // The onError delegate must not be invoked before all failureActions have been
            // added in the list, hence the need to return an Action here, instead
            // of attaching the continuation immediately.
            return () =>
            {
                if (target != null)
                {
                    // Storing the IDisposable returned by the LinkTo, and disposing it after
                    // the completion of the block, would serve no purpose. All links are
                    // released automatically anyway when a block completes.
                    _ = blockAsSource.LinkTo(target);
                }

                _ = block.Completion.ContinueWith(t => OnErrorThrowOnThreadPool(() =>
                {
                    if (t.IsFaulted)
                        onError(); // Signal that the pipeline has failed
                    else if (target != null)
                        target.Complete(); // Propagate the completion to the target
                }), default, TaskContinuationOptions.DenyChildAttach, TaskScheduler.Default);
            };
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
