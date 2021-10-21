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
        /// Creates a pipeline builder that holds the metadata for a target block.
        /// </summary>
        public static PipelineBuilder<TInput> BeginWith<TInput>(ITargetBlock<TInput> block)
        {
            if (block == null) throw new ArgumentNullException(nameof(block));
            return new PipelineBuilder<TInput>(block, block, null, null);
        }

        /// <summary>
        /// Creates a pipeline builder that holds the metadata for a propagator block.
        /// </summary>
        public static PipelineBuilder<TInput, TOutput> BeginWith<TInput, TOutput>(
            IPropagatorBlock<TInput, TOutput> block)
        {
            if (block == null) throw new ArgumentNullException(nameof(block));
            return new PipelineBuilder<TInput, TOutput>(block, block, null, null);
        }
    }

    /// <summary>
    /// An immutable struct that holds the metadata for building a pipeline without output.
    /// </summary>
    public struct PipelineBuilder<TInput>
    {
        private readonly ITargetBlock<TInput> _target;
        private readonly IDataflowBlock _lastBlock;
        private readonly Node[] _nodes;

        internal PipelineBuilder(ITargetBlock<TInput> target, IDataflowBlock lastBlock,
            Node[] previousNodes, Node newNode)
        {
            Debug.Assert(target != null);
            Debug.Assert(lastBlock != null);
            _target = target;
            _lastBlock = lastBlock;
            _nodes = newNode != null ?
                PipelineCommon.Append(previousNodes, newNode) : previousNodes;
        }

        /// <summary>
        /// Creates a new builder that holds all the metadata of the current builder,
        /// plus the metadata for a new target block that is not linked to the
        /// previous block, specifying whether the completion of the previous block
        /// will be propagated to the new block.
        /// </summary>
        /// <remarks>
        /// The current builder is not changed.
        /// </remarks>
        public PipelineBuilder<TInput> AddUnlinked<TNewInput>(
            ITargetBlock<TNewInput> block, bool propagateCompletion)
        {
            if (block == null) throw new ArgumentNullException(nameof(block));
            var newNode = new LinkNode<TNewInput>(_lastBlock, null, block, propagateCompletion);
            return new PipelineBuilder<TInput>(_target, block, _nodes, newNode);
        }

        /// <summary>
        /// Creates a new builder that holds all the metadata of the current builder,
        /// plus the metadata for a new propagator block that is not linked to the
        /// previous block, specifying whether the completion of the previous block
        /// will be propagated to the new block.
        /// </summary>
        /// <remarks>
        /// The current builder is not changed.
        /// </remarks>
        public PipelineBuilder<TInput, TNewOutput> AddUnlinked<TNewInput, TNewOutput>(
            IPropagatorBlock<TNewInput, TNewOutput> block, bool propagateCompletion)
        {
            if (block == null) throw new ArgumentNullException(nameof(block));
            var newNode = new LinkNode<TNewInput>(_lastBlock, null, block, propagateCompletion);
            return new PipelineBuilder<TInput, TNewOutput>(_target, block, _nodes, newNode);
        }

        /// <summary>
        /// Creates a new builder that holds all the metadata of the current builder,
        /// plus the metadata for a synchronous action that will be invoked when
        /// the last block completes.
        /// The completion of the action is propagated to the next block.
        /// </summary>
        /// <remarks>
        /// The current builder is not changed.
        /// </remarks>
        public PipelineBuilder<TInput> WithPostCompletionAction(Action<Task> action)
        {
            if (action == null) throw new ArgumentNullException(nameof(action));
            var newNode = new ActionNode(t => { action(t); return Task.CompletedTask; });
            return new PipelineBuilder<TInput>(_target, _lastBlock, _nodes, newNode);
        }

        /// <summary>
        /// Creates a new builder that holds all the metadata of the current builder,
        /// plus the metadata for an asynchronous action that will be invoked when
        /// the last block completes.
        /// The completion of the action is propagated to the next block.
        /// </summary>
        /// <remarks>
        /// The current builder is not changed.
        /// </remarks>
        public PipelineBuilder<TInput> WithPostCompletionAction(Func<Task, Task> action)
        {
            if (action == null) throw new ArgumentNullException(nameof(action));
            var newNode = new ActionNode(action);
            return new PipelineBuilder<TInput>(_target, _lastBlock, _nodes, newNode);
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
            // Add a dummy final node so that there is one node for each block
            var newNode = new LinkNode<object>(_lastBlock, null, null, false);
            var newNodes = PipelineCommon.Append(_nodes, newNode);
            var completion = PipelineCommon.CreatePipeline(_target, newNodes);
            return new Pipeline<TInput>(_target, completion);
        }
    }

    /// <summary>
    /// An immutable struct that holds the metadata for building a pipeline.
    /// </summary>
    public struct PipelineBuilder<TInput, TOutput>
    {
        private readonly ITargetBlock<TInput> _target;
        private readonly ISourceBlock<TOutput> _source;
        private readonly Node[] _nodes;

        internal PipelineBuilder(ITargetBlock<TInput> target, ISourceBlock<TOutput> source,
            Node[] previousNodes, Node newNode)
        {
            Debug.Assert(target != null);
            Debug.Assert(source != null);
            _target = target;
            _source = source;
            _nodes = newNode != null ?
                PipelineCommon.Append(previousNodes, newNode) : previousNodes;
        }

        /// <summary>
        /// Creates a new builder that holds all the metadata of the current builder,
        /// plus the metadata for the new target block.
        /// </summary>
        /// <remarks>
        /// The current builder is not changed.
        /// </remarks>
        public PipelineBuilder<TInput> LinkTo(ITargetBlock<TOutput> block)
        {
            if (block == null) throw new ArgumentNullException(nameof(block));
            var newNode = new LinkNode<TOutput>(_source, _source, block, true);
            return new PipelineBuilder<TInput>(_target, block, _nodes, newNode);
        }

        /// <summary>
        /// Creates a new builder that holds all the metadata of the current builder,
        /// plus the metadata for the new propagator block.
        /// </summary>
        /// <remarks>
        /// The current builder is not changed.
        /// </remarks>
        public PipelineBuilder<TInput, TNewOutput> LinkTo<TNewOutput>(
            IPropagatorBlock<TOutput, TNewOutput> block)
        {
            if (block == null) throw new ArgumentNullException(nameof(block));
            var newNode = new LinkNode<TOutput>(_source, _source, block, true);
            return new PipelineBuilder<TInput, TNewOutput>(_target, block, _nodes, newNode);
        }

        /// <summary>
        /// Creates a new builder that holds all the metadata of the current builder,
        /// plus the metadata for a new target block that is not linked to the
        /// previous block, specifying whether the completion of the previous block
        /// will be propagated to the new block.
        /// </summary>
        /// <remarks>
        /// The current builder is not changed.
        /// </remarks>
        public PipelineBuilder<TInput> AddUnlinked<TNewInput>(
            ITargetBlock<TNewInput> block, bool propagateCompletion)
        {
            if (block == null) throw new ArgumentNullException(nameof(block));
            var newNode = new LinkNode<TNewInput>(_source, null, block, propagateCompletion);
            return new PipelineBuilder<TInput>(_target, block, _nodes, newNode);
        }

        /// <summary>
        /// Creates a new builder that holds all the metadata of the current builder,
        /// plus the metadata for a new propagator block that is not linked to the
        /// previous block, specifying whether the completion of the previous block
        /// will be propagated to the new block.
        /// </summary>
        /// <remarks>
        /// The current builder is not changed.
        /// </remarks>
        public PipelineBuilder<TInput, TNewOutput> AddUnlinked<TNewInput, TNewOutput>(
            IPropagatorBlock<TNewInput, TNewOutput> block, bool propagateCompletion)
        {
            if (block == null) throw new ArgumentNullException(nameof(block));
            var newNode = new LinkNode<TNewInput>(_source, null, block, propagateCompletion);
            return new PipelineBuilder<TInput, TNewOutput>(_target, block, _nodes, newNode);
        }

        /// <summary>
        /// Creates a new builder that holds all the metadata of the current builder,
        /// plus the metadata for a synchronous action that will be invoked when
        /// the last block completes.
        /// The completion of the action is propagated to the next block.
        /// </summary>
        /// <remarks>
        /// The current builder is not changed.
        /// </remarks>
        public PipelineBuilder<TInput, TOutput> WithPostCompletionAction(Action<Task> action)
        {
            if (action == null) throw new ArgumentNullException(nameof(action));
            var newNode = new ActionNode(t => { action(t); return Task.CompletedTask; });
            return new PipelineBuilder<TInput, TOutput>(_target, _source, _nodes, newNode);
        }

        /// <summary>
        /// Creates a new builder that holds all the metadata of the current builder,
        /// plus the metadata for an asynchronous action that will be invoked when
        /// the last block completes.
        /// The completion of the action is propagated to the next block.
        /// </summary>
        /// <remarks>
        /// The current builder is not changed.
        /// </remarks>
        public PipelineBuilder<TInput, TOutput> WithPostCompletionAction(Func<Task, Task> action)
        {
            if (action == null) throw new ArgumentNullException(nameof(action));
            var newNode = new ActionNode(action);
            return new PipelineBuilder<TInput, TOutput>(_target, _source, _nodes, newNode);
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
            // Add a dummy final node so that there is one node for each block
            var newNode = new LinkNode<TOutput>(_source, _source, null, false);
            var newNodes = PipelineCommon.Append(_nodes, newNode);
            var completion = PipelineCommon.CreatePipeline(_target, newNodes);
            return new Pipeline<TInput, TOutput>(_target, _source, completion);
        }
    }

    /// <summary>
    /// Represents an error that occurred in an other dataflow block, owned by the same pipeline.
    /// </summary>
    public sealed class PipelineException : Exception
    {
        // Prevent this type from being publicly creatable.
        internal PipelineException() : base("An other dataflow block, owned by the same pipeline, failed.") { }
    }

    internal abstract class Node
    {
        public abstract void Run(List<Task> completions,
            Task lastCompletion, out Task nextCompletion, Action onError);
        public virtual void Fault() { }
    }

    internal sealed class LinkNode<TOutput> : Node
    {
        private static readonly DataflowLinkOptions _nullTargetLinkOptions
            = new DataflowLinkOptions() { Append = false };

        private readonly IDataflowBlock _block;
        private readonly ISourceBlock<TOutput> _blockAsSource;
        private readonly ITargetBlock<TOutput> _target;
        private readonly bool _propagateCompletion;

        public LinkNode(IDataflowBlock block, ISourceBlock<TOutput> blockAsSource,
            ITargetBlock<TOutput> target, bool propagateCompletion)
        {
            Debug.Assert(block != null);
            _block = block;
            _blockAsSource = blockAsSource;
            _target = target;
            _propagateCompletion = propagateCompletion;
        }

        public override void Run(List<Task> completions,
            Task lastCompletion, out Task nextCompletion, Action onError)
        {
            Debug.Assert(completions != null);
            Debug.Assert(lastCompletion != null);
            Debug.Assert(onError != null);

            completions.Add(_block.Completion);

            if (_blockAsSource != null && _target != null)
            {
                // Storing the IDisposable returned by the LinkTo, and disposing it after
                // the completion of the block, would serve no purpose. All links are
                // released automatically anyway when a block completes.
                _blockAsSource.LinkTo(_target);
            }

            // Propagating the completion of the blocks follows the same pattern implemented
            // internally by the TPL Dataflow library. The ContinueWith method is used for
            // creating fire-and-forget continuations, with any error thrown inside the
            // continuations propagated to the ThreadPool.
            // https://source.dot.net/System.Threading.Tasks.Dataflow/Internal/Common.cs.html#7160be0ba468d387
            // It's extremely unlikely that any of these continuations will ever fail since,
            // according to the documentation, the invoked APIs (Complete, LinkTo, NullTarget)
            // do not throw exceptions.
            lastCompletion.ContinueWith(t => PipelineCommon.OnErrorThrowOnThreadPool(() =>
            {
                if (t.IsFaulted)
                    onError(); // Signal that the pipeline has failed
                else if (_target != null && _propagateCompletion)
                    _target.Complete(); // Propagate the completion to the target
            }), default(CancellationToken), TaskContinuationOptions.DenyChildAttach, TaskScheduler.Default);
            nextCompletion = _target?.Completion;
        }

        public override void Fault()
        {
            if (_block.Completion.IsCompleted) return;
            _block.Fault(new PipelineException());
            // Discard the output of the block, if it's actually a source block.
            // The last block in the pipeline may not be a source block.
            if (_blockAsSource != null)
                _blockAsSource.LinkTo(
                    DataflowBlock.NullTarget<TOutput>(), _nullTargetLinkOptions);
        }
    }

    internal sealed class ActionNode : Node
    {
        private readonly Func<Task, Task> _action;

        public ActionNode(Func<Task, Task> action)
        {
            Debug.Assert(action != null);
            _action = action;
        }

        public override void Run(List<Task> completions,
            Task lastCompletion, out Task nextCompletion, Action onError)
        {
            nextCompletion = lastCompletion.ContinueWith(_action,
                default(CancellationToken), TaskContinuationOptions.DenyChildAttach,
                TaskScheduler.Default).Unwrap();
            completions.Add(nextCompletion);
        }
    }

    internal static class PipelineCommon
    {
        internal static Task CreatePipeline<TInput>(ITargetBlock<TInput> target,
            Node[] nodes)
        {
            Debug.Assert(target != null);
            Debug.Assert(nodes != null);

            var errorTcs = new TaskCompletionSource<object>();
            Action onError = () => errorTcs.TrySetResult(null);

            Task lastCompletion = target.Completion;
            var completions = new List<Task>();
            foreach (var node in nodes)
            {
                node.Run(completions, lastCompletion, out var completion, onError);
                lastCompletion = completion;
            }
            Debug.Assert(lastCompletion == null); // The last dummy node has no target

            // The onError delegate should not be invoked before all nodes have Run,
            // and should be invoked only once. Hence the need for the
            // TaskCompletionSource.TrySetResult + Task.ContinueWith complexity.
            errorTcs.Task.ContinueWith(_ => PipelineCommon.OnErrorThrowOnThreadPool(() =>
            {
                foreach (var node in nodes) node.Fault();
            }), default(CancellationToken), TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);

            // Combine the completions of all blocks, excluding the sentinel exceptions.
            return Task.WhenAll(completions).ContinueWith(t =>
            {
                if (!t.IsFaulted) return t;

                // Propagate all non-PipelineException errors. At least one should exist.
                var filtered = t.Exception.InnerExceptions
                    .Where(ex => !(ex is PipelineException));
                if (!filtered.Any()) throw new InvalidOperationException(
                    "After filtering out the PipelineExceptions, no other exception was left.");
                var tcs = new TaskCompletionSource<object>();
                tcs.SetException(filtered);
                return tcs.Task;
            }, default(CancellationToken), TaskContinuationOptions.DenyChildAttach, TaskScheduler.Default).Unwrap();
        }

        internal static void OnErrorThrowOnThreadPool(Action action)
        {
            Debug.Assert(action != null);
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

    internal sealed class Pipeline<TInput> : ITargetBlock<TInput>
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

    internal sealed class Pipeline<TInput, TOutput> : IPropagatorBlock<TInput, TOutput>,
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
            var receivable = _source as IReceivableSourceBlock<TOutput>;
            if (receivable != null) return receivable.TryReceive(filter, out item);
            item = default(TOutput);
            return false;
        }

        public bool TryReceiveAll(out IList<TOutput> items)
        {
            var receivable = _source as IReceivableSourceBlock<TOutput>;
            if (receivable != null) return receivable.TryReceiveAll(out items);
            items = null;
            return false;
        }
    }
}
