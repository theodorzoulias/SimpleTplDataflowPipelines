using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace SimpleTplDataflowPipelines.Tests
{
    internal static class DataflowBlockExtensions
    {
        // https://stackoverflow.com/questions/49389273/for-a-tpl-dataflow-how-do-i-get-my-hands-on-all-the-output-produced-by-a-transf/62410007#62410007
        public static Task<List<T>> ToListAsync<T>(this IReceivableSourceBlock<T> block,
            CancellationToken cancellationToken = default)
        {
            return Implementation().ContinueWith(t =>
            {
                if (t.IsCanceled) return t;
                Debug.Assert(block.Completion.IsCompleted);
                if (block.Completion.IsFaulted)
                {
                    var tcs = new TaskCompletionSource<List<T>>();
                    tcs.SetException(block.Completion.Exception.InnerExceptions);
                    return tcs.Task;
                }
                if (block.Completion.IsCanceled) block.Completion.GetAwaiter().GetResult();
                return t;
            }, cancellationToken, TaskContinuationOptions.DenyChildAttach |
                TaskContinuationOptions.LazyCancellation, TaskScheduler.Default).Unwrap();

            async Task<List<T>> Implementation()
            {
                var list = new List<T>();
                while (await block.OutputAvailableAsync(cancellationToken).ConfigureAwait(false))
                    while (block.TryReceive(out var item))
                        list.Add(item);
                await block.Completion.ConfigureAwait(false);
                return list;
            }
        }
    }

    internal static class TaskExtensions
    {
        // https://stackoverflow.com/questions/4238345/asynchronously-wait-for-taskt-to-complete-with-timeout/11191070#11191070
        public static Task WithTimeout(this Task task, int timeoutMilliseconds)
        {
            var cts = new CancellationTokenSource(timeoutMilliseconds);
            return task
                .ContinueWith(_ => { }, cts.Token,
                    TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default)
                .ContinueWith(continuation =>
                {
                    cts.Dispose();
                    if (task.IsCompleted) return task;
                    if (continuation.IsCanceled) return Task.FromException(new TimeoutException());
                    return task;
                }, default(CancellationToken), TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default).Unwrap();
        }

        // Missing from .NET Framework
        public static bool IsCompletedSuccessfully(this Task task)
        {
            return task.Status == TaskStatus.RanToCompletion;
        }
    }
}
