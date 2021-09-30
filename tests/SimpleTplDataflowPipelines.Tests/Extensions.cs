using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace SimpleTplDataflowPipelines.Tests
{
    internal static class DataflowBlockExtensions
    {
        // https://stackoverflow.com/questions/49389273/for-a-tpl-dataflow-how-do-i-get-my-hands-on-all-the-output-produced-by-a-transf/62410007#62410007
        public static async Task<List<T>> ToListAsync<T>(
            this IReceivableSourceBlock<T> block)
        {
            var list = new List<T>();
            while (await block.OutputAvailableAsync().ConfigureAwait(false))
                while (block.TryReceive(out var item))
                    list.Add(item);
            await block.Completion.ConfigureAwait(false);
            return list;
        }
    }

    internal static class TaskExtensions
    {
        // https://stackoverflow.com/questions/4238345/asynchronously-wait-for-taskt-to-complete-with-timeout/11191070#11191070
        public static Task WithTimeout(this Task task, TimeSpan timeout)
        {
            var cts = new CancellationTokenSource(timeout);
            return task
                .ContinueWith(_ => { }, cts.Token,
                    TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default)
                .ContinueWith(continuation =>
                {
                    cts.Dispose();
                    if (task.IsCompleted) return task;
                    if (continuation.IsCanceled) throw new TimeoutException();
                    return task;
                }, default, TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default).Unwrap();
        }
    }
}
