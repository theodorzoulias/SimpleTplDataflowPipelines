![Logo](logo.png)

# Simple TPL Dataflow Pipelines

This library helps at building simple [TPL Dataflow](https://docs.microsoft.com/en-us/dotnet/standard/parallel-programming/dataflow-task-parallel-library) pipelines,
that enforce the following guarantees:

1. In case any constituent block fails, all other blocks should complete as soon as possible.
Under no circumstances a single failed block should result in a deadlocked pipeline.
2. When a pipeline as a whole completes either successfully or with an error, all of its
constituent blocks are also completed. No active fire-and-forget blocks are left behind.
3. The [`Completion`](https://docs.microsoft.com/en-us/dotnet/api/system.threading.tasks.dataflow.idataflowblock.completion)
of the pipeline propagates all errors that may have occurred in all blocks,
accumulated inside a flat [`AggregateException`](https://docs.microsoft.com/en-us/dotnet/api/system.aggregateexception).

This StackOverflow question provides a deeper insight about the problem that this library
attempts to solve: [TPL Dataflow exception in transform block with bounded capacity](https://stackoverflow.com/questions/21603428/tpl-dataflow-exception-in-transform-block-with-bounded-capacity).

## How to make a pipeline

At first you instantiate the individual [dataflow blocks](https://docs.microsoft.com/en-us/dotnet/api/system.threading.tasks.dataflow), as usual. Any built-in or custom
dataflow block can be part of the pipeline. This library helps only at linking the blocks,
not at creating them.

After all the dataflow blocks are created, you use the static `PipelineBuilder.BeginWith`
method in order to start building the pipeline, specifying the first dataflow block.
This will be the entry point of the pipeline. Then you call the `LinkTo` method to add
more blocks in the pipeline. The `LinkTo` invocations should be chained, because
they don't modify the current builder. Instead they return a new builder each time. Finally,
when all the dataflow blocks have been added, you call the `ToPipeline` method that
physically links the blocks, and composes the pipeline. Example:

```C#
var block1 = new TransformManyBlock<string, string>(
    folderPath => Directory.EnumerateFiles(folderPath));
var block2 = new TransformBlock<string, (string, int)>(
    filePath => (filePath, File.ReadLines(filePath).Count()));
var block3 = new ActionBlock<(string, int)>(
    e => Console.WriteLine($"{Path.GetFileName(e.Item1)} has {e.Item2} lines"));

var pipeline = PipelineBuilder // This pipeline is a ITargetBlock<string>
    .BeginWith(block1)
    .LinkTo(block2)
    .LinkTo(block3)
    .ToPipeline();

pipeline.Post(@"C:\Users\Public\Documents");
pipeline.Complete();
await pipeline.Completion;
```

After the pipeline has been created, it now owns all the dataflow blocks
from which it is composed. You don't need to interact with the individual blocks any longer.
The pipeline represents them as a whole. The pipeline is a [`ITargetBlock<T>`](https://docs.microsoft.com/en-us/dotnet/api/system.threading.tasks.dataflow.itargetblock-1) that can
receive messages, and potentially also a [`ISourceBlock<T>`](https://docs.microsoft.com/en-us/dotnet/api/system.threading.tasks.dataflow.isourceblock-1) that can emit messages.
Whether it can emit messages depends on the type of the last block added in the pipeline.

## Discussion

It might be helpful to compare the functionality offered by this library with the
functionality offered by the [`DataflowBlock.Encapsulate`](https://docs.microsoft.com/en-us/dotnet/api/system.threading.tasks.dataflow.dataflowblock.encapsulate) method.
The result of this method is similar with the result of the `ToPipeline` method: both return
an [`IPropagatorBlock<TInput, TOutput>`](https://docs.microsoft.com/en-us/dotnet/api/system.threading.tasks.dataflow.ipropagatorblock-2) implementation
(a block that is both a target and a source). The `DataflowBlock.Encapsulate`
accepts a `target` and a `source` block, and returns a propagator that delegates to
these two blocks. The two blocks are not linked automatically in any way, and the completion
of the propagator represents the completion of the second (the `source`) block only.
On the contrary the `ToPipeline` returns a propagator that links all the dataflow
blocks tightly in both directions, and its `Completion` represents the completion of all its
constituent blocks, not just the last one.

It should be noted that the `IPropagatorBlock<TInput, TOutput>` returned by both of
these approaches also implements the [`IReceivableSourceBlock<TOutput>`](https://docs.microsoft.com/en-us/dotnet/api/system.threading.tasks.dataflow.ireceivablesourceblock-1) interface,
in exactly the same way. Casting the propagator to this interface always succeeds
(provided that the `TOutput` has the correct type). Invoking the
`TryReceive`/`TryReceiveAll` methods returns `true` if the underlying dataflow block implements
this interface, and also has at least one available message to emit. Example:

```C#
var receivable = (IReceivableSourceBlock<string>)pipeline;
bool received = receivable.TryReceive(out string item);
```

## Embedding the library into your project

This library has no NuGet package. You can either [download](https://github.com/theodorzoulias/SimpleTplDataflowPipelines/releases) the project and build it locally, or just
embed the single code file [`PipelineBuilder.cs`](https://github.com/theodorzoulias/SimpleTplDataflowPipelines/blob/main/src/SimpleTplDataflowPipelines/PipelineBuilder.cs) (~300 lines of code) into your project.
This library has been tested on the .NET Core 3.0 and .NET 5 platforms.

## Performance

The pipelines created with the help of this library, are neither slower or faster that
the pipelines created manually by using the [`LinkTo`](https://docs.microsoft.com/en-us/dotnet/api/system.threading.tasks.dataflow.dataflowblock.linkto) method. This library has not been
micro-optimized regarding the allocation of the few, small, short-lived objects that are
created during the construction of a pipeline. The emphasis has been put on simplicity,
readability and correctness, than on writing the most GC-friendy code possible.
