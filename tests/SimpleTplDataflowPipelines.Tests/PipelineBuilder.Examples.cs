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
        //[TestMethod]
        public async Task Example()
        {
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
        }
    }
}
