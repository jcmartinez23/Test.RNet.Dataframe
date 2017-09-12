using Newtonsoft.Json;
using RDotNet;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;

namespace Test.RNet.DataFrame
{
    class Program
    {
        static void Main(string[] args)
        {
            var dataFilePath = ConfigurationManager.AppSettings["dataFilePath"];
            var json = File.ReadAllText(dataFilePath);

            var dataCols = JsonConvert.DeserializeObject<IEnumerable<string[]>>(json).ToList();

            var rhome = ConfigurationManager.AppSettings["rHome"];
            var rPath = rhome + @"\bin\x64";

            REngine.SetEnvironmentVariables(rPath, rhome);
            var engine = REngine.GetInstance();
            engine.AutoPrint = false;
            engine.Evaluate("rxSetComputeContext(\"localpar\")");
            engine.Evaluate("workingDirectory <- getwd()");

            var totalPreDataFrame = dataCols[1].ToList().Count;
            Console.WriteLine($"Total Pre-Dataframe: {totalPreDataFrame}");

            var duplicatedPreDataFrame = totalPreDataFrame - dataCols[1].GroupBy(a => a).Where(a => a.Count() == 1).ToList().Count;
            Console.WriteLine($"Duplicated Pre-Dataframe: {duplicatedPreDataFrame}");

            for (int i = 0; i < 6; i++)
            {
                var dataFrame = engine.CreateDataFrame(dataCols.ToArray(), new string[] { "Col1", "Col2", "Col3", "Col4", "Col5", "Col6", "Col7", "Col8", "Col9" }, stringsAsFactors: false);

                var count = dataFrame[1].Length;
                Console.WriteLine($"{i} - Total Dataframe: {count}");

                var groupedCols = dataFrame[1].Distinct().ToList();
                Console.WriteLine($"{i} - Distinct Col2: {groupedCols.Count}");

                engine.SetSymbol($"df{i}", dataFrame);
                engine.Evaluate($"print(length(df{i}$Col2))");
                engine.Evaluate($"print(length(unique(df{i}$Col2)))");
            }
            Console.ReadLine();
        }
    }
}
