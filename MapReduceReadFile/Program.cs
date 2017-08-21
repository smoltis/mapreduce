using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace MapReduceReadFile
{
    class Program
    {
        private static string OutputFolder { get; set; }
        private const string OutFile = "results.txt";
        static void Main(string[] args)
        {

            var sw = System.Diagnostics.Stopwatch.StartNew();

            string infile;

            ProcessCmdLineArgs(args, out infile);

            var globalSalesStats = new SalesStats();

            sw.Restart();

            Parallel.ForEach(File.ReadLines(infile),
                // Initializer:  create task-local storage:
                () => new SalesStats(),

                // Loop-body: mapping our results into the local storage
                (line, loopCtrl, localSalesStats) =>
                {
                    var salesLine = ParseLine(line);
                    if (salesLine == null)
                        return localSalesStats;

                    //distinct stores
                    if (!localSalesStats.Stores.ContainsKey(salesLine.StoreCode))
                    {
                    localSalesStats.Stores.Add(salesLine.StoreCode,salesLine.StoreName);
                    }

                    //count DistinctBasketsPerStore
                    localSalesStats.DistinctBasketsPerStore.Add(
                        new Tuple<int, int, int, DateTime>(
                            //issues: some baskets have different timestamps within one transaction; duplicates that differ by sales date only; transactionId is not unique
                            salesLine.StoreCode, 
                            salesLine.TillNo, 
                            salesLine.TransactionId,
                            salesLine.SalesDate));

                    //Total Sales per Store
                    if (!localSalesStats.SalesPerStore.ContainsKey(salesLine.StoreCode))
                    {
                        localSalesStats.SalesPerStore.Add(salesLine.StoreCode, salesLine.SalesValue);
                    }
                    else
                    {
                        localSalesStats.SalesPerStore[salesLine.StoreCode] += salesLine.SalesValue;
                    }

                    return localSalesStats;
                },
                // Finalizer: reduce(merge) individual local storage into global storage
                (localSalesStats) =>
                {
                    lock (globalSalesStats)
                    {
                        //distinct Stores
                        foreach (var key in localSalesStats.Stores.Keys)
                        {
                            var value = localSalesStats.Stores[key];
                            if (!globalSalesStats.Stores.ContainsKey(key))
                            {
                                globalSalesStats.Stores.Add(key, value);
                            }
                        }
                        //DistinctBasketsPerStore
                        globalSalesStats.DistinctBasketsPerStore.UnionWith(localSalesStats.DistinctBasketsPerStore);
                        
                        //SalesPerStore
                        foreach (var key in localSalesStats.SalesPerStore.Keys)
                        {
                            var value = localSalesStats.SalesPerStore[key];
                            if (!globalSalesStats.SalesPerStore.ContainsKey(key))
                            {
                                globalSalesStats.SalesPerStore.Add(key, value);
                            }
                            else
                            {
                                globalSalesStats.SalesPerStore[key] += value;
                            }
                        }
                    }
                }
            );


           
            DisplayDistinctStoresWithSales(globalSalesStats);
            DisplayBasketsPerStore(globalSalesStats);

            long timems = sw.ElapsedMilliseconds;
            double time = timems / 1000.0;  // convert milliseconds to secs

            Console.WriteLine();
            AppendFile(string.Empty);
            var stats = $"** Done! Time: {time:0.000} secs";
            Console.WriteLine(stats);
            AppendFile(stats);
            Console.WriteLine();
            Console.WriteLine();
            Console.Write("Press a key to exit...");
            Console.ReadKey();
        }

        private static void DeleteOldStatsFile()
        {
            var outputFile = Path.Combine(OutputFolder, OutFile);
            FileInfo fileInfo = new FileInfo(outputFile);
            if (fileInfo.Exists)
            {
                File.Delete(outputFile);
            }
        }

        private static void DisplayBasketsPerStore(SalesStats globalSalesStats)
        {
            var basketCount = new Dictionary<int,int>(); 
            //StoreCode and number of baskets
            foreach (var tuple in globalSalesStats.DistinctBasketsPerStore)
            {
                if (!basketCount.ContainsKey(tuple.Item1))
                    basketCount.Add(tuple.Item1, 1);
                else
                {
                    basketCount[tuple.Item1]++;
                }
            }
            var baskets = from allbaskets in basketCount
                join stores in globalSalesStats.Stores on allbaskets.Key equals stores.Key
                          select new {StoreName = stores.Value, TotalBaskets = allbaskets.Value};
            Console.WriteLine();
            Console.WriteLine($"** Total count of baskets/transactions by Store");
            AppendFile(string.Empty);
            AppendFile($"** Total count of baskets/transactions by Store");
            foreach (var item in baskets.OrderByDescending(p => p.TotalBaskets).ToList())
                if (!string.IsNullOrEmpty(item.StoreName))
                {
                    var line = $"{item.StoreName}: {item.TotalBaskets}";
                    Console.WriteLine(line);
                    AppendFile(line);
                }
                    
        }

        private static void AppendFile(string line)
        {
            var outputFile = Path.Combine(OutputFolder, OutFile);
            using (var file = new StreamWriter(outputFile, true))
            {
                file.WriteLineAsync(line);
            }
        }

        private static void DisplayDistinctStoresWithSales(SalesStats globalSalesStats)
        {
            var sales = from allsales in globalSalesStats.SalesPerStore
                join stores in globalSalesStats.Stores on allsales.Key equals stores.Key
                orderby allsales.Value descending
                select new {StoreName = stores.Value, TotalSales = allsales.Value};
            Console.WriteLine();
            AppendFile(string.Empty);
            Console.WriteLine($"** Total amount of sales by Store");
            AppendFile($"** Total amount of sales by Store");
            foreach (var item in sales.OrderByDescending(p => p.TotalSales).ToList())
                if (!string.IsNullOrEmpty(item.StoreName))
                {
                    var line = $"{item.StoreName}: {item.TotalSales:C}";
                    Console.WriteLine(line);
                    AppendFile(line);
                }
                    
        }

        private static SalesLine ParseLine(string line)
        {
            char[] separators = { '|' };

			string[] tokens = line.Split(separators);
            //ignore header
            if (Convert.ToString(tokens[1]) == "StoreName")
            {
                return new SalesLine();
            }

            var salesLine = new SalesLine()
            {
                //gotta be a fortune teller to work out data type from 19M+ strings ;)
                StoreCode = Convert.ToInt32(tokens[0]),
                StoreName = Convert.ToString(tokens[1]),
                ProductCode = Convert.ToInt32(tokens[2]),
                ItemSeqNo = Convert.ToInt32(tokens[3]),
                UnitPrice = Convert.ToDouble(tokens[4]),
                Quantity = Convert.ToDouble(tokens[5]),
                SalesValue = Convert.ToDouble(tokens[6]),
                Weighted = Convert.ToBoolean(tokens[7]),
                TillNo = Convert.ToInt32(tokens[8]),
                TransactionId = Convert.ToInt32(tokens[9]),
                SalesDate = Convert.ToDateTime(tokens[10])
            };

            return salesLine;
        }

        private static void ProcessCmdLineArgs(string[] args, out string infile)
        {
            String version, platform;

#if DEBUG
            version = "debug";
#else
			version = "release";
#endif

#if _WIN64
	platform = "64-bit";
#elif _WIN32
	platform = "32-bit";
#else
            platform = "any-cpu";
#endif

            //
            // Defaults:
            //
            infile = @"C:\DataTest\TestData\TestData1.csv";

            if (args.Length == 0)
            {
                Console.WriteLine("Usage: mapreducereadfile.exe {filename}");
                Console.WriteLine();
                Console.WriteLine();
                Console.WriteLine($"Using default filename: {infile}");
            }
            else
            {
                infile = args[1];
            }

            if (!File.Exists(infile))
            {
                Console.WriteLine($"** Error: file '{infile}' does not exist.");
                Console.WriteLine();
                Console.WriteLine();
                Environment.Exit(-1);
            }

            //
            // Process command-line args to get infile:
            //
            
            FileInfo fi = new FileInfo(infile);
            double sizeinMb = fi.Length / 1048576.0;
            OutputFolder = fi.DirectoryName;
            if (OutputFolder != null)
            {
                DeleteOldStatsFile();
            }
            var line = $"** Parallel, MapReduce Data Mining App by Stan Smoltis (c) 2017 [{platform}, {version}] **";
            Console.WriteLine(line);
            AppendFile(line);
            line = $"   File: '{infile}'({sizeinMb:#,##0.00 MB})";
            Console.Write(line);
            AppendFile(line);
            Console.WriteLine();
            AppendFile(string.Empty);
        }
    }
}
