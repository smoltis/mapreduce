using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;

namespace MapReduceReadFile
{
    class Program
    {
        private static string OutputFolder { get; set; }

        static void Main(string[] args)
        {

            var sw = System.Diagnostics.Stopwatch.StartNew();

            string infile, prodfile;


            ProcessCmdLineArgs(args, out infile, out prodfile);

            var globalSalesStats = new SalesStats();

            sw.Restart();

            #region MapReduce
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
                    
                    var basket = Tuple.Create(
                        //issues: some baskets have different timestamps within one transaction; duplicates that differ by sales date only; transactionId is not unique
                        salesLine.StoreCode,
                        salesLine.TillNo,
                        salesLine.TransactionId,
                        salesLine.SalesDate);
                    var key = basket.GetHashCode(); //basketid
                    //count DistinctBasketsPerStore
                    if (!localSalesStats.DistinctBasketsPerStore.ContainsKey(key))
                    {
                        localSalesStats.DistinctBasketsPerStore.Add(key,salesLine.StoreCode);
                    }
                    
                    if (!localSalesStats.BasketItems.ContainsKey(key))
                    {
                        localSalesStats.BasketItems.Add(key, new List<int> { salesLine.ProductCode });
                    }
                    else
                    {
                        localSalesStats.BasketItems[key].Add(salesLine.ProductCode);
                    }

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
                        //Distinct Baskets Per Store
                        foreach (var key in localSalesStats.DistinctBasketsPerStore.Keys)
                        {
                            var value = localSalesStats.DistinctBasketsPerStore[key];
                            if (!globalSalesStats.DistinctBasketsPerStore.ContainsKey(key))
                            {
                                globalSalesStats.DistinctBasketsPerStore.Add(key, value);
                            }
                        }
                        
                        //Total SalesPerStore
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

                        //BasketItems
                        foreach (var key in localSalesStats.BasketItems.Keys)
                        {
                            var value = localSalesStats.BasketItems[key]; //list
                            if (!globalSalesStats.BasketItems.ContainsKey(key))
                            {
                                globalSalesStats.BasketItems.Add(key, value);
                            }
                            else
                            {
                                globalSalesStats.BasketItems[key].AddRange(value);
                            }
                        }

                    }
                }
            );
            #endregion

            //Parallel.ForEach(File.ReadLines(prodfile), line =>
            //{
            //    var productDetails = ParseProductDetails(line);
            //    if (!globalSalesStats.ProductDetails.ContainsKey(productDetails.ProductCode))
            //    {
            //        globalSalesStats.ProductDetails.Add(productDetails.ProductCode, productDetails);
            //    }
            //});

            DisplayDistinctStores(globalSalesStats);
            DisplayDistinctStoresWithSales(globalSalesStats);
            DisplayBasketsPerStore(globalSalesStats);

            SaveStoresToFile(globalSalesStats);
            SaveBasketsToFile(globalSalesStats);

            long timems = sw.ElapsedMilliseconds;
            double time = timems / 1000.0;  // convert milliseconds to secs

            AppendFile(string.Empty);
            var stats = $"** Done! Time: {time:0.000} secs";
            AppendFile(stats);

            Console.WriteLine();
            Console.WriteLine();
            Console.Write("Press a key to exit...");
            Console.ReadKey();
        }

        private static void SaveStoresToFile(SalesStats globalSalesStats)
        {
            var outputFile = Path.Combine(OutputFolder, StringConstants.DefaultStoresFile);
            using (var file = new StreamWriter(outputFile, true))
            {
                foreach (var store in globalSalesStats.Stores)
                {
                    var line = $"{store.Key}|{store.Value}";
                    file.WriteLineAsync(line);
                }
                
            }
        }

        private static void DisplayDistinctStores(SalesStats globalSalesStats)
        {
            AppendFile(string.Empty);
            AppendFile($"** Stores by StoreCode:");

            foreach (var store in globalSalesStats.Stores)
            {
                var line = $"{store.Key}: {store.Value}";
                AppendFile(line);
            }
        }

        private static void SaveBasketsToFile(SalesStats globalSalesStats)
        {
            //use format StoreCode,DistinctProductCodes

            var storeBasketItems = from basketItems in globalSalesStats.BasketItems
                join storeCodes in globalSalesStats.DistinctBasketsPerStore on basketItems.Key equals storeCodes.Key
                select new {StoreCode = storeCodes.Value, Items = string.Join(",", basketItems.Value.ToArray())};

            foreach (var basket in storeBasketItems.ToList())
            {
                var newLine = $"{basket.StoreCode}|{basket.Items}";
                AppendBasketFile(newLine);
            }
            
            //save products
            //save departments
            //save categories
            //save subcategories

            //File.WriteAllLines();
        }

        private static void DeleteOldStatsFile()
        {
            var outputFile = Path.Combine(OutputFolder, StringConstants.DefaultOutFile);
            FileInfo fileInfo = new FileInfo(outputFile);
            if (fileInfo.Exists)
            {
                File.Delete(outputFile);
            }
        }

        private static void DisplayBasketsPerStore(SalesStats globalSalesStats)
        {
            var basketByStore = from allBaskets in globalSalesStats.DistinctBasketsPerStore
                                group allBaskets by allBaskets.Value into g
                                join stores in globalSalesStats.Stores on g.Key equals stores.Key
                                select new { StoreName = stores.Value, TotalBaskets = g.Count()};
            AppendFile(string.Empty);
            AppendFile($"** Total count of baskets/transactions by Store");
            foreach (var item in basketByStore.OrderByDescending(p => p.TotalBaskets).ToList())
                if (!string.IsNullOrEmpty(item.StoreName))
                {
                    var line = $"{item.StoreName}: {item.TotalBaskets}";
                    AppendFile(line);
                }
                    
        }

        private static void AppendFile(string line)
        {
            Console.WriteLine(line);
            var outputFile = Path.Combine(OutputFolder, StringConstants.DefaultOutFile);
            using (var file = new StreamWriter(outputFile, true))
            {
                file.WriteLineAsync(line);
            }
        }

        private static void AppendBasketFile(string line)
        {
            var outputFile = Path.Combine(OutputFolder, StringConstants.DefaultBasketsFile);
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
            AppendFile(string.Empty);
            AppendFile($"** Total amount of sales by Store");
            foreach (var item in sales.OrderByDescending(p => p.TotalSales).ToList())
                if (!string.IsNullOrEmpty(item.StoreName))
                {
                    var line = $"{item.StoreName}: {item.TotalSales:C}";
                    AppendFile(line);
                }
                    
        }

        private static ProductDetailsLine ParseProductDetails(string line)
        {
            char[] separators = {StringConstants.ColumnSeparator};
            string[] tokens = line.Split(separators);
            //ignore header
            if (Convert.ToString(tokens[1]) == StringConstants.ProductHeaderColumn)
            {
                return new ProductDetailsLine();
            }
            var productLine = new ProductDetailsLine()
            {
                ProductCode = Convert.ToInt32(tokens[0]),
                ProductDescription = Convert.ToString(tokens[1]),
                DepartmentDescription = Convert.ToString(tokens[2]),
                CategoryDescription = Convert.ToString(tokens[3]),
                SubCategoryDescription = Convert.ToString(tokens[4])
            };
            return productLine;
        }

        private static SalesLine ParseLine(string line)
        {
            char[] separators = { StringConstants.ColumnSeparator };

			string[] tokens = line.Split(separators);
            //ignore header
            if (Convert.ToString(tokens[1]) == StringConstants.SalesHeaderColumn)
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

        private static void ProcessCmdLineArgs(string[] args, out string infile, out string prodfile)
        {
            string version, platform;

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
            infile = StringConstants.DefaultInFile;
            prodfile = StringConstants.DefaultProductRefFile;

            if (args.Length == 0)
            {
                Console.WriteLine("Usage: mapreducereadfile.exe {sales_filename} {product_filename}");
                Console.WriteLine();
                Console.WriteLine();
                Console.WriteLine($"Using default filename: {infile}");
            }
            else if (args.Length == 2)
            {
                infile = args[1];
                prodfile = args[2];
            }

            if (!File.Exists(infile) || !File.Exists(prodfile))
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
            FileInfo fr = new FileInfo(prodfile);

            double sizeinMb = fi.Length / 1048576.0;
            double sizeMbRef = fr.Length / 1048576.0;
            OutputFolder = fi.DirectoryName;
            if (OutputFolder != null)
            {
                DeleteOldStatsFile();
            }
            var line = $"** Parallel, MapReduce Data Mining App by Stan Smoltis (c) 2017 [{platform}, {version}] **";
            AppendFile(line);
            line = $"   Transactions File: '{infile}'({sizeinMb:#,##0.00 MB})";
            AppendFile(line);
            AppendFile(string.Empty);
            line = $"   Items File: '{prodfile}'({sizeMbRef:#,##0.00 MB})";
            AppendFile(line);
        }
    }
}
