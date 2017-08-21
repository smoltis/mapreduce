using System;
using System.Collections.Generic;

namespace MapReduceReadFile
{
    public class SalesStats
    {
        public Dictionary<int,string> Stores { get; set; } = new Dictionary<int, string>();
        public Dictionary<int, int> DistinctBasketsPerStore { get; set; } = new Dictionary<int, int>();
        public Dictionary<int, double> SalesPerStore { get; set; } = new Dictionary<int, double>();
    }
}