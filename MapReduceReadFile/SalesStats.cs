using System;
using System.Collections.Generic;

namespace MapReduceReadFile
{
    public class SalesStats
    {
        public Dictionary<int,string> Stores { get; set; } = new Dictionary<int, string>();
        //public HashSet<Tuple<int, int, int, DateTime>> DistinctBasketsPerStore { get; set; } = new HashSet<Tuple<int, int, int, DateTime>>();
        public Dictionary<int, int> DistinctBasketsPerStore { get; set; } = new Dictionary<int, int>(); //basketCode,StoreCode
        public Dictionary<int, double> SalesPerStore { get; set; } = new Dictionary<int, double>();
        public Dictionary<int, List<int>> BasketItems { get; set; } = new Dictionary<int, List<int>>(); //basketCode,ProductsList
    }
}