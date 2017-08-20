using System;

namespace MapReduceReadFile
{
    public class SalesLine
    {
        public int StoreCode { get; set; }
        public string StoreName { get; set; }
        public int ProductCode { get; set; }
        public int ItemSeqNo { get; set; }
        public double UnitPrice { get; set; }
        public double Quantity { get; set; }
        public double SalesValue { get; set; }
        public bool Weighted { get; set; }
        public int TillNo { get; set; }
        public int TransactionId { get; set; }
        public DateTime SalesDate { get; set; }
    }
}