using System;
using System.Diagnostics;

namespace Comperator
{
    class Program
    {
        static void Main(string[] args)
        {
            var sw = new Stopwatch();
            var overwatch = new Stopwatch();
            overwatch.Start();
            sw.Start();
            if (!Query.AreChecksumsEqual())
            {
                Console.WriteLine("Table checksums are not equal.");

                // Shows which metrics are different
                //Query.WhichMetricsDifferent();

                //Console.WriteLine("Elapsed: " + sw.Elapsed);
                //sw.Restart();

                //if (!Query.AreMetricsEqual())
                //    Console.WriteLine("Metric sums are not equal.");
                //else Console.WriteLine("Metric sums are equal");

                //Console.WriteLine("Elapsed: " + sw.Elapsed + "\r\n");
                //sw.Restart();

                //Console.WriteLine("========== Row class differences ==========\r\n");
                //Query.RowDiscrepancyPrint();

                //Console.WriteLine(" ========== Elapsed: " + sw.Elapsed + "==========\r\n");
                //sw.Restart();

                //Console.WriteLine("========== String differences ==========\r\n");
                //Query.StringDiscrepancyPrint(Query.SelectAllFrom1Query, Query.SelectAllFrom2Query);

                //Console.WriteLine(" ========== Elapsed: " + sw.Elapsed + "==========\r\n");
                //sw.Restart();

                Console.WriteLine("========== Checksum differences ==========\r\n");
                Query.StringDiscrepancyPrint(Query.SelectChecksumFrom1Query, Query.SelectChecksumFrom2Query);

                Console.WriteLine(" ========== Elapsed: " + sw.Elapsed + "==========\r\n");
                sw.Restart();

                //Console.WriteLine("========== Binary checksum differences ==========\r\n");
                //Query.StringDiscrepancyPrint(Query.SelectBinaryChecksumFrom1Query, Query.SelectBinaryChecksumFrom2Query);

                //Console.WriteLine(" ========== Elapsed: " + sw.Elapsed + "==========\r\n");
                //sw.Restart();

                //Console.WriteLine("========== Hashbytes differences ==========\r\n");
                //Query.StringDiscrepancyPrint(Query.SelectHashbytes1Query, Query.SelectHashbytes2Query);

                //Console.WriteLine(" ========== Elapsed: " + sw.Elapsed + "==========\r\n");
            }
            else
            {
                Console.WriteLine("Table checksums equal.");
            }

            overwatch.Stop();
            Console.WriteLine("\r\n === Overall elapsed: " + overwatch.Elapsed + " ===");
            Console.ReadLine();
        }
    }
}
