﻿using System;
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

                //Shows which metrics are different
                //Query.WhichMetricsDifferent();

                Console.WriteLine("Elapsed: " + sw.Elapsed);
                sw.Restart();

                if (!Query.AreMetricsEqual())
                    Console.WriteLine("Metric sums are not equal.");
                else Console.WriteLine("Metric sums are equal");

                //Console.WriteLine("Elapsed: " + sw.Elapsed + "\r\n");
                //sw.Restart();

                //Console.WriteLine("========== Row class differences ==========\r\n");
                //Query.RowDiscrepancyPrint();

                //Console.WriteLine(" ========== Elapsed: " + sw.Elapsed + "==========\r\n");
                //sw.Restart();

                //Console.WriteLine("========== String differences ==========\r\n");
                //Query.StringDiscrepancyPrint(SqlData.SelectAllFrom1Query, SqlData.SelectAllFrom2Query);

                //Console.WriteLine(" ========== Elapsed: " + sw.Elapsed + "==========\r\n");
                //sw.Restart();

                Console.WriteLine("========== Checksum differences ==========\r\n");
                Query.StringDiscrepancyPrint(SqlData.SelectChecksumFrom1Query, SqlData.SelectChecksumFrom2Query);

                Console.WriteLine(" ========== Elapsed: " + sw.Elapsed + "==========\r\n");
                sw.Restart();

                //Console.WriteLine("========== Binary checksum differences ==========\r\n");
                //Query.StringDiscrepancyPrint(SqlData.SelectBinaryChecksumFrom1Query, SqlData.SelectBinaryChecksumFrom2Query);

                //Console.WriteLine(" ========== Elapsed: " + sw.Elapsed + "==========\r\n");
                //sw.Restart();

                //Console.WriteLine("========== Hashbytes differences ==========\r\n");
                //Query.StringDiscrepancyPrint(SqlData.SelectHashbytes1Query, SqlData.SelectHashbytes2Query);

                //Console.WriteLine(" ========== Elapsed: " + sw.Elapsed + "==========\r\n");
            }
            else
            {
                Console.WriteLine("Table checksums equal.");
            }
            Query.CloseConnections();
            overwatch.Stop();
            Console.WriteLine("\r\n === Overall elapsed: " + overwatch.Elapsed + " ===");
            Console.ReadLine();
        }
    }
}
