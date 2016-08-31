using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Betkas
{
    class Program
    {

        private static string query1 =
            "select * from web_transactionDays1";
        private static string query2 =
           "select * from web_transactionDays2";

        private static string query3 = " (SELECT \'Table1\' as TableID,  CHECKSUM(*) as CheckSum, * \r\nFROM web_transactionDays1\r\n  WHERE CHECKSUM(*) NOT IN\r\n    (\r\n    SELECT CHECKSUM(*)\r\n        FROM web_transactionDays2\r\n       ))\r\nORDER by CheckSum";
        private static string query4 = "(SELECT \'Table2\' as TableID, CHECKSUM(*) as CheckSum, * \r\nFROM web_transactionDays2\r\n  WHERE CHECKSUM(*) NOT IN\r\n    (\r\n    SELECT CHECKSUM(*)\r\n        FROM web_transactionDays1\r\n       ))\r\nORDER by CheckSum";

        private static string query5 = " (SELECT \'Table1\' as TableID,  BINARY_CHECKSUM(*) as BINARY_CHECKSUM, * \r\nFROM web_transactionDays1\r\n  WHERE BINARY_CHECKSUM(*) NOT IN\r\n    (\r\n    SELECT BINARY_CHECKSUM(*)\r\n        FROM web_transactionDays2\r\n       ))\r\nORDER by BINARY_CHECKSUM";
        private static string query6 = "(SELECT \'Table2\' as TableID, BINARY_CHECKSUM(*) as BINARY_CHECKSUM, * \r\nFROM web_transactionDays2\r\n  WHERE BINARY_CHECKSUM(*) NOT IN\r\n    (\r\n    SELECT BINARY_CHECKSUM(*)\r\n        FROM web_transactionDays1\r\n       ))\r\nORDER by BINARY_CHECKSUM";

        private static string query7 = "SELECT HASHBYTES(\'SHA1\', \r\nCAST (ISNULL(logtime,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(transactionType,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Campaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_Logpoints,\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(bannerId,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Sale,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Total,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_AffiliateBanners,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_BannerClickURLs,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(RecentAdInteraction,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AdInteractionTransactionType,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AllSitesUniqueDay,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointUniqueSession,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointRepeat,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AllSitesUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(SiteUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueOrderID,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueSession,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(CAST(WinningPrice as varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(RtbPartyId,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(CAST( WinningCost AS varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(CAST(WinningCostWithAddedFee AS varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(TestID,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ulIsInScreen,\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(CAST( BrandSafetyCost as varchar(18)),\'\') AS VARBINARY(MAX))\r\n  )\r\n as HashedValue, * \r\nFROM web_transactionDays1\r\n  WHERE HASHBYTES(\'SHA1\', \r\nCAST (ISNULL(logtime,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(transactionType,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Campaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_Logpoints,\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(bannerId,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Sale,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Total,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_AffiliateBanners,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_BannerClickURLs,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(RecentAdInteraction,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AdInteractionTransactionType,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AllSitesUniqueDay,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointUniqueSession,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointRepeat,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AllSitesUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(SiteUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueOrderID,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueSession,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(CAST(WinningPrice as varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(RtbPartyId,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(CAST( WinningCost AS varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(CAST(WinningCostWithAddedFee AS varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(TestID,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ulIsInScreen,\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(CAST( BrandSafetyCost as varchar(18)),\'\') AS VARBINARY(MAX))\r\n  ) NOT IN\r\n    (\r\n    SELECT HASHBYTES(\'SHA1\', \r\nCAST (ISNULL(logtime,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(transactionType,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Campaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_Logpoints,\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(bannerId,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Sale,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Total,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_AffiliateBanners,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_BannerClickURLs,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(RecentAdInteraction,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AdInteractionTransactionType,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AllSitesUniqueDay,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointUniqueSession,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointRepeat,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AllSitesUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(SiteUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueOrderID,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueSession,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(CAST(WinningPrice as varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(RtbPartyId,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(CAST( WinningCost AS varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(CAST(WinningCostWithAddedFee AS varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(TestID,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ulIsInScreen,\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(CAST( BrandSafetyCost as varchar(18)),\'\') AS VARBINARY(MAX))\r\n  )\r\n   FROM web_transactionDays2\r\n)";
        private static string query8 = "SELECT HASHBYTES(\'SHA1\', \r\nCAST (ISNULL(logtime,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(transactionType,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Campaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_Logpoints,\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(bannerId,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Sale,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Total,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_AffiliateBanners,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_BannerClickURLs,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(RecentAdInteraction,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AdInteractionTransactionType,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AllSitesUniqueDay,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointUniqueSession,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointRepeat,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AllSitesUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(SiteUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueOrderID,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueSession,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(CAST(WinningPrice as varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(RtbPartyId,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(CAST( WinningCost AS varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(CAST(WinningCostWithAddedFee AS varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(TestID,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ulIsInScreen,\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(CAST( BrandSafetyCost as varchar(18)),\'\') AS VARBINARY(MAX))\r\n  )\r\n as HashedValue, * \r\nFROM web_transactionDays2\r\n  WHERE HASHBYTES(\'SHA1\', \r\nCAST (ISNULL(logtime,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(transactionType,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Campaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_Logpoints,\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(bannerId,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Sale,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Total,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_AffiliateBanners,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_BannerClickURLs,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(RecentAdInteraction,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AdInteractionTransactionType,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AllSitesUniqueDay,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointUniqueSession,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointRepeat,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AllSitesUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(SiteUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueOrderID,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueSession,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(CAST(WinningPrice as varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(RtbPartyId,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(CAST( WinningCost AS varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(CAST(WinningCostWithAddedFee AS varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(TestID,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ulIsInScreen,\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(CAST( BrandSafetyCost as varchar(18)),\'\') AS VARBINARY(MAX))\r\n  ) NOT IN\r\n    (\r\n    SELECT HASHBYTES(\'SHA1\', \r\nCAST (ISNULL(logtime,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(transactionType,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Campaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_Logpoints,\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(bannerId,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Sale,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Total,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_AffiliateBanners,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_BannerClickURLs,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(RecentAdInteraction,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AdInteractionTransactionType,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AllSitesUniqueDay,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointUniqueSession,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointRepeat,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AllSitesUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(SiteUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueOrderID,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueSession,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(CAST(WinningPrice as varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(RtbPartyId,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(CAST( WinningCost AS varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(CAST(WinningCostWithAddedFee AS varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(TestID,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ulIsInScreen,\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(CAST( BrandSafetyCost as varchar(18)),\'\') AS VARBINARY(MAX))\r\n  )\r\n   FROM web_transactionDays1\r\n)";

        private static string metrics =
            "SELECT SUM(Sale) + SUM(Total) + SUM(WinningCost) + SUM(WinningPrice) + SUM(WinningCostWithAddedFee) + SUM(BrandSafetyCost) as Sumall FROM web_transactionDays1\r\nEXCEPT\r\nSELECT SUM(Sale) + SUM(Total) + SUM(WinningCost) + SUM(WinningPrice) + SUM(WinningCostWithAddedFee) + SUM(BrandSafetyCost) as Sumall FROM web_transactionDays2";

        private static string tableChecksumQueryString =
            "SELECT CHECKSUM_AGG(CHECKSUM(*)) FROM web_transactionDays1 EXCEPT SELECT CHECKSUM_AGG(CHECKSUM(*)) FROM web_transactionDays2";

        private static string GetConnectionString()
        {
            return "Data Source=fedw.dwhinfra.d1.adform.zone,35352;Initial Catalog=DiscrepancyTest;Persist Security Info=True;User ID=discrepancy;Password=discrepancy; ";
        }
        static void Main(string[] args)
        {
            if (!AreChecksumsEqual())
            {
                Console.WriteLine("Table checksums are not equal.");
                if (!AreMetricsEqual())
                {
                    Console.WriteLine("Metric sums are not equal.");
                }
                Query.ShowDifferences();
            }
            Console.ReadLine();
            // CheckSum();
            // BinaryCheckSum();
            // Substring();
            // HashByte();
        }

        private static bool AreChecksumsEqual()
        {
            using (SqlConnection con = new SqlConnection(GetConnectionString()))
            {
                try
                {
                    con.Open();
                    Console.WriteLine("Comparing checksums of tables...");
                    return Query.GetStringList(tableChecksumQueryString, con).Count == 0;
                }
                catch (Exception e)
                {
                    Console.WriteLine("Error when comparing table checksums: " + e);
                    throw;
                }
            }
        }

        public static bool AreMetricsEqual()
        {
            List<string> tableStrings1 = new List<string>();
            string connectionString = GetConnectionString();

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    Console.WriteLine("Comparing sums of all metrics...");
                    tableStrings1 = Query.GetStringList(metrics, connection);
                }
                catch (Exception exc)
                {
                    Console.WriteLine(exc);
                }

            }
            return tableStrings1.Count == 0;
        }


        public static void HashByte()
        {
            List<string> tableStrings1 = new List<string>();
            List<string> tableStrings2 = new List<string>();
            string connectionString = GetConnectionString();

            Stopwatch timer = new Stopwatch();
            timer.Start();
            Parallel.Invoke(() =>
            {

                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    try
                    {
                        connection.Open();
                        Console.WriteLine("Query7 started please wait");
                        tableStrings1 = Query.GetStringList(query7, connection);
                    }
                    catch (Exception exc)
                    {
                        Console.WriteLine(exc);
                    }

                }
            }, () =>
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    try
                    {
                        Console.WriteLine("Query8 started please wait");
                        connection.Open();
                        tableStrings2 = Query.GetStringList(query8, connection);
                    }
                    catch (Exception exc)
                    {
                        Console.WriteLine(exc);
                    }

                }
            });
            timer.Stop();

            IEnumerable<string> unitedTable = tableStrings1.Union(tableStrings2);

            Console.WriteLine("Data in first Table but not in second Table:");
            foreach (var n in unitedTable)
            {
                Console.WriteLine(n);
            }

            Console.WriteLine(timer.Elapsed);


            Console.ReadKey();
        }

        public static void BinaryCheckSum()
        {
            List<string> tableStrings1 = new List<string>();
            List<string> tableStrings2 = new List<string>();
            string connectionString = GetConnectionString();

            Stopwatch timer = new Stopwatch();
            timer.Start();
            Parallel.Invoke(() =>
            {

                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    try
                    {
                        connection.Open();
                        Console.WriteLine("Query5 started please wait");
                        tableStrings1 = Query.GetStringList(query5, connection);
                    }
                    catch (Exception exc)
                    {
                        Console.WriteLine(exc);
                    }

                }
            }, () =>
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    try
                    {
                        Console.WriteLine("Query6 started please wait");
                        connection.Open();
                        tableStrings2 = Query.GetStringList(query6, connection);
                    }
                    catch (Exception exc)
                    {
                        Console.WriteLine(exc);
                    }

                }
            });
            timer.Stop();

            IEnumerable<string> unitedTable = tableStrings1.Union(tableStrings2);

            Console.WriteLine("Data in first Table but not in second Table:");
            foreach (var n in unitedTable)
            {
                Console.WriteLine(n);
            }

            Console.WriteLine(timer.Elapsed);


            Console.ReadKey();
        }

        public static void CheckSum()
        {
            List<string> tableStrings1 = new List<string>();
            List<string> tableStrings2 = new List<string>();
            string connectionString = GetConnectionString();

            Stopwatch timer = new Stopwatch();
            timer.Start();
            Parallel.Invoke(() =>
            {

                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    try
                    {
                        connection.Open();
                        Console.WriteLine("Query3 started please wait");
                        tableStrings1 = Query.GetStringList(query3, connection);
                    }
                    catch (Exception exc)
                    {
                        Console.WriteLine(exc);
                    }

                }
            }, () =>
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    try
                    {
                        Console.WriteLine("Query4 started please wait");
                        connection.Open();
                        tableStrings2 = Query.GetStringList(query4, connection);
                    }
                    catch (Exception exc)
                    {
                        Console.WriteLine(exc);
                    }

                }
            });
            timer.Stop();

            IEnumerable<string> unitedTable = tableStrings1.Union(tableStrings2);

            Console.WriteLine("Data in first Table but not in second Table:");
            foreach (var n in unitedTable)
            {
                Console.WriteLine(n);
            }

            Console.WriteLine(timer.Elapsed);


            Console.ReadKey();
        }

        public static void Substring()
        {
            List<string> tableStrings1 = new List<string>();
            List<string> tableStrings2 = new List<string>();
            string connectionString = GetConnectionString();

            Stopwatch timer = new Stopwatch();
            timer.Start();
            Parallel.Invoke(() =>
            {

                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    try
                    {
                        connection.Open();
                        Console.WriteLine("Query started please wait");
                        tableStrings1 = Query.GetStringList(query1, connection);
                    }
                    catch (Exception exc)
                    {
                        Console.WriteLine(exc);
                    }

                }
            }, () =>
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    try
                    {
                        Console.WriteLine("Query2 started please wait");
                        connection.Open();
                        tableStrings2 = Query.GetStringList(query2, connection);
                    }
                    catch (Exception exc)
                    {
                        Console.WriteLine(exc);
                    }

                }
            });
            timer.Stop();

            IEnumerable<string> aOnlyTable1 = tableStrings1.Except(tableStrings2);
            IEnumerable<string> aOnlyTable2 = tableStrings2.Except(tableStrings1);


            Console.WriteLine("Data in first Table but not in second Table:");
            foreach (var n in aOnlyTable1)
            {
                Console.WriteLine(n);
            }
            Console.WriteLine("Data in second Table but not in first Table:");
            foreach (var n in aOnlyTable2)
            {
                Console.WriteLine(n);
            }

            Console.WriteLine(timer.Elapsed);


            Console.ReadKey();
        }
    }
}
