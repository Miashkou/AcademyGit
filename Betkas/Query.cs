using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Betkas;
using Dapper;

namespace Comperator
{
    public class Query
    {
        public static string SelectAllFrom1Query =
            "SELECT * FROM web_transactionDays1";

        public static string SelectAllFrom2Query =
            "SELECT * FROM web_transactionDays2";

        public static string SelectChecksumFrom1Query = "SELECT CHECKSUM(*) as CheckSum, * \r\nFROM web_transactionDays1";
        public static string SelectChecksumFrom2Query = "SELECT CHECKSUM(*) as CheckSum, * \r\nFROM web_transactionDays2";

        public static string SelectBinaryChecksumFrom1Query = "SELECT BINARY_CHECKSUM(*) as BINARY_CHECKSUM, * \r\nFROM web_transactionDays1";
        public static string SelectBinaryChecksumFrom2Query = "SELECT BINARY_CHECKSUM(*) as BINARY_CHECKSUM, * \r\nFROM web_transactionDays2";

        public static string SelectHashbytes1Query = "SELECT HASHBYTES(\'SHA1\', \r\nCAST (ISNULL(logtime,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(transactionType,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Campaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_Logpoints,\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(bannerId,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Sale,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Total,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_AffiliateBanners,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_BannerClickURLs,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(RecentAdInteraction,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AdInteractionTransactionType,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AllSitesUniqueDay,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointUniqueSession,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointRepeat,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AllSitesUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(SiteUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueOrderID,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueSession,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(CAST(WinningPrice as varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(RtbPartyId,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(CAST( WinningCost AS varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(CAST(WinningCostWithAddedFee AS varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(TestID,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ulIsInScreen,\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(CAST( BrandSafetyCost as varchar(18)),\'\') AS VARBINARY(MAX))\r\n  )\r\n as HashedValue, * \r\nFROM web_transactionDays1";
        public static string SelectHashbytes2Query = "SELECT HASHBYTES(\'SHA1\', \r\nCAST (ISNULL(logtime,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(transactionType,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Campaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_Logpoints,\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(bannerId,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Sale,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Total,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_AffiliateBanners,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_BannerClickURLs,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(RecentAdInteraction,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AdInteractionTransactionType,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AllSitesUniqueDay,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointUniqueSession,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointRepeat,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AllSitesUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(SiteUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueOrderID,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueSession,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(CAST(WinningPrice as varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(RtbPartyId,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(CAST( WinningCost AS varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(CAST(WinningCostWithAddedFee AS varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(TestID,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ulIsInScreen,\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(CAST( BrandSafetyCost as varchar(18)),\'\') AS VARBINARY(MAX))\r\n  )\r\n as HashedValue, * \r\nFROM web_transactionDays2";

        public static string MetricsQuery =
            @"SELECT SUM(Sale) + SUM(Total) + SUM(WinningCost) + SUM(WinningPrice) + SUM(WinningCostWithAddedFee) + SUM(BrandSafetyCost) as Sumall FROM web_transactionDays1
            EXCEPT
            SELECT SUM(Sale) + SUM(Total) + SUM(WinningCost) + SUM(WinningPrice) + SUM(WinningCostWithAddedFee) + SUM(BrandSafetyCost) as Sumall FROM web_transactionDays2";

        public static string TableChecksumQuery =
            "SELECT CHECKSUM_AGG(CHECKSUM(*)) FROM web_transactionDays1 EXCEPT SELECT CHECKSUM_AGG(CHECKSUM(*)) FROM web_transactionDays2";

        public static string GetConnectionString()
        {
            return "Data Source=fedw.dwhinfra.d1.adform.zone,35352;Initial Catalog=DiscrepancyTest;Persist Security Info=True;User ID=discrepancy;Password=discrepancy; ";
        }
        private static SqlConnection _connect = new SqlConnection(GetConnectionString());

        public static SqlConnection Connect
        {
            get
            {
                return _connect;
            }

            set
            {
                _connect = value;
            }
        }

        private static SqlConnection _connect2 = new SqlConnection(GetConnectionString());

        public static SqlConnection Connect2
        {
            get
            {
                return _connect2;
            }

            set
            {
                _connect2 = value;
            }
        }

        public static void RowShowDifferences()
        {
            IEnumerable<Row> rows1 = new List<Row>();
            IEnumerable<Row> rows2 = new List<Row>();
            Parallel.Invoke(() =>
            {
                try
                {
                    Connect.Open();
                    rows1 = Connect.Query<Row>("SELECT * FROM web_transactionDays1");
                    Connect.Close();
                }
                catch (Exception exc)
                {
                    Connect.Close();
                    Console.WriteLine(exc);
                }
            }, () =>
             {
                 try
                 {
                     Connect2.Open();
                     rows2 = Connect2.Query<Row>("SELECT * FROM web_transactionDays2");
                     Connect2.Close();
                 }
                 catch (Exception exc)
                 {
                     Connect.Close();
                     Console.WriteLine(exc);
                 }
             });

            List<string> diff1 = rows1.Except(rows2).Select(a => a.ToString()).ToList();
            List<string> diff2 = rows2.Except(rows1).Select(a => a.ToString()).ToList();

            DiffPrint.Coloring(diff1, diff2);
        }

        public static List<string> GetStringList(string queryString, SqlConnection connection)
        {
            List<string> allRows = new List<string>();
            try
            {
                using (SqlCommand command = new SqlCommand(queryString, connection))
                {
                    command.CommandTimeout = 600;
                    SqlDataReader reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        List<string> row = new List<string>();
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            var bandimas = reader.GetValue(i);
                            row.Add(bandimas.ToString());
                        }
                        allRows.Add(String.Join("|", row));

                    }
                    reader.Close();
                }
            }
            catch (Exception)
            {
                connection.Close();
            }
            return allRows;
        }
        public static void Substring(string query1, string query2)
        {
            List<string> tableStrings1 = new List<string>();
            List<string> tableStrings2 = new List<string>();

            Parallel.Invoke(() =>
            {
                try
                {
                    Connect.Open();
                    tableStrings1 = GetStringList(query1, Connect);
                    Connect.Close();
                }
                catch (Exception exc)
                {
                    Connect.Close();
                    Console.WriteLine(exc);
                }
            }, () =>
            {

                try
                {
                    Connect2.Open();
                    tableStrings2 = GetStringList(query2, Connect2);
                    Connect2.Close();
                }
                catch (Exception exc)
                {
                    Connect2.Close();
                    Console.WriteLine(exc);
                }
            });

            IEnumerable<string> aOnlyTable1 = tableStrings1.Except(tableStrings2);
            IEnumerable<string> aOnlyTable2 = tableStrings2.Except(tableStrings1);

            DiffPrint.Coloring(aOnlyTable1.ToList(), aOnlyTable2.ToList());
        }

        public static bool AreChecksumsEqual()
        {
            try
            {
                Connect.Open();
                Console.WriteLine("Comparing checksums of tables...");
                var sarasas = Query.GetStringList(TableChecksumQuery, Connect).Count == 0;
                Connect.Close();
                return sarasas;
            }
            catch (Exception e)
            {
                Connect.Close();
                Console.WriteLine("Error when comparing table checksums: " + e);
                throw;
            }
        }

        public static bool AreMetricsEqual()
        {
            List<string> tableStrings1 = new List<string>();
            try
            {
                Connect.Open();
                Console.WriteLine("Comparing sums of all metrics...");
                tableStrings1 = Query.GetStringList(MetricsQuery, Connect);
                Connect.Close();
            }
            catch (Exception exc)
            {
                Connect.Close();
                Console.WriteLine(exc);
            }

            return tableStrings1.Count == 0;
        }
    }
}
