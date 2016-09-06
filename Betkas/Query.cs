using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Dapper;

namespace Comperator
{
    public class Query
    {
        // Query strings
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

        public static List<string> ListOfAtributesName = new List<string>();

        public static void SetListOfTableAtributeNames(SqlDataReader reader)
        {
            ListOfAtributesName = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToList();
            //if (ListOfAtributesName.Count == 0)
            //    ListOfAtributesName = GetColumnNames();
            //return ListOfAtributesName;
            //ListOfAtributesName = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToList();
        }

        // Known column data
        private static List<string> Dimensions = new List<string>
        {
            "logTime",
            "TransactionType",
            "Campaign",
            "ID_Logpoints",
            "ID_Memberships",
            "bannerId",
            "ID_AffiliateBanners",
            "ID_BannerClickURLs",
            "AdInteractionTransactionType",
            "AllSitesUniqueDay",
            "PointUniqueSession",
            "PointRepeat",
            "AllSitesUniqueCampaign",
            "SiteUniqueCampaign",
            "PointUniqueCampaign",
            "UniqueOrderID",
            "UniqueCampaign",
            "UniqueSession",
            "RtbPartyId",
            "TestId",
            "ulIsInScreen"
        };
        private static List<string> Metrics = new List<string>
        {
            "Sale", "Total", "WinningPrice", "WinningCost", "WinningCostWithAddedFee", "BrandSafetyCost"
        
        };

            // Two connections for threaded data reading
        private static SqlConnection _connect = new SqlConnection(GetConnectionString());
        public static SqlConnection Connect
        {
            get
            {
                if (_connect.State.ToString() != "Open")
                    _connect.Open();
                    
                return _connect;
            }
        }

        private static SqlConnection _connect2 = new SqlConnection(GetConnectionString());
        public static SqlConnection Connect2
        {
            get
            {
                if (_connect2.State.ToString() != "Open")
                    _connect2.Open();
                return _connect2;
            }
        }

        public static void CloseConnections()
        {
            if (Connect.State.ToString() == "Open")
                Connect.Close();
            if (Connect2.State.ToString() == "Open")
                Connect2.Close();
        }

        /// <summary>
        /// Show differences from the default tables using Row class. Uses static queries.
        /// </summary>
        public static void RowDiscrepancyPrint()
        {
            ListOfAtributesName = GetColumnNames();
            IEnumerable<Row> rows1 = new List<Row>();
            IEnumerable<Row> rows2 = new List<Row>();
            Parallel.Invoke(() =>
            {
                try
                {
                    rows1 = Connect.Query<Row>("SELECT * FROM web_transactionDays1");
                }
                catch (Exception exc)
                {
                    Connect.Close();
                    Console.WriteLine("Exception during parallel query: " + exc);
                }
            }, () =>
             {
                 try
                 {
                     rows2 = Connect2.Query<Row>("SELECT * FROM web_transactionDays2");
                 }
                 catch (Exception exc)
                 {
                     Connect2.Close();
                     Console.WriteLine("Exception during parallel query: " + exc);
                 }
             });

            List<string> diff1 = rows1.Except(rows2).Select(a => a.ToString()).ToList();
            List<string> diff2 = rows2.Except(rows1).Select(a => a.ToString()).ToList();

            DiffPrint.Coloring(diff1, diff2);
        }

        /// <summary>
        /// Get the query output as a list of strings, seperated by Row.columnValueSeperatorStr. Opens and closes the connection automatically
        /// </summary>
        /// <param name="queryString">Query to perform on connection</param>
        /// <param name="connection">Connection to use for query</param>
        /// <param name="closeConnection">Whether to close connection after query. Default: true</param>
        /// <returns>Output of query as a list of strings</returns>
        public static List<string> GetStringsByQuery(string queryString, SqlConnection connection, bool closeConnection = false)
        {
            List<string> allRows = new List<string>();
            try
            {
                using (SqlCommand command = new SqlCommand(queryString, connection))
                {
                    command.CommandTimeout = 600;
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            List<string> row = new List<string>();
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                var value = reader.GetValue(i);
                                row.Add(value.ToString());
                            }
                            allRows.Add(string.Join(Row.columnValueSeperatorStr, row));
                        }
                        SetListOfTableAtributeNames(reader);
                        reader.Close();
                    }  
                }

                if(closeConnection)
                    connection.Close();
            }
            catch (Exception e)
            {
                Console.WriteLine("Caught exception when performing query. Closing connection. Error: " + e);
                connection.Close();
                throw;
            }
            return allRows;
        }

        /// <summary>
        /// Print table differences using two queries. Uses lists of strings.
        /// </summary>
        /// <param name="query1">First table to compare</param>
        /// <param name="query2">Second table to compare</param>
        public static void StringDiscrepancyPrint(string query1, string query2)
        {
            List<string> tableStrings1 = new List<string>();
            List<string> tableStrings2 = new List<string>();

            Parallel.Invoke(() =>
            {
                try
                {
                    tableStrings1 = GetStringsByQuery(query1, Connect);
                }
                catch (Exception exc)
                {
                    Connect.Close();
                    Console.WriteLine("Exception during parallel query: " + exc);
                }
            }, () =>
            {

                try
                {
                    tableStrings2 = GetStringsByQuery(query2, Connect2);
                }
                catch (Exception exc)
                {
                    Connect2.Close();
                    Console.WriteLine("Exception during parallel query: " + exc);
                }
            });

            IEnumerable<string> aOnlyTable1 = tableStrings1.Except(tableStrings2);
            IEnumerable<string> aOnlyTable2 = tableStrings2.Except(tableStrings1);

            DiffPrint.Coloring(aOnlyTable1.ToList(), aOnlyTable2.ToList());
        }

        /// <summary>
        /// Check if checksums of two tables are equal. Uses static queries.
        /// </summary>
        /// <returns>True if equal, else false</returns>
        public static bool AreChecksumsEqual()
        {
            try
            {
                Console.WriteLine("Comparing checksums of tables...");
                var sarasas = GetStringsByQuery(TableChecksumQuery, Connect).Count == 0;
                return sarasas;
            }
            catch (Exception e)
            {
                Console.WriteLine("Error when comparing table checksums: " + e);
                throw;
            }
        }

        /// <summary>
        /// Checks if all metrics are equal in two tables. Uses static queries.
        /// </summary>
        /// <returns>True if equal, else false</returns>
        public static bool AreMetricsEqual()
        {
            List<string> tableStrings1 = new List<string>();
            try
            {
                Console.WriteLine("Comparing sums of all metrics...");
                tableStrings1 = GetStringsByQuery(MetricsQuery, Connect);
            }
            catch (Exception exc)
            {
                Console.WriteLine("Error when comparing all metrics: " + exc);
            }

            return tableStrings1.Count == 0;
        }

        /// <summary>
        /// Returns names of metrics that were different.
        /// </summary>
        public static List<string> WhichMetricsDifferent()
        {
            List<string> differences = new List<string>();
            foreach (var metric in GetMetricsList())
            {
                try
                {
                    if (GetStringsByQuery("SELECT CHECKSUM_AGG(CHECKSUM(" + metric 
                        + ")) FROM web_transactionDays1 EXCEPT SELECT CHECKSUM_AGG(CHECKSUM(" 
                        + metric + ")) FROM web_transactionDays2", Connect).Count != 0)
                    {
                        // Debug
                        Console.WriteLine($"`{metric}` is different! <----");
                        differences.Add(metric);
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("Exception when comparing metric " + metric + ": " + e);
                }
                
            }
            return differences;
        }

        public static List<string> GetMetricsList()
        {
            List<string> metrics = new List<string>();
            foreach (var name in GetColumnNames())
            {
                if (Metrics.Contains(name))
                    metrics.Add(name);
                    
            }
            return metrics;
        }

        /// <summary>
        /// Uses default connection string and first table to get column data. Hardcoded.
        /// </summary>
        /// <returns></returns>
        public static List<string> GetColumnNames()
        {
            using (SqlConnection conn = new SqlConnection(GetConnectionString()))
            {
                string[] restrictions = new string[4] { null, null, "web_transactionDays1", null };
                conn.Open();
                return conn.GetSchema("Columns", restrictions).AsEnumerable().Select(s => s.Field<string>("Column_Name")).ToList();
            }
        }
    }

    // Parameterized queries
    //    string sql = "SELECT empSalary from employee where salary = @salary";
    //using (SqlConnection connection = new SqlConnection(/* connection info */))
    //{
    //    using (SqlCommand command = new SqlCommand(sql, connection))
    //    {
    //        var salaryParam = new SqlParameter("salary", SqlDbType.Money);
    //    salaryParam.Value = txtMoney.Text;

    //        command.Parameters.Add(salaryParam);

    //        var results = command.ExecuteReader();
    //}
    //}

}
