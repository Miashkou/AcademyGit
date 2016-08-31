using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using MoreLinq;

namespace Betkas
{
    public class Query
    {
        public static void ShowDifferences()
        {
            string conString = "Data Source=fedw.dwhinfra.d1.adform.zone,35352;Initial Catalog=DiscrepancyTest;Persist Security Info=True;User ID=discrepancy;Password=discrepancy";
            IEnumerable<Row> rows1 = new List<Row>();
            IEnumerable<Row> rows2 = new List<Row>();

            Parallel.Invoke(() =>
            {
                using (SqlConnection con = new SqlConnection(conString))
                    rows1 = con.Query<Row>("SELECT * FROM web_transactionDays1");
            },
            () =>
            {
                using (SqlConnection con = new SqlConnection(conString))
                    rows2 = con.Query<Row>("SELECT * FROM web_transactionDays2");
            });
            var diffs1 = new List<Row>(rows1.Except(rows2));
            var diffs2 = new List<Row>(rows2.Except(rows1));


            Console.WriteLine("\r\nIn table 1 but not in table 2:");
            foreach (var diff in diffs1)
            {
                Console.WriteLine(diff);
            }

            Console.WriteLine("\r\nIn table 2 but not in table 1:");
            foreach (var diff in diffs2)
            {
                Console.WriteLine(diff);
            }

            Console.WriteLine("\r\nBest corruption guesses:");
            foreach (var row in diffs1)
            {
                var guessRow = diffs2.MaxBy(a => a.CompareScore(row));
                var differencesIndexes = row.FindDifferencesIndexes(guessRow);
                var rowSplit = row.ToString().Split(' ');
                var guessSplit = guessRow.ToString().Split(' ');

                for (int i = 0; i < rowSplit.Length; i++)
                {
                    if (differencesIndexes.Contains(i))
                    {
                        Console.BackgroundColor = ConsoleColor.Blue;
                        Console.Write(rowSplit[i]);
                        Console.ResetColor();
                    }
                    else
                    {
                        Console.Write(rowSplit[i]);
                    }
                    Console.Write(" ");
                }

                Console.WriteLine();

                for (int i = 0; i < guessSplit.Length; i++)
                {
                    if (differencesIndexes.Contains(i))
                    {
                        Console.BackgroundColor = ConsoleColor.Blue;
                        Console.Write(guessSplit[i]);
                        Console.ResetColor();
                    }
                    else
                    {
                        Console.Write(guessSplit[i]);
                    }
                    Console.Write(" ");
                }

                Console.WriteLine("\r\n");
            }
        }

        public static void TableQuery(string queryString, SqlConnection connection)
        {
            try
            {
                using (SqlCommand command = new SqlCommand(
                queryString

                  , connection))
                {
                    command.CommandTimeout = 600;
                    SqlDataReader reader = command.ExecuteReader();

                    while (reader.Read())
                    {
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            Console.Write(reader.GetValue(i) + " | ");
                        }
                       
                    } Console.WriteLine("\n");
                    command.ExecuteNonQuery();
                }
            }
            catch (Exception exc)
            {
                Console.WriteLine("Could not select!");
            }
        }

        public static List<string> GetStringList(string queryString, SqlConnection connection)
        {
            List<string> allRows = new List<string>();
            try
            {
                using (SqlCommand command = new SqlCommand(
                queryString

                  , connection))
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
                        allRows.Add(string.Join(" ", row));
                 
                    }
                
                    
                    command.ExecuteNonQuery();

                }

            }
            catch (Exception exc)
            {
                connection.Close();
                
            }
            return allRows;
        }

    }
}
