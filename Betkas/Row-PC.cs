using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Runtime.Remoting.Messaging;

namespace Betkas
{
    public class Row
    {
        // Dapper.NET magic
        public DateTime logTime { get; set; } // D
        public Byte transactionType { get; set; } // D
        public Int32 Campaign{ get; set; } // D
        public Int32 ID_Logpoints{ get; set; } // D
        public Int32 ID_Memberships{ get; set; } // D
        public Int32 bannerId{ get; set; } // D
        public decimal Sale{ get; set; } // M
        public Int32 Total{ get; set; } // M
        public Int32 ID_AffiliateBanners{ get; set; } // D
        public Int32 ID_BannerClickURLs{ get; set; } // D
        public Boolean RecentAdInteraction{ get; set; } // D
        public Byte AdInteractionTransactionType{ get; set; } // D
        public Boolean AllSitesUniqueDay{ get; set; } // D
        public Boolean PointUniqueSession{ get; set; } // D
        public Boolean PointRepeat{ get; set; } // D
        public Boolean AllSitesUniqueCampaign{ get; set; } // D
        public Boolean SiteUniqueCampaign{ get; set; } // D
        public Boolean PointUniqueCampaign{ get; set; } // D
        public Boolean UniqueOrderID{ get; set; } // D
        public Boolean UniqueCampaign{ get; set; } // D
        public Boolean UniqueSession{ get; set; } // D
        public decimal WinningPrice{ get; set; } // M
        public Byte RtbPartyId{ get; set; } // D
        public decimal WinningCost{ get; set; } // M
        public decimal WinningCostWithAddedFee{ get; set; } // M
        public Int32 TestId{ get; set; } // D
        public Byte ulIsInScreen { get; set; } // D
        public decimal BrandSafetyCost{ get; set; } // M


        /// D: logTime, TransactionType, Campaign, ID_Logpoints, ID_Memberships, bannerId, ID_AffiliateBanners, ID_BannerClickURLs, AdInteractionTransactionType, AllSitesUniqueDay, PointUniqueSession,
		/// PointRepeat, AllSitesUniqueCampaign, SiteUniqueCampaign, PointUniqueCampaign, UniqueOrderID, UniqueCampaign, UniqueSession, RtbPartyId, TestId, ulIsInScreen
        /// 
        /// M: Sale, Total, WinningPrice, WinningCost, WinningCostWithAddedFee, BrandSafetyCost
        /// 

        public override string ToString()
        {
            return string.Join(" ", logTime, transactionType, Campaign, ID_Logpoints, ID_Memberships,
                bannerId, Sale, Total, ID_AffiliateBanners, ID_BannerClickURLs, RecentAdInteraction,
                AdInteractionTransactionType, AllSitesUniqueDay, PointUniqueSession, PointRepeat,
                AllSitesUniqueCampaign, SiteUniqueCampaign, PointUniqueCampaign, UniqueOrderID, UniqueCampaign,
                UniqueSession, WinningPrice, RtbPartyId, WinningCost, WinningCostWithAddedFee, 
                TestId, ulIsInScreen, BrandSafetyCost);
        }

        public override int GetHashCode()
        {
            return ToString().GetHashCode();
        }

        public override bool Equals(object obj)
        {
            return GetHashCode() == obj.GetHashCode();
        }

        public int CompareScore(Row other)
        {
            return 29 - FindDifferencesIndexes(other).Count;
            /*
            string[] thisList = ToString().Split(' ');
            string[] otherList = other.ToString().Split(' ');
            int score = 0;
            for (int i = 0; i < thisList.Length; i++)
            {
                if (thisList[i] == otherList[i])
                    score++;
            }
            return score;
            */
        }

        public List<int> FindDifferencesIndexes(Row other)
        {
            string[] thisList = ToString().Split(' ');
            string[] otherList = other.ToString().Split(' ');
            List<int> differences = new List<int>();
            for (int i = 0; i < thisList.Length; i++)
            {
                if (thisList[i] != otherList[i])
                    differences.Add(i);
            }
            return differences;
        }
    }

	/// Example usage:
		/*
            string conString = "Data Source=fedw.dwhinfra.d1.adform.zone,35352;Initial Catalog=DiscrepancyTest;Persist Security Info=True;User ID=discrepancy;Password=discrepancy";
            List<Row> rows1 = new List<Row>();
            List<Row> rows2 = new List<Row>();

            Parallel.Invoke(() =>
            {
                using (SqlConnection con = new SqlConnection(conString))
                    rows1 = con.Query<Row>("SELECT * FROM web_transactionDays1").ToList();
            },
            () =>
            {
                using (SqlConnection con = new SqlConnection(conString))
                    rows2 = con.Query<Row>("SELECT * FROM web_transactionDays2").ToList();
            });
            
            foreach (var diff in rows1.Except(rows2).Union(rows2.Except(rows1)))
            {
                Console.WriteLine(diff);
            }
            */
    
}