using System;
using System.Collections.Generic;

namespace Comperator
{
    public class Row
    {
        // Dapper.NET magic
        public DateTime logTime { get; set; } // D
        public byte transactionType { get; set; } // D
        public int Campaign{ get; set; } // D
        public int ID_Logpoints{ get; set; } // D
        public int ID_Memberships{ get; set; } // D
        public int bannerId{ get; set; } // D
        public decimal Sale{ get; set; } // M
        public int Total{ get; set; } // M
        public int ID_AffiliateBanners{ get; set; } // D
        public int ID_BannerClickURLs{ get; set; } // D
        public bool RecentAdInteraction{ get; set; } // D
        public byte AdInteractionTransactionType{ get; set; } // D
        public bool AllSitesUniqueDay{ get; set; } // D
        public bool PointUniqueSession{ get; set; } // D
        public bool PointRepeat{ get; set; } // D
        public bool AllSitesUniqueCampaign{ get; set; } // D
        public bool SiteUniqueCampaign{ get; set; } // D
        public bool PointUniqueCampaign{ get; set; } // D
        public bool UniqueOrderID{ get; set; } // D
        public bool UniqueCampaign{ get; set; } // D
        public bool UniqueSession{ get; set; } // D
        public decimal WinningPrice{ get; set; } // M
        public byte RtbPartyId{ get; set; } // D
        public decimal WinningCost{ get; set; } // M
        public decimal WinningCostWithAddedFee{ get; set; } // M
        public int TestId{ get; set; } // D
        public byte ulIsInScreen { get; set; } // D
        public decimal BrandSafetyCost{ get; set; } // M


        public static char columnValueSeperatorChar = '|';
        public static string columnValueSeperatorStr = "|";

        /// D: logTime, TransactionType, Campaign, ID_Logpoints, ID_Memberships, bannerId, ID_AffiliateBanners, ID_BannerClickURLs, AdInteractionTransactionType, AllSitesUniqueDay, PointUniqueSession,
		/// PointRepeat, AllSitesUniqueCampaign, SiteUniqueCampaign, PointUniqueCampaign, UniqueOrderID, UniqueCampaign, UniqueSession, RtbPartyId, TestId, ulIsInScreen
        /// 
        /// M: Sale, Total, WinningPrice, WinningCost, WinningCostWithAddedFee, BrandSafetyCost

        public override string ToString()
        {
            return string.Join(columnValueSeperatorStr, logTime, transactionType, Campaign, ID_Logpoints, ID_Memberships,
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
            return ToString() == obj.ToString();
        }

        /// <summary>
        /// Compares two objects and returns a score - how similiar they are. The score cannot be bigger than the count of columns.
        /// </summary>
        /// <param name="other"></param>
        public int CompareScore(Row other)
        {
            return ToString().Split(columnValueSeperatorChar).Length - FindDifferenceIndexes(other).Count;
        }

        /// <summary>
        /// Compares two Row objects column to column, returns indexes of differences.
        /// </summary>
        /// <param name="other"></param>
        /// <returns>List of int - indexes of columns, where a discrepancy was found</returns>
        public List<int> FindDifferenceIndexes(Row other)
        {
            string[] thisList = ToString().Split(columnValueSeperatorChar);
            string[] otherList = other.ToString().Split(columnValueSeperatorChar);
            List<int> differences = new List<int>();
            for (int i = 0; i < thisList.Length; i++)
            {
                if (thisList[i] != otherList[i])
                    differences.Add(i);
            }
            return differences;
        }
    }
}