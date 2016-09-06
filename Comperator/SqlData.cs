using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Comperator
{
    static class SqlData
    {
        // Known column data
        public static List<string> Dimensions = new List<string>
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

        public static List<string> Metrics = new List<string>
        {
            "Sale", "Total", "WinningPrice", "WinningCost", "WinningCostWithAddedFee", "BrandSafetyCost"

        };

        // Query strings
        public static string SelectAllFrom1Query =
            "SELECT * FROM web_transactionDays1";

        public static string SelectAllFrom2Query =
            "SELECT * FROM web_transactionDays2";

        public static string SelectChecksumFrom1Query =
            @"SELECT CHECKSUM(*) as CheckSum, * FROM web_transactionDays1";

        public static string SelectChecksumFrom2Query =
            @"SELECT CHECKSUM(*) as CheckSum, * FROM web_transactionDays2";

        public static string SelectBinaryChecksumFrom1Query =
            @"SELECT BINARY_CHECKSUM(*) as BINARY_CHECKSUM, * FROM web_transactionDays1";

        public static string SelectBinaryChecksumFrom2Query =
            @"SELECT BINARY_CHECKSUM(*) as BINARY_CHECKSUM, * FROM web_transactionDays2";

        public static string SelectHashbytes1Query =
            @"SELECT HASHBYTES('SHA1', 
            CAST (ISNULL(logtime,'') AS VARBINARY(MAX)) + 
            CAST (ISNULL(transactionType,'') AS VARBINARY(MAX)) + 
            CAST (ISNULL(Campaign,'') AS VARBINARY(MAX)) + 
            CAST (ISNULL(ID_Logpoints,'') AS VARBINARY(MAX)) +
            CAST (ISNULL(bannerId,'') AS VARBINARY(MAX)) + 
            CAST (ISNULL(Sale,'') AS VARBINARY(MAX)) + 
            CAST (ISNULL(Total,'') AS VARBINARY(MAX)) + 
            CAST (ISNULL(ID_AffiliateBanners,'') AS VARBINARY(MAX)) + 
            CAST (ISNULL(ID_BannerClickURLs,'') AS VARBINARY(MAX)) + 
            CAST (ISNULL(RecentAdInteraction,'') AS VARBINARY(MAX)) + 
            CAST (ISNULL(AdInteractionTransactionType,'') AS VARBINARY(MAX)) + 
            CAST (ISNULL(AllSitesUniqueDay,'') AS VARBINARY(MAX)) + 
            CAST (ISNULL(PointUniqueSession,'') AS VARBINARY(MAX)) + 
            CAST (ISNULL(PointRepeat,'') AS VARBINARY(MAX)) + 
            CAST (ISNULL(AllSitesUniqueCampaign,'') AS VARBINARY(MAX)) + 
            CAST (ISNULL(SiteUniqueCampaign,'') AS VARBINARY(MAX)) + 
            CAST (ISNULL(PointUniqueCampaign,'') AS VARBINARY(MAX)) + 
            CAST (ISNULL(UniqueOrderID,'') AS VARBINARY(MAX)) + 
            CAST (ISNULL(UniqueCampaign,'') AS VARBINARY(MAX)) + 
            CAST (ISNULL(UniqueSession,'') AS VARBINARY(MAX)) + 
            CAST (ISNULL(CAST(WinningPrice as varchar(18)),'') AS VARBINARY(MAX)) +
            CAST (ISNULL(RtbPartyId,'') AS VARBINARY(MAX)) + 
            CAST (ISNULL(CAST( WinningCost AS varchar(18)),'') AS VARBINARY(MAX)) +
            CAST (ISNULL(CAST(WinningCostWithAddedFee AS varchar(18)),'') AS VARBINARY(MAX)) +
            CAST (ISNULL(TestID,'') AS VARBINARY(MAX)) + 
            CAST (ISNULL(ulIsInScreen,'') AS VARBINARY(MAX)) +
            CAST (ISNULL(CAST(BrandSafetyCost as varchar(18)),'') AS VARBINARY(MAX)))
            AS HashedValue, * FROM web_transactionDays1";

        public static string SelectHashbytes2Query = @"SELECT HASHBYTES('SHA1', 
            CAST (ISNULL(logtime,'') AS VARBINARY(MAX))+ 
            CAST (ISNULL(transactionType,'') AS VARBINARY(MAX))+ 
            CAST (ISNULL(Campaign,'') AS VARBINARY(MAX))+ 
            CAST (ISNULL(ID_Logpoints,'') AS VARBINARY(MAX))+
            CAST (ISNULL(bannerId,'') AS VARBINARY(MAX))+ 
            CAST (ISNULL(Sale,'') AS VARBINARY(MAX))+ 
            CAST (ISNULL(Total,'') AS VARBINARY(MAX))+ 
            CAST (ISNULL(ID_AffiliateBanners,'') AS VARBINARY(MAX))+ 
            CAST (ISNULL(ID_BannerClickURLs,'') AS VARBINARY(MAX))+ 
            CAST (ISNULL(RecentAdInteraction,'') AS VARBINARY(MAX))+ 
            CAST (ISNULL(AdInteractionTransactionType,'') AS VARBINARY(MAX))+ 
            CAST (ISNULL(AllSitesUniqueDay,'') AS VARBINARY(MAX))+ 
            CAST (ISNULL(PointUniqueSession,'') AS VARBINARY(MAX))+ 
            CAST (ISNULL(PointRepeat,'') AS VARBINARY(MAX))+ 
            CAST (ISNULL(AllSitesUniqueCampaign,'') AS VARBINARY(MAX))+ 
            CAST (ISNULL(SiteUniqueCampaign,'') AS VARBINARY(MAX))+ 
            CAST (ISNULL(PointUniqueCampaign,'') AS VARBINARY(MAX))+ 
            CAST (ISNULL(UniqueOrderID,'') AS VARBINARY(MAX))+ 
            CAST (ISNULL(UniqueCampaign,'') AS VARBINARY(MAX))+ 
            CAST (ISNULL(UniqueSession,'') AS VARBINARY(MAX))+ 
            CAST (ISNULL(CAST(WinningPrice as varchar(18)),'') AS VARBINARY(MAX))+
            CAST (ISNULL(RtbPartyId,'') AS VARBINARY(MAX))+ 
            CAST (ISNULL(CAST( WinningCost AS varchar(18)),'') AS VARBINARY(MAX))+
            CAST (ISNULL(CAST(WinningCostWithAddedFee AS varchar(18)),'') AS VARBINARY(MAX))+
            CAST (ISNULL(TestID,'') AS VARBINARY(MAX))+ 
            CAST (ISNULL(ulIsInScreen,'') AS VARBINARY(MAX))+
            CAST (ISNULL(CAST( BrandSafetyCost as varchar(18)),'') AS VARBINARY(MAX)))
            AS HashedValue, * FROM web_transactionDays2";

        public static string MetricsQuery =
            @"SELECT SUM(Sale) + SUM(Total) + SUM(WinningCost) + SUM(WinningPrice)
            + SUM(WinningCostWithAddedFee) + SUM(BrandSafetyCost) as Sumall FROM web_transactionDays1
                EXCEPT
            SELECT SUM(Sale) + SUM(Total) + SUM(WinningCost) + SUM(WinningPrice) 
            + SUM(WinningCostWithAddedFee) + SUM(BrandSafetyCost) as Sumall FROM web_transactionDays2";

        public static string TableChecksumQuery =
            "SELECT CHECKSUM_AGG(CHECKSUM(*)) FROM web_transactionDays1 EXCEPT SELECT CHECKSUM_AGG(CHECKSUM(*)) FROM web_transactionDays2";

        public static string GetConnectionString()
        {
            return "Data Source=fedw.dwhinfra.d1.adform.zone,35352;Initial Catalog=DiscrepancyTest;Persist Security Info=True;User ID=discrepancy;Password=discrepancy; ";
        }
    }
}
