using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Dapper;

namespace Comperator
{
    public class Query
    {
        public static string SelectAllFrom1Query =
            "SELECT * FROM web_transactionDays1";

        public static string SelectAllFrom2Query =
            "select * from web_transactionDays2";

       // public static string SelectChecksumFrom1Query = " (SELECT \'Table1\' as TableID,  CHECKSUM(*) as CheckSum, * \r\nFROM web_transactionDays1\r\n  WHERE CHECKSUM(*) NOT IN\r\n    (\r\n    SELECT CHECKSUM(*)\r\n        FROM web_transactionDays2\r\n       ))\r\nORDER by CheckSum";
       // public static string SelectChecksumFrom2Query = "(SELECT \'Table2\' as TableID, CHECKSUM(*) as CheckSum, * \r\nFROM web_transactionDays2\r\n  WHERE CHECKSUM(*) NOT IN\r\n    (\r\n    SELECT CHECKSUM(*)\r\n        FROM web_transactionDays1\r\n       ))\r\nORDER by CheckSum";
        public static string SelectChecksumFrom1Query = "SELECT CHECKSUM(*) as CheckSum, * FROM web_transactionDays1";
        public static string SelectChecksumFrom2Query = "SELECT CHECKSUM(*) as CheckSum, * FROM web_transactionDays2";

       // public static string SelectBinaryChecksumFrom1Query = " (SELECT \'Table1\' as TableID,  BINARY_CHECKSUM(*) as BINARY_CHECKSUM, * \r\nFROM web_transactionDays1\r\n  WHERE BINARY_CHECKSUM(*) NOT IN\r\n    (\r\n    SELECT BINARY_CHECKSUM(*)\r\n        FROM web_transactionDays2\r\n       ))\r\nORDER by BINARY_CHECKSUM";
       // public static string SelectBinaryChecksumFrom2Query = "(SELECT \'Table2\' as TableID, BINARY_CHECKSUM(*) as BINARY_CHECKSUM, * \r\nFROM web_transactionDays2\r\n  WHERE BINARY_CHECKSUM(*) NOT IN\r\n    (\r\n    SELECT BINARY_CHECKSUM(*)\r\n        FROM web_transactionDays1\r\n       ))\r\nORDER by BINARY_CHECKSUM";
        public static string SelectBinaryChecksumFrom1Query = "SELECT BINARY_CHECKSUM(*) as BINARY_CHECKSUM, * FROM web_transactionDays1";
        public static string SelectBinaryChecksumFrom2Query = "SELECT BINARY_CHECKSUM(*) as BINARY_CHECKSUM, * FROM web_transactionDays2";

        //public static string SelectHashbytes1Query = "SELECT HASHBYTES(\'SHA1\', \r\nCAST (ISNULL(logtime,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(transactionType,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Campaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_Logpoints,\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(bannerId,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Sale,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Total,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_AffiliateBanners,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_BannerClickURLs,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(RecentAdInteraction,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AdInteractionTransactionType,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AllSitesUniqueDay,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointUniqueSession,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointRepeat,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AllSitesUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(SiteUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueOrderID,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueSession,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(CAST(WinningPrice as varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(RtbPartyId,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(CAST( WinningCost AS varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(CAST(WinningCostWithAddedFee AS varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(TestID,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ulIsInScreen,\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(CAST( BrandSafetyCost as varchar(18)),\'\') AS VARBINARY(MAX))\r\n  )\r\n as HashedValue, * \r\nFROM web_transactionDays1\r\n  WHERE HASHBYTES(\'SHA1\', \r\nCAST (ISNULL(logtime,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(transactionType,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Campaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_Logpoints,\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(bannerId,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Sale,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Total,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_AffiliateBanners,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_BannerClickURLs,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(RecentAdInteraction,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AdInteractionTransactionType,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AllSitesUniqueDay,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointUniqueSession,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointRepeat,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AllSitesUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(SiteUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueOrderID,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueSession,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(CAST(WinningPrice as varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(RtbPartyId,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(CAST( WinningCost AS varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(CAST(WinningCostWithAddedFee AS varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(TestID,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ulIsInScreen,\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(CAST( BrandSafetyCost as varchar(18)),\'\') AS VARBINARY(MAX))\r\n  ) NOT IN\r\n    (\r\n    SELECT HASHBYTES(\'SHA1\', \r\nCAST (ISNULL(logtime,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(transactionType,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Campaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_Logpoints,\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(bannerId,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Sale,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Total,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_AffiliateBanners,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_BannerClickURLs,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(RecentAdInteraction,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AdInteractionTransactionType,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AllSitesUniqueDay,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointUniqueSession,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointRepeat,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AllSitesUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(SiteUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueOrderID,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueSession,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(CAST(WinningPrice as varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(RtbPartyId,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(CAST( WinningCost AS varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(CAST(WinningCostWithAddedFee AS varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(TestID,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ulIsInScreen,\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(CAST( BrandSafetyCost as varchar(18)),\'\') AS VARBINARY(MAX))\r\n  )\r\n   FROM web_transactionDays2\r\n)";
        //public static string SelectHashbytes2Query = "SELECT HASHBYTES(\'SHA1\', \r\nCAST (ISNULL(logtime,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(transactionType,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Campaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_Logpoints,\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(bannerId,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Sale,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Total,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_AffiliateBanners,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_BannerClickURLs,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(RecentAdInteraction,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AdInteractionTransactionType,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AllSitesUniqueDay,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointUniqueSession,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointRepeat,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AllSitesUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(SiteUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueOrderID,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueSession,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(CAST(WinningPrice as varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(RtbPartyId,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(CAST( WinningCost AS varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(CAST(WinningCostWithAddedFee AS varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(TestID,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ulIsInScreen,\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(CAST( BrandSafetyCost as varchar(18)),\'\') AS VARBINARY(MAX))\r\n  )\r\n as HashedValue, * \r\nFROM web_transactionDays2\r\n  WHERE HASHBYTES(\'SHA1\', \r\nCAST (ISNULL(logtime,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(transactionType,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Campaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_Logpoints,\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(bannerId,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Sale,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Total,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_AffiliateBanners,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_BannerClickURLs,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(RecentAdInteraction,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AdInteractionTransactionType,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AllSitesUniqueDay,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointUniqueSession,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointRepeat,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AllSitesUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(SiteUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueOrderID,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueSession,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(CAST(WinningPrice as varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(RtbPartyId,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(CAST( WinningCost AS varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(CAST(WinningCostWithAddedFee AS varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(TestID,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ulIsInScreen,\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(CAST( BrandSafetyCost as varchar(18)),\'\') AS VARBINARY(MAX))\r\n  ) NOT IN\r\n    (\r\n    SELECT HASHBYTES(\'SHA1\', \r\nCAST (ISNULL(logtime,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(transactionType,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Campaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_Logpoints,\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(bannerId,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Sale,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Total,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_AffiliateBanners,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_BannerClickURLs,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(RecentAdInteraction,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AdInteractionTransactionType,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AllSitesUniqueDay,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointUniqueSession,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointRepeat,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AllSitesUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(SiteUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueOrderID,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueSession,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(CAST(WinningPrice as varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(RtbPartyId,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(CAST( WinningCost AS varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(CAST(WinningCostWithAddedFee AS varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(TestID,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ulIsInScreen,\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(CAST( BrandSafetyCost as varchar(18)),\'\') AS VARBINARY(MAX))\r\n  )\r\n   FROM web_transactionDays1\r\n)";
        public static string SelectHashbytes1Query = "SELECT HASHBYTES(\'SHA1\', \r\nCAST (ISNULL(logtime,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(transactionType,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Campaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_Logpoints,\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(bannerId,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Sale,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Total,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_AffiliateBanners,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_BannerClickURLs,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(RecentAdInteraction,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AdInteractionTransactionType,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AllSitesUniqueDay,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointUniqueSession,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointRepeat,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AllSitesUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(SiteUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueOrderID,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueSession,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(CAST(WinningPrice as varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(RtbPartyId,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(CAST( WinningCost AS varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(CAST(WinningCostWithAddedFee AS varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(TestID,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ulIsInScreen,\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(CAST( BrandSafetyCost as varchar(18)),\'\') AS VARBINARY(MAX))\r\n  )\r\n as HashedValue, *  FROM web_transactionDays1";
        public static string SelectHashbytes2Query = "SELECT HASHBYTES(\'SHA1\', \r\nCAST (ISNULL(logtime,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(transactionType,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Campaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_Logpoints,\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(bannerId,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Sale,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(Total,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_AffiliateBanners,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ID_BannerClickURLs,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(RecentAdInteraction,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AdInteractionTransactionType,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AllSitesUniqueDay,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointUniqueSession,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointRepeat,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(AllSitesUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(SiteUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(PointUniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueOrderID,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueCampaign,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(UniqueSession,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(CAST(WinningPrice as varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(RtbPartyId,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(CAST( WinningCost AS varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(CAST(WinningCostWithAddedFee AS varchar(18)),\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(TestID,\'\') AS VARBINARY(MAX))+ \r\nCAST (ISNULL(ulIsInScreen,\'\') AS VARBINARY(MAX))+\r\nCAST (ISNULL(CAST( BrandSafetyCost as varchar(18)),\'\') AS VARBINARY(MAX))\r\n  )\r\n as HashedValue, *  FROM web_transactionDays2";
        public static string metricsQuery =
            @"SELECT SUM(Sale) + SUM(Total) + SUM(WinningCost) + SUM(WinningPrice) + SUM(WinningCostWithAddedFee) + SUM(BrandSafetyCost) as Sumall FROM web_transactionDays1
            EXCEPT
            SELECT SUM(Sale) + SUM(Total) + SUM(WinningCost) + SUM(WinningPrice) + SUM(WinningCostWithAddedFee) + SUM(BrandSafetyCost) as Sumall FROM web_transactionDays2";

        public static string tableChecksumQuery =
            "SELECT CHECKSUM_AGG(CHECKSUM(*)) FROM web_transactionDays1 EXCEPT SELECT CHECKSUM_AGG(CHECKSUM(*)) FROM web_transactionDays2";

        public static string GetConnectionString()
        {
            return "Data Source=fedw.dwhinfra.d1.adform.zone,35352;Initial Catalog=DiscrepancyTest;Persist Security Info=True;User ID=discrepancy;Password=discrepancy; ";
        }

        public static void ShowDifferences()
        {
            string conString = GetConnectionString();
            IEnumerable<Row> rows1 = new List<Row>();
            IEnumerable<Row> rows2 = new List<Row>();
            Parallel.Invoke(() =>
            {
                
                using (SqlConnection con = new SqlConnection(conString))
                {

                    rows1 = con.Query<Row>("SELECT * FROM web_transactionDays1");
                }

            }, () =>
             {
                 using (SqlConnection con = new SqlConnection(conString))
                 {
                     rows2 = con.Query<Row>("SELECT * FROM web_transactionDays2");
                 }
                     
             });

            List<string> diff1 = rows1.Except(rows2).Select(a=>a.ToString()).ToList();
            List<string> diff2 = rows2.Except(rows1).Select(a=>a.ToString()).ToList();

            Coloring(diff1, diff2);
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
                        allRows.Add(String.Join(" ", row));

                    }
                    command.ExecuteNonQuery();
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
            string connectionString = GetConnectionString();

            Parallel.Invoke(() =>
            {

                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    try
                    {
                        connection.Open();
                        tableStrings1 = GetStringList(query1, connection);
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
                        connection.Open();
                        tableStrings2 = GetStringList(query2, connection);
                    }
                    catch (Exception exc)
                    {
                        Console.WriteLine(exc);
                    }

                }
            });

            IEnumerable<string> aOnlyTable1 = tableStrings1.Except(tableStrings2);
            IEnumerable<string> aOnlyTable2 = tableStrings2.Except(tableStrings1);

            Coloring(aOnlyTable1.ToList(), aOnlyTable2.ToList());
        }

        public static string GetSimiliarString(string original, IEnumerable<string> stringList)
        {
            var bestScore = 0;
            var similiarString = String.Empty;
            var originalSplit = original.Split(' ');

            foreach (var otherString in stringList)
            {
                var score = 0;
                var otherSplit = otherString.Split(' ');
                for (int i = 0; i < originalSplit.Length; i++)
                {
                    if (originalSplit[i] == otherSplit[i])
                    {
                        score++;
                    }
                }

                if (score > bestScore)
                {
                    similiarString = otherString;
                    bestScore = score;
                }
            }

            return similiarString;
        }
        public static List<int> FindDifferenceIndexes(string similarString, string original)
        {
            string[] thisList = original.Split(' ');
            string[] otherList = similarString.Split(' ');
            List<int> differences = new List<int>();
            for (int i = 0; i < thisList.Length; i++)
            {
                if (thisList[i] != otherList[i])
                    differences.Add(i);
            }
            return differences;
        }

        public static void Coloring(List<string> list1, List<string> list2)
        {
            foreach (var diff in list1)
            {
                var bestGuess = GetSimiliarString(diff, list2);

                var differences = FindDifferenceIndexes(diff, bestGuess);

                var diffSplit = diff.Split(' ');
                var bestGuessSplit = bestGuess.Split(' ');

                PrintWithDifferences(diffSplit, differences);
                PrintWithDifferences(bestGuessSplit, differences);
                Console.WriteLine();
            }
        }

        public static void PrintWithDifferences(string[] strings, List<int> diffIndexes, ConsoleColor color = ConsoleColor.DarkCyan)
        {
            for (int i = 0; i < strings.Length; i++)
            {
                if (diffIndexes.Contains(i))
                {
                    Console.BackgroundColor = color;
                    Console.Write(strings[i]);
                    Console.ResetColor();
                }
                else
                {
                    Console.Write(strings[i]);
                }
                Console.Write(" ");
            }
            Console.WriteLine();
        }

        public static bool AreChecksumsEqual()
        {
            using (SqlConnection con = new SqlConnection(GetConnectionString()))
            {
                try
                {
                    con.Open();
                    Console.WriteLine("Comparing checksums of tables...");
                    return Query.GetStringList(tableChecksumQuery, con).Count == 0;
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
                    tableStrings1 = Query.GetStringList(metricsQuery, connection);
                }
                catch (Exception exc)
                {
                    Console.WriteLine(exc);
                }

            }
            return tableStrings1.Count == 0;
        }
    }
}
