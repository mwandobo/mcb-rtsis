#!/usr/bin/env python3
"""
Test mock overdraft query
"""

import logging
from db2_connection import DB2Connection

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_mock_overdraft():
    """Test mock overdraft query"""
    logger.info("🧪 Testing Mock Overdraft Query")
    
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            # Simple mock query
            mock_query = """
            SELECT 
                VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHH24MI') AS reportingDate,
                'OD001' AS accountNumber,
                'CID001' AS customerIdentificationNumber,
                'Sample Overdraft Client' AS clientName,
                'Individual' AS clientType,
                'Tanzania' as borrowerCountry,
                CAST(NULL AS VARCHAR(50)) as ratingStatus,
                CAST(NULL AS VARCHAR(50)) as crRatingBorrower,
                CAST(NULL AS VARCHAR(50)) as gradesUnratedBanks,
                CAST(NULL AS VARCHAR(50)) as groupCode,
                CAST(NULL AS VARCHAR(200)) as relatedEntityName,
                CAST(NULL AS VARCHAR(50)) as relatedParty,
                CAST(NULL AS VARCHAR(50)) as relationshipCategory,
                'Business Overdraft' as loanProductType,
                'OtherServices' as overdraftEconomicActivity,
                'Existing' as loanPhase,
                'NotSpecified' as transferStatus,
                'Business Overdraft' AS purposeOtherLoans,
                CURRENT_DATE - 30 DAYS as contractDate,
                '001' as branchCode,
                'Sample Officer' as loanOfficer,
                CAST(NULL AS VARCHAR(200)) as loanSupervisor,
                'TZS' as currency,
                1000000.00 as orgSanctionedAmount,
                CAST(NULL AS DECIMAL(15,2)) AS usdSanctionedAmount,
                1000000.00 AS tzsSanctionedAmount,
                500000.00 as orgUtilisedAmount,
                CAST(NULL AS DECIMAL(15,2)) AS usdUtilisedAmount,
                500000.00 AS tzsUtilisedAmount,
                50000.00 as orgCrUsageLast30DaysAmount,
                CAST(NULL AS DECIMAL(15,2)) AS usdCrUsageLast30DaysAmount,
                50000.00 AS tzsCrUsageLast30DaysAmount,
                CURRENT_DATE - 30 DAYS AS disbursementDate,
                CURRENT_DATE + 365 DAYS AS expiryDate,
                CURRENT_DATE + 365 DAYS AS realEndDate,
                450000.00 AS orgOutstandingAmount,
                CAST(NULL AS DECIMAL(15,2)) AS usdOutstandingAmount,
                450000.00 AS tzsOutstandingAmount,
                CURRENT_DATE - 30 DAYS AS latestCustomerCreditDate,
                100000.00 AS latestCreditAmount,
                15.50 AS primeLendingRate,
                18.00 AS annualInterestRate,
                800000.00 AS collateralPledged,
                800000.00 AS orgCollateralValue,
                CAST(NULL AS DECIMAL(15,2)) AS usdCollateralValue,
                800000.00 AS tzsCollateralValue,
                0 AS restructuredLoans,
                0 AS pastDueDays,
                0.00 AS pastDueAmount,
                5000.00 AS orgAccruedInterestAmount,
                CAST(NULL AS DECIMAL(15,2)) AS usdAccruedInterestAmount,
                5000.00 AS tzsAccruedInterestAmount,
                0.00 AS orgPenaltyChargedAmount,
                CAST(NULL AS DECIMAL(15,2)) AS usdPenaltyChargedAmount,
                0.00 AS tzsPenaltyChargedAmount,
                2500.00 AS orgLoanFeesChargedAmount,
                CAST(NULL AS DECIMAL(15,2)) AS usdLoanFeesChargedAmount,
                2500.00 AS tzsLoanFeesChargedAmount,
                2500.00 AS orgLoanFeesPaidAmount,
                CAST(NULL AS DECIMAL(15,2)) AS usdLoanFeesPaidAmount,
                2500.00 AS tzsLoanFeesPaidAmount,
                25000.00 AS orgTotMonthlyPaymentAmount,
                CAST(NULL AS DECIMAL(15,2)) AS usdTotMonthlyPaymentAmount,
                25000.00 AS tzsTotMonthlyPaymentAmount,
                15000.00 AS orgInterestPaidTotal,
                CAST(NULL AS DECIMAL(15,2)) AS usdInterestPaidTotal,
                15000.00 AS tzsInterestPaidTotal,
                'Current' AS assetClassificationCategory,
                'Other Financial Intermediary' AS sectorSnaClassification,
                'Active' AS negStatusContract,
                'Individual' AS customerRole,
                0.00 AS allowanceProbableLoss,
                0.00 AS botProvision
            FROM SYSIBM.SYSDUMMY1
            """
            
            logger.info("📊 Executing mock overdraft query...")
            cursor.execute(mock_query)
            rows = cursor.fetchall()
            
            logger.info(f"✅ Fetched {len(rows)} mock overdraft records")
            
            if rows:
                row = rows[0]
                logger.info("📋 Mock overdraft data:")
                logger.info(f"  Account: {row[1]}, Client: {row[3]}, Amount: {row[23]:,.2f} {row[22]}")
                logger.info(f"  Total columns: {len(row)}")
            
            return rows
            
    except Exception as e:
        logger.error(f"❌ Mock overdraft test failed: {e}")
        import traceback
        traceback.print_exc()
        return []

if __name__ == "__main__":
    test_mock_overdraft()