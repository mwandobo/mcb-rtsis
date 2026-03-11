#!/usr/bin/env python3
"""
Verify Investment Debt Securities Data Quality
Checks for duplicates, data integrity, and provides summary statistics
"""

import sys
import os
import logging
import psycopg2
from collections import defaultdict

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def verify_data_quality():
    """Verify the data quality in the investmentDebtSecurities table"""
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    config = Config()
    
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        
        cursor = conn.cursor()
        
        logger.info("=" * 60)
        logger.info("INVESTMENT DEBT SECURITIES DATA QUALITY REPORT")
        logger.info("=" * 60)
        
        # 1. Basic record count
        cursor.execute('SELECT COUNT(*) FROM "investmentDebtSecurities"')
        total_records = cursor.fetchone()[0]
        logger.info(f"Total records in table: {total_records:,}")
        
        # 2. Check for duplicates by security number
        cursor.execute("""
            SELECT "securityNumber", COUNT(*) as count
            FROM "investmentDebtSecurities"
            GROUP BY "securityNumber"
            HAVING COUNT(*) > 1
            ORDER BY count DESC
        """)
        duplicates = cursor.fetchall()
        
        if duplicates:
            logger.warning(f"Found {len(duplicates)} duplicate security numbers:")
            for security, count in duplicates[:10]:  # Show top 10
                logger.warning(f"  Security {security}: {count} records")
            if len(duplicates) > 10:
                logger.warning(f"  ... and {len(duplicates) - 10} more")
        else:
            logger.info("✓ No duplicate security numbers found")
        
        # 3. Security type distribution
        cursor.execute("""
            SELECT "securityType", COUNT(*) as count
            FROM "investmentDebtSecurities"
            GROUP BY "securityType"
            ORDER BY count DESC
        """)
        security_types = cursor.fetchall()
        logger.info(f"\nSecurity type distribution:")
        for sec_type, count in security_types:
            percentage = (count / total_records) * 100
            logger.info(f"  {sec_type}: {count:,} records ({percentage:.1f}%)")
        
        # 4. Currency distribution
        cursor.execute("""
            SELECT currency, COUNT(*) as count
            FROM "investmentDebtSecurities"
            GROUP BY currency
            ORDER BY count DESC
        """)
        currencies = cursor.fetchall()
        logger.info(f"\nCurrency distribution:")
        for currency, count in currencies:
            percentage = (count / total_records) * 100
            logger.info(f"  {currency}: {count:,} records ({percentage:.1f}%)")
        
        # 5. Issuer country distribution
        cursor.execute("""
            SELECT "securityIssuerCountry", COUNT(*) as count
            FROM "investmentDebtSecurities"
            GROUP BY "securityIssuerCountry"
            ORDER BY count DESC
            LIMIT 10
        """)
        countries = cursor.fetchall()
        logger.info(f"\nTop 10 issuer countries:")
        for country, count in countries:
            percentage = (count / total_records) * 100
            logger.info(f"  {country}: {count:,} records ({percentage:.1f}%)")
        
        # 6. Sector classification distribution
        cursor.execute("""
            SELECT "sectorSnaClassification", COUNT(*) as count
            FROM "investmentDebtSecurities"
            GROUP BY "sectorSnaClassification"
            ORDER BY count DESC
        """)
        sectors = cursor.fetchall()
        logger.info(f"\nSector SNA classification distribution:")
        for sector, count in sectors:
            percentage = (count / total_records) * 100
            logger.info(f"  {sector}: {count:,} records ({percentage:.1f}%)")
        
        # 7. Trading intent distribution
        cursor.execute("""
            SELECT "tradingIntent", COUNT(*) as count
            FROM "investmentDebtSecurities"
            GROUP BY "tradingIntent"
            ORDER BY count DESC
        """)
        trading_intents = cursor.fetchall()
        logger.info(f"\nTrading intent distribution:")
        for intent, count in trading_intents:
            percentage = (count / total_records) * 100
            logger.info(f"  {intent}: {count:,} records ({percentage:.1f}%)")
        
        # 8. Interest rate analysis
        cursor.execute("""
            SELECT 
                COUNT(*) as count,
                AVG(CAST("interestRate" AS DECIMAL)) as avg_rate,
                MIN(CAST("interestRate" AS DECIMAL)) as min_rate,
                MAX(CAST("interestRate" AS DECIMAL)) as max_rate
            FROM "investmentDebtSecurities"
            WHERE "interestRate" IS NOT NULL 
            AND "interestRate" != ''
            AND "interestRate" ~ '^[0-9]+\\.?[0-9]*$'
        """)
        interest_rates = cursor.fetchone()
        if interest_rates and interest_rates[0] > 0:
            count, avg_rate, min_rate, max_rate = interest_rates
            logger.info(f"\nInterest rate analysis:")
            logger.info(f"  Securities with rates: {count:,}")
            logger.info(f"  Average rate: {avg_rate:.2f}%")
            logger.info(f"  Range: {min_rate:.2f}% - {max_rate:.2f}%")
        
        # 9. Amount analysis (TZS fair value amounts)
        cursor.execute("""
            SELECT 
                COUNT(*) as total_securities,
                SUM(CAST("tzsFairValueAmount" AS DECIMAL)) as total_fair_value,
                AVG(CAST("tzsFairValueAmount" AS DECIMAL)) as avg_fair_value,
                MIN(CAST("tzsFairValueAmount" AS DECIMAL)) as min_fair_value,
                MAX(CAST("tzsFairValueAmount" AS DECIMAL)) as max_fair_value
            FROM "investmentDebtSecurities"
            WHERE "tzsFairValueAmount" IS NOT NULL 
            AND "tzsFairValueAmount" != ''
            AND "tzsFairValueAmount" ~ '^[0-9]+\\.?[0-9]*$'
            AND CAST("tzsFairValueAmount" AS DECIMAL) > 0
        """)
        amounts = cursor.fetchone()
        if amounts and amounts[0] > 0:
            total_securities, total_fair_value, avg_fair_value, min_fair_value, max_fair_value = amounts
            logger.info(f"\nFair value amounts (TZS):")
            logger.info(f"  Securities with fair values: {total_securities:,}")
            logger.info(f"  Total fair value: {total_fair_value:,.2f} TZS")
            logger.info(f"  Average fair value: {avg_fair_value:,.2f} TZS")
            logger.info(f"  Range: {min_fair_value:,.2f} - {max_fair_value:,.2f} TZS")
        
        # 10. Data completeness check
        logger.info(f"\nData completeness check:")
        required_fields = [
            'reportingDate', 'securityNumber', 'securityType', 'securityIssuerName',
            'currency', 'sectorSnaClassification', 'tradingIntent'
        ]
        
        for field in required_fields:
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM "investmentDebtSecurities" 
                WHERE "{field}" IS NULL OR "{field}" = ''
            """)
            null_count = cursor.fetchone()[0]
            completeness = ((total_records - null_count) / total_records) * 100
            status = "✓" if completeness >= 95 else "⚠" if completeness >= 90 else "✗"
            logger.info(f"  {field}: {completeness:.1f}% complete {status}")
        
        # 11. Maturity date analysis
        cursor.execute("""
            SELECT 
                MIN("maturityDate") as earliest,
                MAX("maturityDate") as latest,
                COUNT(DISTINCT "maturityDate") as unique_dates
            FROM "investmentDebtSecurities"
            WHERE "maturityDate" IS NOT NULL AND "maturityDate" != ''
        """)
        date_info = cursor.fetchone()
        if date_info:
            earliest, latest, unique_dates = date_info
            logger.info(f"\nMaturity date range:")
            logger.info(f"  Earliest: {earliest}")
            logger.info(f"  Latest: {latest}")
            logger.info(f"  Unique dates: {unique_dates:,}")
        
        # 12. Asset classification analysis
        cursor.execute("""
            SELECT "assetClassificationCategory", COUNT(*) as count
            FROM "investmentDebtSecurities"
            GROUP BY "assetClassificationCategory"
            ORDER BY count DESC
        """)
        classifications = cursor.fetchall()
        logger.info(f"\nAsset classification distribution:")
        for classification, count in classifications:
            percentage = (count / total_records) * 100
            logger.info(f"  {classification}: {count:,} records ({percentage:.1f}%)")
        
        # 13. Top issuers by fair value
        cursor.execute("""
            SELECT 
                "securityIssuerName",
                COUNT(*) as security_count,
                SUM(CAST("tzsFairValueAmount" AS DECIMAL)) as total_fair_value
            FROM "investmentDebtSecurities"
            WHERE "tzsFairValueAmount" IS NOT NULL 
            AND "tzsFairValueAmount" != ''
            AND "tzsFairValueAmount" ~ '^[0-9]+\\.?[0-9]*$'
            AND CAST("tzsFairValueAmount" AS DECIMAL) > 0
            GROUP BY "securityIssuerName"
            ORDER BY total_fair_value DESC
            LIMIT 10
        """)
        top_issuers = cursor.fetchall()
        if top_issuers:
            logger.info(f"\nTop 10 issuers by fair value:")
            for issuer, security_count, fair_value in top_issuers:
                logger.info(f"  {issuer}: {security_count} securities, {fair_value:,.2f} TZS")
        
        # 14. External rating distribution
        cursor.execute("""
            SELECT "externalIssuerRating", COUNT(*) as count
            FROM "investmentDebtSecurities"
            GROUP BY "externalIssuerRating"
            ORDER BY count DESC
        """)
        ratings = cursor.fetchall()
        logger.info(f"\nExternal issuer rating distribution:")
        for rating, count in ratings:
            percentage = (count / total_records) * 100
            logger.info(f"  {rating}: {count:,} records ({percentage:.1f}%)")
        
        cursor.close()
        conn.close()
        
        logger.info("\n" + "=" * 60)
        logger.info("DATA QUALITY VERIFICATION COMPLETED")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Error during data quality verification: {e}")
        raise

if __name__ == "__main__":
    verify_data_quality()