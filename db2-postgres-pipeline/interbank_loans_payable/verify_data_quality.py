#!/usr/bin/env python3
"""
Verify Interbank Loans Payable Data Quality
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
    """Verify the data quality in the interbankLoansPayable table"""
    
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
        logger.info("INTERBANK LOANS PAYABLE DATA QUALITY REPORT")
        logger.info("=" * 60)
        
        # 1. Basic record count
        cursor.execute('SELECT COUNT(*) FROM "interbankLoansPayable"')
        total_records = cursor.fetchone()[0]
        logger.info(f"Total records in table: {total_records:,}")
        
        # 2. Check for duplicates by account number
        cursor.execute("""
            SELECT "accountNumber", COUNT(*) as count
            FROM "interbankLoansPayable"
            GROUP BY "accountNumber"
            HAVING COUNT(*) > 1
            ORDER BY count DESC
        """)
        duplicates = cursor.fetchall()
        
        if duplicates:
            logger.warning(f"Found {len(duplicates)} duplicate account numbers:")
            for account, count in duplicates[:10]:  # Show top 10
                logger.warning(f"  Account {account}: {count} records")
            if len(duplicates) > 10:
                logger.warning(f"  ... and {len(duplicates) - 10} more")
        else:
            logger.info("✓ No duplicate account numbers found")
        
        # 3. Currency distribution
        cursor.execute("""
            SELECT currency, COUNT(*) as count
            FROM "interbankLoansPayable"
            GROUP BY currency
            ORDER BY count DESC
        """)
        currencies = cursor.fetchall()
        logger.info(f"\nCurrency distribution:")
        for currency, count in currencies:
            percentage = (count / total_records) * 100
            logger.info(f"  {currency}: {count:,} records ({percentage:.1f}%)")
        
        # 4. Borrowing type distribution
        cursor.execute("""
            SELECT "borrowingType", COUNT(*) as count
            FROM "interbankLoansPayable"
            GROUP BY "borrowingType"
            ORDER BY count DESC
        """)
        borrowing_types = cursor.fetchall()
        logger.info(f"\nBorrowing type distribution:")
        for btype, count in borrowing_types:
            percentage = (count / total_records) * 100
            logger.info(f"  {btype}: {count:,} records ({percentage:.1f}%)")
        
        # 5. Lender country distribution
        cursor.execute("""
            SELECT "lenderCountry", COUNT(*) as count
            FROM "interbankLoansPayable"
            GROUP BY "lenderCountry"
            ORDER BY count DESC
            LIMIT 10
        """)
        countries = cursor.fetchall()
        logger.info(f"\nTop 10 lender countries:")
        for country, count in countries:
            percentage = (count / total_records) * 100
            logger.info(f"  {country}: {count:,} records ({percentage:.1f}%)")
        
        # 6. Interest rate analysis
        cursor.execute("""
            SELECT 
                "interestRateType",
                COUNT(*) as count,
                AVG(CAST("annualInterestRate" AS DECIMAL)) as avg_rate,
                MIN(CAST("annualInterestRate" AS DECIMAL)) as min_rate,
                MAX(CAST("annualInterestRate" AS DECIMAL)) as max_rate
            FROM "interbankLoansPayable"
            WHERE "annualInterestRate" IS NOT NULL 
            AND "annualInterestRate" != ''
            AND "annualInterestRate" ~ '^[0-9]+\\.?[0-9]*$'
            GROUP BY "interestRateType"
        """)
        interest_rates = cursor.fetchall()
        logger.info(f"\nInterest rate analysis:")
        for rate_type, count, avg_rate, min_rate, max_rate in interest_rates:
            logger.info(f"  {rate_type}: {count:,} loans, Avg: {avg_rate:.2f}%, Range: {min_rate:.2f}% - {max_rate:.2f}%")
        
        # 7. Amount analysis (TZS amounts)
        cursor.execute("""
            SELECT 
                COUNT(*) as total_loans,
                SUM(CAST("tzsAmountClosing" AS DECIMAL)) as total_outstanding,
                AVG(CAST("tzsAmountClosing" AS DECIMAL)) as avg_outstanding,
                MIN(CAST("tzsAmountClosing" AS DECIMAL)) as min_outstanding,
                MAX(CAST("tzsAmountClosing" AS DECIMAL)) as max_outstanding
            FROM "interbankLoansPayable"
            WHERE "tzsAmountClosing" IS NOT NULL 
            AND "tzsAmountClosing" != ''
            AND "tzsAmountClosing" ~ '^[0-9]+\\.?[0-9]*$'
            AND CAST("tzsAmountClosing" AS DECIMAL) > 0
        """)
        amounts = cursor.fetchone()
        if amounts and amounts[0] > 0:
            total_loans, total_outstanding, avg_outstanding, min_outstanding, max_outstanding = amounts
            logger.info(f"\nOutstanding loan amounts (TZS):")
            logger.info(f"  Active loans: {total_loans:,}")
            logger.info(f"  Total outstanding: {total_outstanding:,.2f} TZS")
            logger.info(f"  Average loan: {avg_outstanding:,.2f} TZS")
            logger.info(f"  Range: {min_outstanding:,.2f} - {max_outstanding:,.2f} TZS")
        
        # 8. Data completeness check
        logger.info(f"\nData completeness check:")
        required_fields = [
            'reportingDate', 'lenderName', 'accountNumber', 'lenderCountry',
            'borrowingType', 'currency', 'orgAmountOpening'
        ]
        
        for field in required_fields:
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM "interbankLoansPayable" 
                WHERE "{field}" IS NULL OR "{field}" = ''
            """)
            null_count = cursor.fetchone()[0]
            completeness = ((total_records - null_count) / total_records) * 100
            status = "✓" if completeness >= 95 else "⚠" if completeness >= 90 else "✗"
            logger.info(f"  {field}: {completeness:.1f}% complete {status}")
        
        # 9. Recent data check
        cursor.execute("""
            SELECT 
                MIN("transactionDate") as earliest,
                MAX("transactionDate") as latest,
                COUNT(DISTINCT "transactionDate") as unique_dates
            FROM "interbankLoansPayable"
            WHERE "transactionDate" IS NOT NULL AND "transactionDate" != ''
        """)
        date_info = cursor.fetchone()
        if date_info:
            earliest, latest, unique_dates = date_info
            logger.info(f"\nTransaction date range:")
            logger.info(f"  Earliest: {earliest}")
            logger.info(f"  Latest: {latest}")
            logger.info(f"  Unique dates: {unique_dates:,}")
        
        # 10. Top lenders by outstanding amount
        cursor.execute("""
            SELECT 
                "lenderName",
                COUNT(*) as loan_count,
                SUM(CAST("tzsAmountClosing" AS DECIMAL)) as total_outstanding
            FROM "interbankLoansPayable"
            WHERE "tzsAmountClosing" IS NOT NULL 
            AND "tzsAmountClosing" != ''
            AND "tzsAmountClosing" ~ '^[0-9]+\\.?[0-9]*$'
            AND CAST("tzsAmountClosing" AS DECIMAL) > 0
            GROUP BY "lenderName"
            ORDER BY total_outstanding DESC
            LIMIT 10
        """)
        top_lenders = cursor.fetchall()
        if top_lenders:
            logger.info(f"\nTop 10 lenders by outstanding amount:")
            for lender, loan_count, outstanding in top_lenders:
                logger.info(f"  {lender}: {loan_count} loans, {outstanding:,.2f} TZS")
        
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