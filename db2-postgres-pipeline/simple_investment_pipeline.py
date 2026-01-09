#!/usr/bin/env python3
"""
Simple Investment Debt Securities Pipeline
Process each part of the UNION query separately
"""

import logging
import sys
from datetime import datetime
from typing import Dict, Any, List

import psycopg2
from psycopg2.extras import RealDictCursor

from config import Config
from db2_connection import DB2Connection
from processors.investment_debt_securities_processor import InvestmentDebtSecuritiesProcessor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/simple_investment_pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

class SimpleInvestmentPipeline:
    """Simple pipeline for processing investment debt securities data"""
    
    def __init__(self):
        self.config = Config()
        self.db2_conn = DB2Connection()
        self.processor = InvestmentDebtSecuritiesProcessor()
        self.pg_conn = None
        
        # Statistics
        self.stats = {
            'total_processed': 0,
            'successful_inserts': 0,
            'failed_inserts': 0,
            'start_time': None,
            'end_time': None
        }
    
    def connect_postgresql(self):
        """Establish connection to PostgreSQL"""
        try:
            self.pg_conn = psycopg2.connect(
                host=self.config.database.pg_host,
                port=self.config.database.pg_port,
                database=self.config.database.pg_database,
                user=self.config.database.pg_user,
                password=self.config.database.pg_password,
                cursor_factory=RealDictCursor
            )
            logger.info("Connected to PostgreSQL database")
            
        except Exception as e:
            logger.error(f"PostgreSQL connection failed: {e}")
            raise
    
    def fetch_deposit_account_data(self) -> List[Dict[str, Any]]:
        """Fetch investment securities from DEPOSIT_ACCOUNT"""
        try:
            logger.info("Fetching investment securities from DEPOSIT_ACCOUNT...")
            
            query = """
            SELECT
                CURRENT_TIMESTAMP AS reportingDate,
                CAST(da.ACCOUNT_NUMBER AS VARCHAR(50)) AS securityNumber,
                CASE 
                    WHEN da.DEPOSIT_TYPE = '1' THEN 'Corporate bonds'
                    WHEN da.DEPOSIT_TYPE = '2' THEN 'Treasury bonds'
                    WHEN da.DEPOSIT_TYPE = '3' THEN 'Treasury bills'
                    WHEN da.DEPOSIT_TYPE = '4' THEN 'RGOZ Treasury bond'
                    WHEN da.DEPOSIT_TYPE = '5' THEN 'Municipal/Local Government bond'
                    ELSE 'Others investments'
                END AS securityType,
                CASE 
                    WHEN da.DEPOSIT_TYPE = '2' THEN 'Government of Tanzania'
                    WHEN da.DEPOSIT_TYPE = '3' THEN 'Bank of Tanzania'
                    WHEN da.DEPOSIT_TYPE = '4' THEN 'Government of Tanzania'
                    WHEN da.DEPOSIT_TYPE = '5' THEN 'Local Government Authority'
                    ELSE 'Unknown Issuer'
                END AS securityIssuerName,
                CASE 
                    WHEN da.DEPOSIT_TYPE IN ('2', '3', '4') THEN 'AAA'
                    WHEN da.DEPOSIT_TYPE = '5' THEN 'A'
                    ELSE NULL
                END AS externalIssuerRatting,
                NULL AS gradesUnratedBanks,
                'Tanzania' AS securityIssuerCountry,
                CASE 
                    WHEN da.DEPOSIT_TYPE IN ('2', '3', '4') THEN 'Central Government'
                    WHEN da.DEPOSIT_TYPE = '5' THEN 'Local Government'
                    ELSE 'Other Non-Financial Corporations'
                END AS snaIssuerSector,
                COALESCE(cur.SHORT_DESCR, 'TZS') AS currency,
                COALESCE(da.OPENING_BALANCE, da.BOOK_BALANCE, 0) AS orgCostValueAmount,
                CASE 
                    WHEN cur.SHORT_DESCR = 'USD' 
                        THEN COALESCE(da.OPENING_BALANCE, da.BOOK_BALANCE, 0) * 2730.50
                    ELSE COALESCE(da.OPENING_BALANCE, da.BOOK_BALANCE, 0)
                END AS tzsCostValueAmount,
                CASE 
                    WHEN cur.SHORT_DESCR = 'USD' 
                        THEN COALESCE(da.OPENING_BALANCE, da.BOOK_BALANCE, 0)
                    WHEN cur.SHORT_DESCR = 'TZS'
                        THEN COALESCE(da.OPENING_BALANCE, da.BOOK_BALANCE, 0) / 2730.50
                    ELSE NULL
                END AS usdCostValueAmount,
                COALESCE(da.BOOK_BALANCE, 0) AS orgFaceValueAmount,
                CASE 
                    WHEN cur.SHORT_DESCR = 'USD' 
                        THEN COALESCE(da.BOOK_BALANCE, 0) * 2730.50
                    ELSE COALESCE(da.BOOK_BALANCE, 0)
                END AS tzsgFaceValueAmount,
                CASE 
                    WHEN cur.SHORT_DESCR = 'USD' 
                        THEN COALESCE(da.BOOK_BALANCE, 0)
                    WHEN cur.SHORT_DESCR = 'TZS'
                        THEN COALESCE(da.BOOK_BALANCE, 0) / 2730.50
                    ELSE NULL
                END AS usdgFaceValueAmount,
                COALESCE(da.AVAILABLE_BALANCE, da.BOOK_BALANCE, 0) AS orgFairValueAmount,
                CASE 
                    WHEN cur.SHORT_DESCR = 'USD' 
                        THEN COALESCE(da.AVAILABLE_BALANCE, da.BOOK_BALANCE, 0) * 2730.50
                    ELSE COALESCE(da.AVAILABLE_BALANCE, da.BOOK_BALANCE, 0)
                END AS tzsgFairValueAmount,
                CASE 
                    WHEN cur.SHORT_DESCR = 'USD' 
                        THEN COALESCE(da.AVAILABLE_BALANCE, da.BOOK_BALANCE, 0)
                    WHEN cur.SHORT_DESCR = 'TZS'
                        THEN COALESCE(da.AVAILABLE_BALANCE, da.BOOK_BALANCE, 0) / 2730.50
                    ELSE NULL
                END AS usdgFairValueAmount,
                COALESCE(da.FIXED_INTER_RATE, 0) AS interestRate,
                da.OPENING_DATE AS purchaseDate,
                COALESCE(da.START_DATE_TD, da.OPENING_DATE) AS valueDate,
                COALESCE(da.EXPIRY_DATE_TD, da.EXPIRY_DATE) AS maturityDate,
                CASE 
                    WHEN da.DEPOSIT_TYPE IN ('2', '3', '4') THEN 'Hold to Maturity'
                    WHEN da.DEPOSIT_TYPE = '5' THEN 'Hold to Maturity'
                    ELSE 'Available for Sale'
                END AS tradingIntent,
                CASE 
                    WHEN da.COLLATERAL_FLG = '1' THEN 'Encumbered'
                    ELSE 'Unencumbered'
                END AS securityEncumbaranceStatus,
                CASE 
                    WHEN COALESCE(da.EXPIRY_DATE_TD, da.EXPIRY_DATE) IS NOT NULL 
                         AND COALESCE(da.EXPIRY_DATE_TD, da.EXPIRY_DATE) < CURRENT_DATE
                        THEN DAYS(CURRENT_DATE) - DAYS(COALESCE(da.EXPIRY_DATE_TD, da.EXPIRY_DATE))
                    ELSE 0
                END AS pastDueDays,
                CAST(0 AS DECIMAL(15, 2)) AS allowanceProbableLoss,
                CASE 
                    WHEN da.ENTRY_STATUS NOT IN ('1', '6') THEN 5
                    WHEN COALESCE(da.EXPIRY_DATE_TD, da.EXPIRY_DATE) IS NULL 
                         OR COALESCE(da.EXPIRY_DATE_TD, da.EXPIRY_DATE) >= CURRENT_DATE
                        THEN 1
                    WHEN DAYS(CURRENT_DATE) - DAYS(COALESCE(da.EXPIRY_DATE_TD, da.EXPIRY_DATE)) <= 90
                        THEN 2
                    WHEN DAYS(CURRENT_DATE) - DAYS(COALESCE(da.EXPIRY_DATE_TD, da.EXPIRY_DATE)) <= 180
                        THEN 3
                    WHEN DAYS(CURRENT_DATE) - DAYS(COALESCE(da.EXPIRY_DATE_TD, da.EXPIRY_DATE)) <= 365
                        THEN 4
                    ELSE 5
                END AS assetClassificationCategory
            FROM DEPOSIT_ACCOUNT da
            LEFT JOIN CUSTOMER c ON da.FK_CUSTOMERCUST_ID = c.CUST_ID
            LEFT JOIN CURRENCY cur ON da.FK_CURRENCYID_CURR = cur.ID_CURRENCY
            WHERE da.DEPOSIT_TYPE IN ('1', '2', '3', '4', '5')
            AND da.ENTRY_STATUS IN ('1', '6')
            AND (da.BOOK_BALANCE > 0 OR da.AVAILABLE_BALANCE > 0 OR da.OPENING_BALANCE > 0)
            ORDER BY da.DEPOSIT_TYPE, da.ACCOUNT_NUMBER
            FETCH FIRST 1000 ROWS ONLY
            """
            
            records = []
            
            with self.db2_conn.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                
                # Get column names (DB2 returns them in uppercase)
                columns = [column[0].lower() for column in cursor.description]
                for row in cursor.fetchall():
                    record = dict(zip(columns, row))
                    records.append(record)
            
            logger.info(f"Fetched {len(records)} investment securities from DEPOSIT_ACCOUNT")
            return records
            
        except Exception as e:
            logger.error(f"Error fetching DEPOSIT_ACCOUNT data: {e}")
            raise
    
    def process_and_insert_batch(self, records: List[Dict[str, Any]]) -> Dict[str, int]:
        """Process and insert a batch of records"""
        batch_stats = {'successful': 0, 'failed': 0}
        
        try:
            with self.pg_conn.cursor() as cursor:
                upsert_query = self.processor.get_upsert_query()
                
                for i, record in enumerate(records):
                    try:
                        # Process the record
                        processed_record = self.processor.process_record(record)
                        
                        # Get insert parameters
                        params = self.processor.get_insert_params(processed_record)
                        
                        # Execute upsert
                        cursor.execute(upsert_query, params)
                        batch_stats['successful'] += 1
                        
                        if batch_stats['successful'] % 100 == 0:
                            logger.info(f"Processed {batch_stats['successful']} records...")
                        
                    except Exception as e:
                        batch_stats['failed'] += 1
                        logger.error(f"Error processing record {i+1}: {e}")
                        logger.debug(f"Failed record: {record}")
                        continue
                
                # Commit the batch
                self.pg_conn.commit()
                logger.info(f"Batch committed: {batch_stats['successful']} successful, {batch_stats['failed']} failed")
                
        except Exception as e:
            logger.error(f"Error in batch processing: {e}")
            self.pg_conn.rollback()
            raise
        
        return batch_stats
    
    def verify_data(self):
        """Verify the inserted data"""
        try:
            with self.pg_conn.cursor() as cursor:
                cursor.execute('SELECT COUNT(*) as total FROM "investmentDebtSecurities"')
                total_count = cursor.fetchone()['total']
                
                cursor.execute('''
                    SELECT "securityType", COUNT(*) as count 
                    FROM "investmentDebtSecurities" 
                    GROUP BY "securityType" 
                    ORDER BY count DESC
                ''')
                type_counts = cursor.fetchall()
                
                logger.info("Investment Debt Securities Data Verification:")
                logger.info(f"   Total Records: {total_count}")
                
                logger.info("   Security Types:")
                for row in type_counts:
                    logger.info(f"     {row['securityType']}: {row['count']} records")
                
        except Exception as e:
            logger.error(f"Error verifying data: {e}")
    
    def run_pipeline(self):
        """Run the simple investment debt securities pipeline"""
        try:
            self.stats['start_time'] = datetime.now()
            logger.info("Starting Simple Investment Debt Securities Pipeline")
            
            # Connect to PostgreSQL
            self.connect_postgresql()
            
            # Fetch data from DEPOSIT_ACCOUNT
            records = self.fetch_deposit_account_data()
            self.stats['total_processed'] = len(records)
            
            if not records:
                logger.warning("No investment debt securities records found")
                return
            
            # Process and insert data
            logger.info("Processing and inserting investment debt securities data...")
            batch_stats = self.process_and_insert_batch(records)
            
            self.stats['successful_inserts'] = batch_stats['successful']
            self.stats['failed_inserts'] = batch_stats['failed']
            
            # Verify data
            self.verify_data()
            
            self.stats['end_time'] = datetime.now()
            duration = self.stats['end_time'] - self.stats['start_time']
            
            logger.info("Simple Investment Debt Securities Pipeline completed!")
            logger.info(f"Final Statistics:")
            logger.info(f"   Total Processed: {self.stats['total_processed']}")
            logger.info(f"   Successful Inserts: {self.stats['successful_inserts']}")
            logger.info(f"   Failed Inserts: {self.stats['failed_inserts']}")
            logger.info(f"   Duration: {duration}")
            if self.stats['total_processed'] > 0:
                logger.info(f"   Success Rate: {(self.stats['successful_inserts']/self.stats['total_processed']*100):.1f}%")
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise
        finally:
            if self.pg_conn:
                self.pg_conn.close()

def main():
    """Main function"""
    try:
        pipeline = SimpleInvestmentPipeline()
        pipeline.run_pipeline()
        
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()