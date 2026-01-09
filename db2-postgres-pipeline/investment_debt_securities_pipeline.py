#!/usr/bin/env python3
"""
Investment Debt Securities Pipeline for RTSIS Reporting
Processes both Government bonds from GLI_TRX_EXTRACT and Investment securities from DEPOSIT_ACCOUNT
"""

import logging
import sys
import time
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
        logging.FileHandler('logs/investment_debt_securities_pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

class InvestmentDebtSecuritiesPipeline:
    """Pipeline for processing investment debt securities data"""
    
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
            # PostgreSQL Connection
            self.pg_conn = psycopg2.connect(
                host=self.config.database.pg_host,
                port=self.config.database.pg_port,
                database=self.config.database.pg_database,
                user=self.config.database.pg_user,
                password=self.config.database.pg_password,
                cursor_factory=RealDictCursor
            )
            
            logger.info("✅ Connected to PostgreSQL database")
            
        except Exception as e:
            logger.error(f"❌ PostgreSQL connection failed: {e}")
            raise
    
    def disconnect_postgresql(self):
        """Close PostgreSQL connection"""
        try:
            if self.pg_conn:
                self.pg_conn.close()
                logger.info("🔌 Disconnected from PostgreSQL")
                
        except Exception as e:
            logger.error(f"❌ Error disconnecting PostgreSQL: {e}")
    
    def fetch_investment_debt_securities_data(self) -> List[Dict[str, Any]]:
        """Fetch investment debt securities data from DB2"""
        try:
            table_config = self.config.tables['investmentDebtSecurities']
            query = table_config.query
            
            logger.info("📊 Executing investment debt securities query...")
            logger.debug(f"Query: {query}")
            
            records = []
            
            with self.db2_conn.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                
                # Fetch all results
                columns = [column[0] for column in cursor.description]
                for row in cursor.fetchall():
                    record = dict(zip(columns, row))
                    records.append(record)
            
            logger.info(f"📈 Fetched {len(records)} investment debt securities records from DB2")
            return records
            
        except Exception as e:
            logger.error(f"❌ Error fetching investment debt securities data: {e}")
            raise
    
    def process_and_insert_batch(self, records: List[Dict[str, Any]]) -> Dict[str, int]:
        """Process and insert a batch of records"""
        batch_stats = {'successful': 0, 'failed': 0}
        
        try:
            with self.pg_conn.cursor() as cursor:
                upsert_query = self.processor.get_upsert_query()
                
                for record in records:
                    try:
                        # Process the record
                        processed_record = self.processor.process_record(record)
                        
                        # Get insert parameters
                        params = self.processor.get_insert_params(processed_record)
                        
                        # Execute upsert
                        cursor.execute(upsert_query, params)
                        batch_stats['successful'] += 1
                        
                        if batch_stats['successful'] % 100 == 0:
                            logger.info(f"📝 Processed {batch_stats['successful']} records...")
                        
                    except Exception as e:
                        batch_stats['failed'] += 1
                        logger.error(f"❌ Error processing record: {e}")
                        logger.debug(f"Failed record: {record}")
                        continue
                
                # Commit the batch
                self.pg_conn.commit()
                logger.info(f"✅ Batch committed: {batch_stats['successful']} successful, {batch_stats['failed']} failed")
                
        except Exception as e:
            logger.error(f"❌ Error in batch processing: {e}")
            self.pg_conn.rollback()
            raise
        
        return batch_stats
    
    def verify_data(self):
        """Verify the inserted data"""
        try:
            with self.pg_conn.cursor() as cursor:
                # Count total records
                cursor.execute('SELECT COUNT(*) as total FROM "investmentDebtSecurities"')
                total_count = cursor.fetchone()['total']
                
                # Count by security type
                cursor.execute('''
                    SELECT "securityType", COUNT(*) as count 
                    FROM "investmentDebtSecurities" 
                    GROUP BY "securityType" 
                    ORDER BY count DESC
                ''')
                type_counts = cursor.fetchall()
                
                # Count by currency
                cursor.execute('''
                    SELECT "currency", COUNT(*) as count 
                    FROM "investmentDebtSecurities" 
                    GROUP BY "currency" 
                    ORDER BY count DESC
                ''')
                currency_counts = cursor.fetchall()
                
                # Sum amounts by currency
                cursor.execute('''
                    SELECT 
                        "currency",
                        SUM("orgCostValueAmount") as total_cost,
                        SUM("orgFaceValueAmount") as total_face,
                        SUM("orgFairValueAmount") as total_fair
                    FROM "investmentDebtSecurities" 
                    WHERE "orgCostValueAmount" IS NOT NULL
                    GROUP BY "currency" 
                    ORDER BY total_cost DESC
                ''')
                amount_sums = cursor.fetchall()
                
                logger.info("📊 Investment Debt Securities Data Verification:")
                logger.info(f"   Total Records: {total_count}")
                
                logger.info("   Security Types:")
                for row in type_counts:
                    logger.info(f"     {row['securityType']}: {row['count']} records")
                
                logger.info("   Currency Distribution:")
                for row in currency_counts:
                    logger.info(f"     {row['currency']}: {row['count']} records")
                
                logger.info("   Amount Totals by Currency:")
                for row in amount_sums:
                    logger.info(f"     {row['currency']}: Cost={row['total_cost']:,.2f}, Face={row['total_face']:,.2f}, Fair={row['total_fair']:,.2f}")
                
        except Exception as e:
            logger.error(f"❌ Error verifying data: {e}")
    
    def run_pipeline(self):
        """Run the complete investment debt securities pipeline"""
        try:
            self.stats['start_time'] = datetime.now()
            logger.info("🚀 Starting Investment Debt Securities Pipeline")
            
            # Connect to PostgreSQL
            self.connect_postgresql()
            
            # Fetch data from DB2
            records = self.fetch_investment_debt_securities_data()
            self.stats['total_processed'] = len(records)
            
            if not records:
                logger.warning("⚠️ No investment debt securities records found")
                return
            
            # Process and insert data
            logger.info("💾 Processing and inserting investment debt securities data...")
            batch_stats = self.process_and_insert_batch(records)
            
            self.stats['successful_inserts'] = batch_stats['successful']
            self.stats['failed_inserts'] = batch_stats['failed']
            
            # Verify data
            self.verify_data()
            
            self.stats['end_time'] = datetime.now()
            duration = self.stats['end_time'] - self.stats['start_time']
            
            logger.info("🎉 Investment Debt Securities Pipeline completed successfully!")
            logger.info(f"📊 Final Statistics:")
            logger.info(f"   Total Processed: {self.stats['total_processed']}")
            logger.info(f"   Successful Inserts: {self.stats['successful_inserts']}")
            logger.info(f"   Failed Inserts: {self.stats['failed_inserts']}")
            logger.info(f"   Duration: {duration}")
            logger.info(f"   Success Rate: {(self.stats['successful_inserts']/self.stats['total_processed']*100):.1f}%")
            
        except Exception as e:
            logger.error(f"❌ Pipeline failed: {e}")
            raise
        finally:
            self.disconnect_postgresql()

def main():
    """Main function"""
    try:
        pipeline = InvestmentDebtSecuritiesPipeline()
        pipeline.run_pipeline()
        
    except KeyboardInterrupt:
        logger.info("⏹️ Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"💥 Pipeline failed with error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()