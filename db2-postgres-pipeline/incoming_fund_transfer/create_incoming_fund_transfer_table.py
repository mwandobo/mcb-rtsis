#!/usr/bin/env python3
"""
Create incomingFundTransfer table in PostgreSQL
Based on incoming-fund-transfer.sql structure
"""

import psycopg2
import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def create_incoming_fund_transfer_table():
    """Create the incomingFundTransfer table in PostgreSQL"""
    
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
        
        # Drop table if exists
        logger.info("Dropping existing incomingFundTransfer table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS "incomingFundTransfer" CASCADE')
        
        # Create incomingFundTransfer table
        logger.info("Creating incomingFundTransfer table...")
        create_table_sql = """
        CREATE TABLE "incomingFundTransfer" (
            id SERIAL PRIMARY KEY,
            "reportingDate" VARCHAR(12),
            "transactionId" VARCHAR(255),
            "transactionDate" VARCHAR(12),
            "reportingDateTimestamp" VARCHAR(50),
            "transferChannel" VARCHAR(50),
            "subCategoryTransferChannel" VARCHAR(100),
            "recipientName" VARCHAR(255),
            "senderAccountNumber" VARCHAR(50),
            "recipientIdentificationType" VARCHAR(50),
            "recipientIdentificationNumber" VARCHAR(50),
            "recipientCountry" VARCHAR(100),
            "senderName" VARCHAR(255),
            "senderBankOrFspCode" VARCHAR(50),
            "senderAccountOrWalletNumber" VARCHAR(50),
            "serviceCategory" VARCHAR(100),
            "serviceSubCategory" VARCHAR(100),
            "currency" VARCHAR(10),
            "orgAmount" VARCHAR(50),
            "usdAmount" VARCHAR(50),
            "tzsAmount" VARCHAR(50),
            "senderInstruction" VARCHAR(255),
            "purposes" TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_sql)
        
        # Create indexes for better performance
        logger.info("Creating indexes...")
        
        indexes = [
            'CREATE INDEX idx_incomingFundTransfer_reporting_date ON "incomingFundTransfer"("reportingDate")',
            'CREATE UNIQUE INDEX idx_incomingFundTransfer_transaction_id ON "incomingFundTransfer"("transactionId")',
            'CREATE INDEX idx_incomingFundTransfer_transaction_date ON "incomingFundTransfer"("transactionDate")',
            'CREATE INDEX idx_incomingFundTransfer_transfer_channel ON "incomingFundTransfer"("transferChannel")',
            'CREATE INDEX idx_incomingFundTransfer_recipient_name ON "incomingFundTransfer"("recipientName")',
            'CREATE INDEX idx_incomingFundTransfer_sender_name ON "incomingFundTransfer"("senderName")',
            'CREATE INDEX idx_incomingFundTransfer_currency ON "incomingFundTransfer"("currency")',
            'CREATE INDEX idx_incomingFundTransfer_service_category ON "incomingFundTransfer"("serviceCategory")',
            'CREATE INDEX idx_incomingFundTransfer_created_at ON "incomingFundTransfer"(created_at)'
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
            # Extract index name from SQL
            index_name = index_sql.split('INDEX')[1].split('ON')[0].strip()
            logger.info(f"Created index: {index_name}")
        
        # Commit changes
        conn.commit()
        
        # Get table info
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'incomingFundTransfer'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        
        logger.info("Incoming Fund Transfer table created successfully!")
        logger.info("Table structure:")
        logger.info("-" * 80)
        logger.info(f"{'Column Name':<35} {'Data Type':<20} {'Max Length':<12} {'Nullable':<10}")
        logger.info("-" * 80)
        
        for col in columns:
            col_name, data_type, max_length, nullable = col
            max_len_str = str(max_length) if max_length else 'N/A'
            logger.info(f"{col_name:<35} {data_type:<20} {max_len_str:<12} {nullable:<10}")
        
        logger.info("-" * 80)
        logger.info(f"Total columns: {len(columns)}")
        
        cursor.close()
        conn.close()
        
        logger.info("incomingFundTransfer table setup completed successfully!")
        
    except Exception as e:
        logger.error(f"Error creating incomingFundTransfer table: {e}")
        raise

if __name__ == "__main__":
    create_incoming_fund_transfer_table()