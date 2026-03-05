#!/usr/bin/env python3
"""
Create outgoingFundTransfer table in PostgreSQL
Based on outgoing-fund-transfer.sql structure
"""

import psycopg2
import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def create_outgoing_fund_transfer_table():
    """Create the outgoingFundTransfer table in PostgreSQL"""
    
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
        logger.info("Dropping existing outgoingFundTransfer table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS "outgoingFundTransfer" CASCADE')
        
        # Create outgoingFundTransfer table
        logger.info("Creating outgoingFundTransfer table...")
        create_table_sql = """
        CREATE TABLE "outgoingFundTransfer" (
            id SERIAL PRIMARY KEY,
            "reportingDate" VARCHAR(12),
            "transactionId" VARCHAR(255),
            "transactionDate" VARCHAR(12),
            "transferChannel" VARCHAR(50),
            "subCategoryTransferChannel" VARCHAR(100),
            "senderName" VARCHAR(255),
            "senderAccountNumber" VARCHAR(50),
            "senderIdentificationType" VARCHAR(50),
            "senderIdentificationNumber" VARCHAR(50),
            "recipientName" VARCHAR(255),
            "recipientMobileNumber" VARCHAR(50),
            "recipientCountry" VARCHAR(100),
            "recipientBankOrFspCode" VARCHAR(50),
            "recipientAccountOrWalletNumber" VARCHAR(50),
            "serviceChannel" VARCHAR(100),
            "serviceCategory" VARCHAR(100),
            "serviceSubCategory" VARCHAR(100),
            "currency" VARCHAR(10),
            "orgAmount" VARCHAR(50),
            "usdAmount" VARCHAR(50),
            "tzsAmount" VARCHAR(50),
            "purposes" TEXT,
            "senderInstruction" VARCHAR(255),
            "transactionPlace" VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_sql)
        
        # Create indexes for better performance
        logger.info("Creating indexes...")
        
        indexes = [
            'CREATE INDEX idx_outgoingFundTransfer_reporting_date ON "outgoingFundTransfer"("reportingDate")',
            'CREATE UNIQUE INDEX idx_outgoingFundTransfer_transaction_id ON "outgoingFundTransfer"("transactionId")',
            'CREATE INDEX idx_outgoingFundTransfer_transaction_date ON "outgoingFundTransfer"("transactionDate")',
            'CREATE INDEX idx_outgoingFundTransfer_transfer_channel ON "outgoingFundTransfer"("transferChannel")',
            'CREATE INDEX idx_outgoingFundTransfer_sender_name ON "outgoingFundTransfer"("senderName")',
            'CREATE INDEX idx_outgoingFundTransfer_recipient_name ON "outgoingFundTransfer"("recipientName")',
            'CREATE INDEX idx_outgoingFundTransfer_currency ON "outgoingFundTransfer"("currency")',
            'CREATE INDEX idx_outgoingFundTransfer_service_category ON "outgoingFundTransfer"("serviceCategory")',
            'CREATE INDEX idx_outgoingFundTransfer_created_at ON "outgoingFundTransfer"(created_at)'
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
            WHERE table_name = 'outgoingFundTransfer'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        
        logger.info("Outgoing Fund Transfer table created successfully!")
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
        
        logger.info("outgoingFundTransfer table setup completed successfully!")
        
    except Exception as e:
        logger.error(f"Error creating outgoingFundTransfer table: {e}")
        raise

if __name__ == "__main__":
    create_outgoing_fund_transfer_table()