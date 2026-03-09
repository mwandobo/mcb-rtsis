#!/usr/bin/env python3
"""
Create agent_transactions table in PostgreSQL
Based on agent-transaction-v1.sql structure
"""

import psycopg2
import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def create_agent_transactions_table():
    """Create the agent_transactions table in PostgreSQL"""
    
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
        logger.info("Dropping existing agentTransactions table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS "agentTransactions" CASCADE')
        
        # Create agentTransactions table
        logger.info("Creating agentTransactions table...")
        create_table_sql = """
        CREATE TABLE "agentTransactions" (
            id SERIAL PRIMARY KEY,
            "reportingDate" VARCHAR(12),
            "agentId" VARCHAR(50),
            "agentStatus" VARCHAR(20),
            "transactionDate" VARCHAR(12),
            "transactionId" VARCHAR(255),
            "transactionType" VARCHAR(50),
            "serviceChannel" VARCHAR(50),
            "tillNumber" VARCHAR(50),
            "currency" VARCHAR(10),
            "tzsAmount" DECIMAL(18,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_sql)
        
        # Create indexes for better performance
        logger.info("Creating indexes...")
        
        indexes = [
            'CREATE INDEX idx_agentTransactions_agent_id ON "agentTransactions"("agentId")',
            'CREATE INDEX idx_agentTransactions_transaction_date ON "agentTransactions"("transactionDate")',
            'CREATE UNIQUE INDEX idx_agentTransactions_transaction_id ON "agentTransactions"("transactionId")',
            'CREATE INDEX idx_agentTransactions_transaction_type ON "agentTransactions"("transactionType")',
            'CREATE INDEX idx_agentTransactions_reporting_date ON "agentTransactions"("reportingDate")',
            'CREATE INDEX idx_agentTransactions_created_at ON "agentTransactions"(created_at)'
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
            # Handle both CREATE INDEX and CREATE UNIQUE INDEX
            index_name = index_sql.split('ON')[0]
            if 'CREATE UNIQUE INDEX' in index_name:
                index_name = index_name.split('CREATE UNIQUE INDEX')[1].strip()
            else:
                index_name = index_name.split('CREATE INDEX')[1].strip()
            logger.info(f"Created index: {index_name}")
        
        # Commit changes
        conn.commit()
        
        # Get table info
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'agentTransactions'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        
        logger.info("Agent Transactions table created successfully!")
        logger.info("Table structure:")
        logger.info("-" * 80)
        logger.info(f"{'Column Name':<25} {'Data Type':<20} {'Max Length':<12} {'Nullable':<10}")
        logger.info("-" * 80)
        
        for col in columns:
            col_name, data_type, max_length, nullable = col
            max_len_str = str(max_length) if max_length else 'N/A'
            logger.info(f"{col_name:<25} {data_type:<20} {max_len_str:<12} {nullable:<10}")
        
        logger.info("-" * 80)
        logger.info(f"Total columns: {len(columns)}")
        
        cursor.close()
        conn.close()
        
        logger.info("agentTransactions table setup completed successfully!")
        
    except Exception as e:
        logger.error(f"Error creating agentTransactions table: {e}")
        raise

if __name__ == "__main__":
    create_agent_transactions_table()