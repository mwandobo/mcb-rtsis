"""
Create loan_transactions table in PostgreSQL
"""

import logging
from config import Config
import psycopg2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_table():
    """Create loan_transactions table"""
    config = Config()
    
    create_table_sql = """
    DROP TABLE IF EXISTS loanTransactions CASCADE;
    
    CREATE TABLE loanTransactions (
        id SERIAL PRIMARY KEY,
        reportingDate TIMESTAMP,
        loanNumber VARCHAR(50),
        transactionDate DATE,
        loanTransactionType VARCHAR(100),
        loanTransactionSubType VARCHAR(100),
        currency VARCHAR(10),
        orgTransactionAmount DECIMAL(18, 2),
        usdTransactionAmount DECIMAL(18, 2),
        tzsTransactionAmount DECIMAL(18, 2),
        createdAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE INDEX idx_loanTransactions_loanNumber 
        ON loanTransactions(loanNumber);
    CREATE INDEX idx_loanTransactions_transactionDate 
        ON loanTransactions(transactionDate);
    CREATE INDEX idx_loanTransactions_type 
        ON loanTransactions(loanTransactionType);
    CREATE INDEX idx_loanTransactions_currency 
        ON loanTransactions(currency);
    CREATE INDEX idx_loanTransactions_reportingDate 
        ON loanTransactions(reportingDate);
    
    COMMENT ON TABLE loanTransactions IS 'Stores loan transaction data streamed from DB2 source';
    """
    
    try:
        conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info("✓ loan_transactions table created successfully")
        return True
        
    except Exception as e:
        logger.error(f"✗ Failed to create table: {e}")
        return False

if __name__ == "__main__":
    create_table()
