#!/usr/bin/env python3
"""
Create investmentDebtSecurities table in PostgreSQL
Based on investment_debt_securities.sql structure
"""

import psycopg2
import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def create_investment_debt_securities_table():
    """Create the investmentDebtSecurities table in PostgreSQL"""
    
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
        logger.info("Dropping existing investmentDebtSecurities table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS "investmentDebtSecurities" CASCADE')
        
        # Create investmentDebtSecurities table
        logger.info("Creating investmentDebtSecurities table...")
        create_table_sql = """
        CREATE TABLE "investmentDebtSecurities" (
            id SERIAL PRIMARY KEY,
            "reportingDate" VARCHAR(12),
            "securityNumber" VARCHAR(50),
            "securityType" VARCHAR(50),
            "securityIssuerName" VARCHAR(255),
            "ratingStatus" VARCHAR(10),
            "externalIssuerRating" VARCHAR(50),
            "gradesUnratedBanks" VARCHAR(50),
            "securityIssuerCountry" VARCHAR(100),
            "sectorSnaClassification" VARCHAR(100),
            "currency" VARCHAR(10),
            "orgCostValueAmount" VARCHAR(50),
            "usdCostValueAmount" VARCHAR(50),
            "tzsCostValueAmount" VARCHAR(50),
            "faceValueAmount" VARCHAR(50),
            "usdFaceValueAmount" VARCHAR(50),
            "tzsFaceValueAmount" VARCHAR(50),
            "orgFairValueAmount" VARCHAR(50),
            "usdFairValueAmount" VARCHAR(50),
            "tzsFairValueAmount" VARCHAR(50),
            "interestRate" VARCHAR(20),
            "purchaseDate" VARCHAR(12),
            "valueDate" VARCHAR(12),
            "maturityDate" VARCHAR(12),
            "tradingIntent" VARCHAR(50),
            "securityEncumbranceStatus" VARCHAR(20),
            "pastDueDays" VARCHAR(10),
            "allowanceProbableLoss" VARCHAR(50),
            "botProvision" VARCHAR(50),
            "assetClassificationCategory" VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_sql)
        
        # Create indexes for better performance
        logger.info("Creating indexes...")
        
        indexes = [
            'CREATE INDEX idx_investmentDebtSecurities_reporting_date ON "investmentDebtSecurities"("reportingDate")',
            'CREATE UNIQUE INDEX idx_investmentDebtSecurities_security_number ON "investmentDebtSecurities"("securityNumber")',
            'CREATE INDEX idx_investmentDebtSecurities_security_type ON "investmentDebtSecurities"("securityType")',
            'CREATE INDEX idx_investmentDebtSecurities_issuer_name ON "investmentDebtSecurities"("securityIssuerName")',
            'CREATE INDEX idx_investmentDebtSecurities_issuer_country ON "investmentDebtSecurities"("securityIssuerCountry")',
            'CREATE INDEX idx_investmentDebtSecurities_sector_classification ON "investmentDebtSecurities"("sectorSnaClassification")',
            'CREATE INDEX idx_investmentDebtSecurities_currency ON "investmentDebtSecurities"("currency")',
            'CREATE INDEX idx_investmentDebtSecurities_purchase_date ON "investmentDebtSecurities"("purchaseDate")',
            'CREATE INDEX idx_investmentDebtSecurities_maturity_date ON "investmentDebtSecurities"("maturityDate")',
            'CREATE INDEX idx_investmentDebtSecurities_trading_intent ON "investmentDebtSecurities"("tradingIntent")',
            'CREATE INDEX idx_investmentDebtSecurities_encumbrance_status ON "investmentDebtSecurities"("securityEncumbranceStatus")',
            'CREATE INDEX idx_investmentDebtSecurities_asset_classification ON "investmentDebtSecurities"("assetClassificationCategory")',
            'CREATE INDEX idx_investmentDebtSecurities_external_rating ON "investmentDebtSecurities"("externalIssuerRating")',
            'CREATE INDEX idx_investmentDebtSecurities_created_at ON "investmentDebtSecurities"(created_at)'
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
            WHERE table_name = 'investmentDebtSecurities'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        
        logger.info("Investment Debt Securities table created successfully!")
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
        
        logger.info("investmentDebtSecurities table setup completed successfully!")
        
    except Exception as e:
        logger.error(f"Error creating investmentDebtSecurities table: {e}")
        raise

if __name__ == "__main__":
    create_investment_debt_securities_table()