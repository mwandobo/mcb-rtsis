#!/usr/bin/env python3
"""
Create Digital Credit Table in PostgreSQL
"""

import psycopg2
import logging
from config import Config

def create_digital_credit_table():
    """Create the digital credit table in PostgreSQL"""
    config = Config()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
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
        logger.info("🗑️ Dropping existing digitalCredit table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS "digitalCredit" CASCADE;')
        
        # Create Digital Credit table
        logger.info("🏗️ Creating digitalCredit table...")
        create_table_sql = """
        CREATE TABLE "digitalCredit" (
            "id" SERIAL PRIMARY KEY,
            "reportingDate" TIMESTAMP,
            "customerName" VARCHAR(50),
            "gender" VARCHAR(20),
            "disabilityStatus" VARCHAR(50),
            "customerIdentificationNumber" VARCHAR(50),
            "institutionCode" VARCHAR(10),
            "branchCode" VARCHAR(20),
            "servicesFacilitator" VARCHAR(100),
            "productName" VARCHAR(200),
            "tzsLoanDisbursedAmount" DECIMAL(15,2),
            "loanDisbursementDate" DATE,
            "tzsLoanBalance" DECIMAL(15,2),
            "maturityDate" DATE,
            "loanId" VARCHAR(50),
            "lastDepositDate" DATE,
            "lastDepositAmount" DECIMAL(15,2),
            "paymentsInstallment" INTEGER,
            "repaymentsFrequency" VARCHAR(50),
            "loanAmotizationType" VARCHAR(50),
            "cycleNumber" INTEGER,
            "loanAmountPaid" DECIMAL(15,2),
            "deliquenceDate" DATE,
            "restructuringDate" DATE,
            "interestRate" DECIMAL(8,4),
            "pastDueDays" INTEGER,
            "pastDueAmount" DECIMAL(15,2),
            "currency" VARCHAR(10),
            "orgAccruedInterest" DECIMAL(15,2),
            "tzsAccruedInterest" DECIMAL(15,2),
            "usdAccruedInterest" DECIMAL(15,2),
            "assetClassification" VARCHAR(50),
            "allowanceProbableLoss" DECIMAL(15,2),
            "botProvision" DECIMAL(15,2),
            "interestSuspended" DECIMAL(15,2)
        );
        """
        cursor.execute(create_table_sql)
        
        # Create indexes
        logger.info("📊 Creating indexes...")
        indexes = [
            'CREATE UNIQUE INDEX IF NOT EXISTS idx_digital_credit_unique ON "digitalCredit"("loanId");',
            'CREATE INDEX IF NOT EXISTS idx_digital_credit_customer ON "digitalCredit"("customerName");',
            'CREATE INDEX IF NOT EXISTS idx_digital_credit_customer_id ON "digitalCredit"("customerIdentificationNumber");',
            'CREATE INDEX IF NOT EXISTS idx_digital_credit_branch ON "digitalCredit"("branchCode");',
            'CREATE INDEX IF NOT EXISTS idx_digital_credit_facilitator ON "digitalCredit"("servicesFacilitator");',
            'CREATE INDEX IF NOT EXISTS idx_digital_credit_product ON "digitalCredit"("productName");',
            'CREATE INDEX IF NOT EXISTS idx_digital_credit_disbursement ON "digitalCredit"("loanDisbursementDate");',
            'CREATE INDEX IF NOT EXISTS idx_digital_credit_maturity ON "digitalCredit"("maturityDate");',
            'CREATE INDEX IF NOT EXISTS idx_digital_credit_currency ON "digitalCredit"("currency");',
            'CREATE INDEX IF NOT EXISTS idx_digital_credit_balance ON "digitalCredit"("tzsLoanBalance");',
            'CREATE INDEX IF NOT EXISTS idx_digital_credit_classification ON "digitalCredit"("assetClassification");'
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
        
        # Commit changes
        conn.commit()
        
        logger.info("✅ Digital credit table and indexes created successfully!")
        
        # Show table info
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length 
            FROM information_schema.columns 
            WHERE table_name = 'digitalCredit' 
            ORDER BY ordinal_position;
        """)
        
        columns = cursor.fetchall()
        logger.info("📋 Table structure:")
        for col_name, data_type, max_length in columns:
            length_info = f"({max_length})" if max_length else ""
            logger.info(f"  {col_name}: {data_type}{length_info}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Failed to create digital credit table: {e}")
        raise

if __name__ == "__main__":
    create_digital_credit_table()