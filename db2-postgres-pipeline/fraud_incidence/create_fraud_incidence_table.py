#!/usr/bin/env python3
"""
Create fraudIncidence table in PostgreSQL
Based on fraud-incidence.sql structure
"""

import psycopg2
import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def create_fraud_incidence_table():
    """Create the fraudIncidence table in PostgreSQL"""

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    config = Config()

    try:
        conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )

        cursor = conn.cursor()

        logger.info("Dropping existing fraudIncidence table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS "fraudIncidence" CASCADE')

        logger.info("Creating fraudIncidence table...")
        create_table_sql = """
        CREATE TABLE "fraudIncidence" (
            id SERIAL PRIMARY KEY,
            "reportingDate" VARCHAR(12),
            "productProcessInvolved" VARCHAR(50),
            "occurrenceDate" VARCHAR(12),
            "detectionDate" VARCHAR(12),
            "concludedInvestigationDate" VARCHAR(12),
            "discoveryModel" VARCHAR(50),
            "fspFraudIdentifiedDate" VARCHAR(12),
            "identifiedFraudCauses" VARCHAR(50),
            "fraudAmount" VARCHAR(50),
            "classification" VARCHAR(50),
            "fraudType" VARCHAR(50),
            "fraudNature" VARCHAR(50),
            "fraudDescription" TEXT,
            "victimName" VARCHAR(255),
            "accountNumber" VARCHAR(50),
            "branchCode" VARCHAR(20),
            "actionTaken" TEXT,
            "mitigationMeasures" TEXT,
            "fraudStatus" VARCHAR(50),
            "empNin" TEXT,
            "fraudsterName" TEXT,
            "fraudsterCountry" TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE ("accountNumber", "occurrenceDate")
        )
        """

        cursor.execute(create_table_sql)

        logger.info("Creating indexes...")
        indexes = [
            'CREATE INDEX idx_fraudIncidence_reporting_date ON "fraudIncidence"("reportingDate")',
            'CREATE INDEX idx_fraudIncidence_occurrence_date ON "fraudIncidence"("occurrenceDate")',
            'CREATE INDEX idx_fraudIncidence_account_number ON "fraudIncidence"("accountNumber")',
            'CREATE INDEX idx_fraudIncidence_fraud_status ON "fraudIncidence"("fraudStatus")',
            'CREATE INDEX idx_fraudIncidence_classification ON "fraudIncidence"("classification")',
            'CREATE INDEX idx_fraudIncidence_branch_code ON "fraudIncidence"("branchCode")',
            'CREATE INDEX idx_fraudIncidence_created_at ON "fraudIncidence"(created_at)',
        ]

        for index_sql in indexes:
            cursor.execute(index_sql)
            index_name = index_sql.split('INDEX')[1].split('ON')[0].strip()
            logger.info(f"Created index: {index_name}")

        conn.commit()

        cursor.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'fraudIncidence'
            ORDER BY ordinal_position
        """)

        columns = cursor.fetchall()

        logger.info("fraudIncidence table created successfully!")
        logger.info(f"Total columns: {len(columns)}")
        for col in columns:
            logger.info(f"  {col[0]}: {col[1]} ({'NULL' if col[2] == 'YES' else 'NOT NULL'})")

        cursor.close()
        conn.close()

        logger.info("fraudIncidence table setup completed successfully!")

    except Exception as e:
        logger.error(f"Error creating fraudIncidence table: {e}")
        raise

if __name__ == "__main__":
    create_fraud_incidence_table()
