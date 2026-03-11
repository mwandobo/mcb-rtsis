#!/usr/bin/env python3
"""
Create complaintStatistics table in PostgreSQL
Based on complaint-statistics.sql structure
"""

import psycopg2
import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def create_complaint_statistics_table():
    """Create the complaintStatistics table in PostgreSQL"""
    
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
        logger.info("Dropping existing complaintStatistics table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS "complaintStatistics" CASCADE')
        
        # Create complaintStatistics table
        logger.info("Creating complaintStatistics table...")
        create_table_sql = """
        CREATE TABLE "complaintStatistics" (
            id SERIAL PRIMARY KEY,
            "reportingDate" VARCHAR(12),
            "complainantName" VARCHAR(255),
            "complainantMobile" VARCHAR(50),
            "complaintType" VARCHAR(100),
            "occurrenceDate" VARCHAR(12),
            "complaintReportingDate" VARCHAR(12),
            "closureDate" VARCHAR(12),
            "agentName" VARCHAR(255),
            "tillNumber" VARCHAR(50),
            "currency" VARCHAR(10),
            "tzsAmount" VARCHAR(50),
            "orgAmount" VARCHAR(50),
            "usdAmount" VARCHAR(50),
            "employeeId" VARCHAR(50),
            "referredComplaints" VARCHAR(100),
            "complaintStatus" VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_sql)
        
        # Create indexes for better performance
        logger.info("Creating indexes...")
        
        indexes = [
            'CREATE INDEX idx_complaintStatistics_reporting_date ON "complaintStatistics"("reportingDate")',
            'CREATE UNIQUE INDEX idx_complaintStatistics_unique ON "complaintStatistics"("complainantName", "occurrenceDate")',
            'CREATE INDEX idx_complaintStatistics_complainant_name ON "complaintStatistics"("complainantName")',
            'CREATE INDEX idx_complaintStatistics_complaint_type ON "complaintStatistics"("complaintType")',
            'CREATE INDEX idx_complaintStatistics_occurrence_date ON "complaintStatistics"("occurrenceDate")',
            'CREATE INDEX idx_complaintStatistics_closure_date ON "complaintStatistics"("closureDate")',
            'CREATE INDEX idx_complaintStatistics_employee_id ON "complaintStatistics"("employeeId")',
            'CREATE INDEX idx_complaintStatistics_complaint_status ON "complaintStatistics"("complaintStatus")',
            'CREATE INDEX idx_complaintStatistics_currency ON "complaintStatistics"("currency")',
            'CREATE INDEX idx_complaintStatistics_created_at ON "complaintStatistics"(created_at)'
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
            WHERE table_name = 'complaintStatistics'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        
        logger.info("Complaint Statistics table created successfully!")
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
        
        logger.info("complaintStatistics table setup completed successfully!")
        
    except Exception as e:
        logger.error(f"Error creating complaintStatistics table: {e}")
        raise

if __name__ == "__main__":
    create_complaint_statistics_table()