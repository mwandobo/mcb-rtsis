#!/usr/bin/env python3
"""
Create employee table in PostgreSQL
Based on employee-v1.sql structure
"""

import psycopg2
import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def create_employee_table():
    """Create the employee table in PostgreSQL"""
    
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
        logger.info("Dropping existing employeesInformation table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS "employeesInformation" CASCADE')
        
        # Create employeesInformation table
        logger.info("Creating employeesInformation table...")
        create_table_sql = """
        CREATE TABLE "employeesInformation" (
            id SERIAL PRIMARY KEY,
            "reportingDate" VARCHAR(12),
            "branchCode" VARCHAR(50),
            "empName" VARCHAR(255),
            "gender" VARCHAR(20),
            "empDob" VARCHAR(12),
            "empIdentificationType" VARCHAR(50),
            "empIdentificationNumber" VARCHAR(50) UNIQUE,
            "empPosition" VARCHAR(255),
            "empPositionCategory" VARCHAR(100),
            "empStatus" VARCHAR(100),
            "empDepartment" VARCHAR(255),
            "appointmentDate" VARCHAR(12),
            "empNationality" VARCHAR(100),
            "lastPromotionDate" VARCHAR(12),
            "basicSalary" INTEGER,
            "nhif" INTEGER,
            "psssf" INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_sql)
        
        # Create indexes for better performance
        logger.info("Creating indexes...")
        
        indexes = [
            'CREATE INDEX IF NOT EXISTS idx_employeesInformation_reporting_date ON "employeesInformation"("reportingDate")',
            'CREATE UNIQUE INDEX IF NOT EXISTS idx_employeesInformation_emp_id_number ON "employeesInformation"("empIdentificationNumber")',
            'CREATE INDEX IF NOT EXISTS idx_employeesInformation_branch_code ON "employeesInformation"("branchCode")',
            'CREATE INDEX IF NOT EXISTS idx_employeesInformation_name ON "employeesInformation"("empName")',
            'CREATE INDEX IF NOT EXISTS idx_employeesInformation_gender ON "employeesInformation"("gender")',
            'CREATE INDEX IF NOT EXISTS idx_employeesInformation_position ON "employeesInformation"("empPosition")',
            'CREATE INDEX IF NOT EXISTS idx_employeesInformation_position_category ON "employeesInformation"("empPositionCategory")',
            'CREATE INDEX IF NOT EXISTS idx_employeesInformation_status ON "employeesInformation"("empStatus")',
            'CREATE INDEX IF NOT EXISTS idx_employeesInformation_department ON "employeesInformation"("empDepartment")',
            'CREATE INDEX IF NOT EXISTS idx_employeesInformation_nationality ON "employeesInformation"("empNationality")',
            'CREATE INDEX IF NOT EXISTS idx_employeesInformation_basic_salary ON "employeesInformation"("basicSalary")',
            'CREATE INDEX IF NOT EXISTS idx_employeesInformation_created_at ON "employeesInformation"(created_at)'
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
            WHERE table_name = 'employeesInformation'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        
        logger.info("Employee table created successfully!")
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
        
        # Create trigger for updated_at
        logger.info("Creating updated_at trigger...")
        trigger_sql = """
        CREATE OR REPLACE FUNCTION update_updated_at_column()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ language 'plpgsql';
        
        CREATE TRIGGER update_employeesInformation_updated_at 
            BEFORE UPDATE ON "employeesInformation" 
            FOR EACH ROW 
            EXECUTE FUNCTION update_updated_at_column();
        """
        
        cursor.execute(trigger_sql)
        conn.commit()
        logger.info("Updated_at trigger created successfully!")
        
        cursor.close()
        conn.close()
        
        logger.info("Employee table setup completed successfully!")
        
    except Exception as e:
        logger.error(f"Error creating employeesInformation table: {e}")
        raise

if __name__ == "__main__":
    create_employee_table()