#!/usr/bin/env python3
"""
Create Employee Information Table - BOT Project
"""

import psycopg2
import logging
from config import Config

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_employee_table():
    """Create employee information table in PostgreSQL"""
    
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
        
        # Drop existing table if it exists
        logger.info("🗑️ Dropping existing employeeInformation table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS "employeeInformation" CASCADE;')
        
        # Create employeeInformation table
        logger.info("🏗️ Creating employeeInformation table...")
        create_table_query = """
        CREATE TABLE "employeeInformation" (
            id SERIAL PRIMARY KEY,
            "reportingDate" VARCHAR(20),
            "branchCode" VARCHAR(20),
            "empName" VARCHAR(200) NOT NULL,
            "gender" VARCHAR(20),
            "empDob" VARCHAR(20),
            "empIdentificationType" VARCHAR(50),
            "empIdentificationNumber" VARCHAR(50),
            "empPosition" VARCHAR(200),
            "empPositionCategory" VARCHAR(100),
            "empStatus" VARCHAR(100),
            "empDepartment" VARCHAR(200),
            "appointmentDate" VARCHAR(20),
            "empNationality" VARCHAR(100),
            "lastPromotionDate" VARCHAR(20),
            "basicSalary" VARCHAR(50),
            "empBenefits" VARCHAR(500),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE("empName", "empIdentificationNumber")
        );
        """
        
        cursor.execute(create_table_query)
        
        # Create indexes
        logger.info("📊 Creating indexes...")
        
        indexes = [
            'CREATE INDEX idx_employee_emp_name ON "employeeInformation"("empName");',
            'CREATE INDEX idx_employee_branch_code ON "employeeInformation"("branchCode");',
            'CREATE INDEX idx_employee_emp_position ON "employeeInformation"("empPosition");',
            'CREATE INDEX idx_employee_emp_department ON "employeeInformation"("empDepartment");',
            'CREATE INDEX idx_employee_emp_identification ON "employeeInformation"("empIdentificationNumber");',
            'CREATE INDEX idx_employee_appointment_date ON "employeeInformation"("appointmentDate");'
        ]
        
        for index_query in indexes:
            cursor.execute(index_query)
        
        # Create trigger for updated_at
        trigger_query = """
        CREATE OR REPLACE FUNCTION update_updated_at_column()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ language 'plpgsql';
        
        CREATE TRIGGER update_employee_updated_at 
            BEFORE UPDATE ON "employeeInformation" 
            FOR EACH ROW 
            EXECUTE FUNCTION update_updated_at_column();
        """
        
        cursor.execute(trigger_query)
        
        # Commit changes
        conn.commit()
        
        logger.info("✅ Employee table and indexes created successfully!")
        
        # Show table structure
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'employeeInformation' 
            ORDER BY ordinal_position;
        """)
        
        columns = cursor.fetchall()
        logger.info("📋 Table structure:")
        for column_name, data_type in columns:
            logger.info(f"  {column_name}: {data_type}")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error creating employee table: {e}")
        return False

def main():
    """Main function"""
    print("🏗️ Creating Employee Information Table")
    print("=" * 50)
    
    success = create_employee_table()
    
    if success:
        print("✅ Employee table created successfully!")
    else:
        print("❌ Failed to create employee table!")

if __name__ == "__main__":
    main()