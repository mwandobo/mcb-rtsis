#!/usr/bin/env python3
"""
Employee Information Pipeline - BOT Project
Direct pipeline without Redis/RabbitMQ dependencies
"""

import psycopg2
from db2_connection import DB2Connection
import logging
from datetime import datetime
from contextlib import contextmanager

from config import Config
from processors.employee_processor import EmployeeProcessor

class EmployeePipeline:
    def __init__(self, limit=None):
        """
        Employee Pipeline
        
        Args:
            limit (int): Number of records to fetch per batch (uses config if not specified)
        """
        self.config = Config()
        self.db2_conn = DB2Connection()
        
        # Read the Employee query from employee.sql file
        import os
        sql_file_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'sqls', 'employee.sql')
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            self.base_employee_query = f.read()
        
        # Use provided limit or default batch size
        self.limit = limit or 500
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Initialize processor
        self.employee_processor = EmployeeProcessor()
        
        self.logger.info(f"👨‍💼 Employee Pipeline initialized")
        self.logger.info(f"📊 Batch limit: {self.limit}")
        self.logger.info(f"📄 Using employee.sql query")
        
    @contextmanager
    def get_db2_connection(self):
        """Get DB2 connection"""
        with self.db2_conn.get_connection() as conn:
            yield conn
            
    @contextmanager
    def get_postgres_connection(self):
        """Get PostgreSQL connection"""
        conn = None
        try:
            conn = psycopg2.connect(
                host=self.config.database.pg_host,
                port=self.config.database.pg_port,
                database=self.config.database.pg_database,
                user=self.config.database.pg_user,
                password=self.config.database.pg_password
            )
            yield conn
        except Exception as e:
            self.logger.error(f"PostgreSQL connection error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def clear_existing_data(self):
        """Clear existing Employee data from PostgreSQL"""
        try:
            with self.get_postgres_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('DELETE FROM "employeeInformation";')
                conn.commit()
                self.logger.info("🗑️ Cleared existing Employee data from PostgreSQL")
        except Exception as e:
            self.logger.error(f"❌ Failed to clear existing data: {e}")
            raise
    
    def get_total_count(self):
        """Get total count of Employee records"""
        try:
            with self.get_db2_connection() as conn:
                cursor = conn.cursor()
                
                # Count query based on the WHERE conditions in the SQL file
                count_query = """
                SELECT COUNT(*) 
                FROM BANKEMPLOYEE be
                LEFT JOIN (SELECT *
                          FROM (SELECT c.*,
                                       ROW_NUMBER() OVER (
                                           PARTITION BY
                                               UPPER(TRIM(c.FIRST_NAME)),
                                               UPPER(TRIM(c.SURNAME))
                                           ORDER BY c.CUST_ID
                                           ) AS rn
                                FROM CUSTOMER c) x
                          WHERE rn = 1) c
                         ON UPPER(TRIM(c.FIRST_NAME)) = UPPER(TRIM(be.FIRST_NAME))
                             AND UPPER(TRIM(c.SURNAME)) = UPPER(TRIM(be.LAST_NAME))
                WHERE STAFF_NO IS NOT NULL
                  AND STAFF_NO = TRIM(STAFF_NO)
                  AND EMPL_STATUS = 1
                  AND STAFF_NO LIKE 'EIC%'
                """
                
                cursor.execute(count_query)
                total = cursor.fetchone()[0]
                return total
                
        except Exception as e:
            self.logger.error(f"❌ Failed to get total count: {e}")
            return 0
    
    def run_complete_pipeline(self):
        """Run the complete Employee pipeline"""
        self.logger.info("🚀 Starting Complete Employee Pipeline")
        self.logger.info("=" * 60)
        
        try:
            # Step 1: Clear existing data
            self.logger.info("🗑️ Clearing existing Employee data...")
            self.clear_existing_data()
            
            # Step 2: Get total count
            self.logger.info("📊 Getting total Employee records...")
            total_records = self.get_total_count()
            self.logger.info(f"📊 Total Employee records available: {total_records}")
            
            if total_records == 0:
                self.logger.info("ℹ️ No Employee records found")
                return
            
            # Step 3: Fetch all records at once (employee.sql)
            self.logger.info("📊 Fetching all Employee records...")
            
            with self.get_db2_connection() as conn:
                cursor = conn.cursor()
                
                # Use the base query to get all records
                cursor.execute(self.base_employee_query)
                all_rows = cursor.fetchall()
                
                self.logger.info(f"👨‍💼 Fetched {len(all_rows)} employee records from employee.sql")
            
            # Show sample data
            self.logger.info("📋 Sample Employee records:")
            for i, row in enumerate(all_rows[:min(3, len(all_rows))], 1):
                self.logger.info(f"  {i}. Employee: {row[2]} | Branch: {row[1]} | Position: {row[7]} | Department: {row[10]}")
            
            # Step 4: Process records in batches
            total_processed = 0
            batch_size = 500  # Process in smaller batches for better performance
            
            for batch_start in range(0, len(all_rows), batch_size):
                batch_end = min(batch_start + batch_size, len(all_rows))
                batch_records = all_rows[batch_start:batch_end]
                batch_number = (batch_start // batch_size) + 1
                
                self.logger.info(f"\n📊 Processing batch {batch_number}: records {batch_start + 1} to {batch_end}")
                
                batch_processed = 0
                batch_skipped = 0
                
                with self.get_postgres_connection() as conn:
                    pg_cursor = conn.cursor()
                    
                    try:
                        for row in batch_records:
                            try:
                                # Process the record using the processor
                                record = self.employee_processor.process_record(row, 'employeeInformation')
                                
                                if self.employee_processor.validate_record(record):
                                    self.employee_processor.insert_to_postgres(record, pg_cursor)
                                    batch_processed += 1
                                    
                                    if batch_processed % 100 == 0:
                                        self.logger.info(f"✅ Processed {batch_processed} records in batch {batch_number}...")
                                else:
                                    self.logger.warning(f"⚠️ Invalid Employee record skipped: {record.emp_name}")
                                    batch_skipped += 1
                                    
                            except Exception as e:
                                self.logger.error(f"❌ Error processing Employee record: {e}")
                                batch_skipped += 1
                                continue
                        
                        # Commit the entire batch at once
                        conn.commit()
                        self.logger.info(f"✅ Batch {batch_number} committed successfully")
                        
                    except Exception as e:
                        self.logger.error(f"❌ Batch {batch_number} failed, rolling back: {e}")
                        conn.rollback()
                        batch_processed = 0
                
                total_processed += batch_processed
                self.logger.info(f"✅ Batch {batch_number} completed: {batch_processed} new records, {batch_skipped} skipped")
                self.logger.info(f"📊 Total processed so far: {total_processed}")
            
            # Step 5: Final verification
            self.logger.info(f"\n🎉 COMPLETE EMPLOYEE PIPELINE FINISHED!")
            self.logger.info(f"📊 Total records processed: {total_processed}")
            self.logger.info(f"📊 Expected Employee records: {total_records}")
            
            # Verify final count in PostgreSQL
            with self.get_postgres_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT COUNT(*) FROM "employeeInformation";')
                final_count = cursor.fetchone()[0]
                self.logger.info(f"📊 Final count in PostgreSQL: {final_count}")
                
                # Show sample of final data
                cursor.execute("""
                    SELECT "empName", "branchCode", "empPosition", "empDepartment", "basicSalary"
                    FROM "employeeInformation" 
                    ORDER BY "empName" ASC
                    LIMIT 5;
                """)
                
                sample_records = cursor.fetchall()
                self.logger.info("📋 Sample of processed records:")
                for record in sample_records:
                    emp_name, branch_code, emp_position, emp_department, basic_salary = record
                    self.logger.info(f"  👨‍💼 Employee: {emp_name} | Branch: {branch_code} | Position: {emp_position} | Dept: {emp_department} | Salary: {basic_salary}")
            
        except Exception as e:
            self.logger.error(f"❌ Pipeline failed: {e}")
            import traceback
            traceback.print_exc()

def main():
    """Main function"""
    
    print("👨‍💼 Complete Employee Pipeline - BOT Project")
    print("=" * 60)
    
    pipeline = EmployeePipeline()
    pipeline.run_complete_pipeline()

if __name__ == "__main__":
    main()