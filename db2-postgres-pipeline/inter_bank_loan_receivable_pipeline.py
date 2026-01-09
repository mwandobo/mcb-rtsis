#!/usr/bin/env python3
"""
Inter-Bank Loan Receivable Pipeline - BOT Project
Direct pipeline without Redis/RabbitMQ dependencies
"""

import psycopg2
from db2_connection import DB2Connection
import logging
from datetime import datetime
from contextlib import contextmanager

from config import Config
from processors.inter_bank_loan_receivable_processor import InterBankLoanReceivableProcessor

class InterBankLoanReceivablePipeline:
    def __init__(self, limit=None, start_date=None, resume=False):
        """
        Inter-Bank Loan Receivable Pipeline
        
        Args:
            limit (int): Number of records to fetch per batch (uses config if not specified)
            start_date (str): Starting date for transactions (YYYY-MM-DD format)
            resume (bool): Whether to resume from last processed record instead of clearing data
        """
        self.config = Config()
        self.db2_conn = DB2Connection()
        
        # Get inter-bank loan receivable table config
        self.table_config = self.config.tables.get('interBankLoanReceivable')
        if not self.table_config:
            raise ValueError("Inter-bank loan receivable table config not found")
        
        # Use provided limit or config batch size
        self.limit = limit or self.table_config.batch_size
        self.start_date = start_date or '2024-01-01'  # Default to 2024 data
        self.resume = resume
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Initialize processor
        self.inter_bank_loan_receivable_processor = InterBankLoanReceivableProcessor()
        
        self.logger.info(f"Inter-Bank Loan Receivable Pipeline initialized")
        self.logger.info(f"Batch limit: {self.limit}")
        self.logger.info(f"Start date: {self.start_date}")
        self.logger.info(f"Resume mode: {self.resume}")
        
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
    
    def get_existing_count(self):
        """Get count of existing records in PostgreSQL"""
        try:
            with self.get_postgres_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f'SELECT COUNT(*) FROM "{self.table_config.target_table}";')
                return cursor.fetchone()[0]
        except Exception as e:
            self.logger.error(f"Failed to get existing count: {e}")
            return 0
    
    def get_total_count(self):
        """Get total count of inter-bank loan receivable records"""
        try:
            with self.get_db2_connection() as conn:
                cursor = conn.cursor()
                
                count_query = """
                SELECT COUNT(*) 
                FROM W_EOM_LOAN_ACCOUNT as wela
                LEFT JOIN CUSTOMER as c ON wela.CUST_ID = c.CUST_ID
                LEFT JOIN PROFITS_ACCOUNT pa ON pa.CUST_ID = wela.CUST_ID
                LEFT JOIN other_id id ON (CASE WHEN (id.serial_no IS NULL) THEN '1' ELSE id.main_flag END = '1' AND id.fk_customercust_id = c.cust_id)
                LEFT JOIN LNS_CRD_CUST_DATA lccd on lccd.CUST_ID = wela.CUST_ID
                LEFT JOIN generic_detail id_country ON (id.fkgh_has_been_issu = id_country.fk_generic_headpar AND id.fkgd_has_been_issu = id_country.serial_num)
                LEFT JOIN CUSTOMER_TYPES_LOOKUP ctl ON ctl.CUSTOMER_TYPE_CODE = c.CUST_TYPE
                LEFT JOIN COUNTRIES_LOOKUP cl ON cl.COUNTRY_NAME = id_country.description
                LEFT JOIN GENERIC_DETAIL GG ON GG.FK_GENERIC_HEADPAR = wela.FKGH_HAS_AS_LOAN_P AND GG.SERIAL_NUM = wela.FKGD_HAS_AS_LOAN_P
                LEFT JOIN LOAN_ACCOUNT L ON wela.FK_UNITCODE = L.FK_UNITCODE AND wela.ACC_TYPE = L.ACC_TYPE AND wela.ACC_SN = L.ACC_SN
                LEFT JOIN LOAN_ADD_INFO N ON N.ROW_ID = 1 AND wela.FK_UNITCODE = N.ACC_UNIT AND wela.ACC_TYPE = N.ACC_TYPE AND wela.ACC_SN = N.ACC_SN
                """
                
                if self.start_date:
                    count_query += f" WHERE wela.ACC_OPEN_DT >= DATE('{self.start_date}')"
                
                cursor.execute(count_query)
                total = cursor.fetchone()[0]
                return total
                
        except Exception as e:
            self.logger.error(f"Failed to get total count: {e}")
            return 0

    def run_complete_pipeline(self):
        """Run the complete inter-bank loan receivable pipeline"""
        self.logger.info("Starting Complete Inter-Bank Loan Receivable Pipeline")
        self.logger.info("=" * 60)
        
        try:
            # Step 1: Check existing data
            existing_count = self.get_existing_count()
            self.logger.info(f"Existing records in PostgreSQL: {existing_count}")
            
            # Step 2: Get total count
            self.logger.info(f"Getting total inter-bank loan receivable records from {self.start_date}...")
            total_records = self.get_total_count()
            self.logger.info(f"Total inter-bank loan receivable records available: {total_records}")
            
            if total_records == 0:
                self.logger.info("No inter-bank loan receivable records found")
                return
            
            # Step 3: Process using the SQL query from config
            self.logger.info("Executing inter-bank loan receivable query...")
            
            with self.get_db2_connection() as conn:
                cursor = conn.cursor()
                
                # Use the query from config with date filter
                query = self.table_config.query
                if self.start_date:
                    # Add date filter to the existing query
                    query = query.replace(
                        "ORDER BY wela.ACC_OPEN_DT DESC",
                        f"WHERE wela.ACC_OPEN_DT >= DATE('{self.start_date}') ORDER BY wela.ACC_OPEN_DT DESC"
                    )
                
                cursor.execute(query)
                rows = cursor.fetchall()
                
                self.logger.info(f"Fetched {len(rows)} inter-bank loan receivable records")
                
                if not rows:
                    self.logger.info("No records found matching criteria")
                    return
                
                # Show sample data
                self.logger.info("Sample inter-bank loan receivable records:")
                for i, row in enumerate(rows[:min(3, len(rows))], 1):
                    self.logger.info(f"  {i}. Customer: {row[1]} | Client: {row[3]} | Loan: {row[16]} | Amount: {row[34]} {row[32]}")
            
            # Step 4: Process records
            processed_count = 0
            skipped_count = 0
            
            with self.get_postgres_connection() as conn:
                pg_cursor = conn.cursor()
                
                for i, row in enumerate(rows, 1):
                    try:
                        # Process the record using the processor
                        record = self.inter_bank_loan_receivable_processor.process_record(row, self.table_config.name)
                        
                        if self.inter_bank_loan_receivable_processor.validate_record(record):
                            # Use regular insert to keep all loan records
                            self.inter_bank_loan_receivable_processor.insert_to_postgres(record, pg_cursor)
                            processed_count += 1
                            
                            if processed_count % 100 == 0:
                                self.logger.info(f"Processed {processed_count}/{len(rows)} records...")
                        else:
                            self.logger.warning(f"Invalid inter-bank loan receivable record skipped: {record.customer_identification_number}")
                            skipped_count += 1
                            
                    except Exception as e:
                        self.logger.error(f"Error processing record {i}: {e}")
                        skipped_count += 1
                        continue
                
                # Commit all changes
                try:
                    conn.commit()
                    self.logger.info(f"All records committed successfully")
                except Exception as e:
                    self.logger.error(f"Failed to commit records: {e}")
                    conn.rollback()
                    return
            
            # Step 5: Final verification
            self.logger.info(f"INTER-BANK LOAN RECEIVABLE PIPELINE FINISHED!")
            self.logger.info(f"Total records processed: {processed_count}")
            self.logger.info(f"Records skipped: {skipped_count}")
            
            # Verify final count in PostgreSQL
            with self.get_postgres_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f'SELECT COUNT(*) FROM "{self.table_config.target_table}";')
                final_count = cursor.fetchone()[0]
                self.logger.info(f"Final count in PostgreSQL: {final_count}")
                
                # Show sample of final data
                cursor.execute(f"""
                    SELECT "customerIdentificationNumber", "clientName", "loanNumber", "currency", "orgOutstandingPrincipalAmount", "contractDate"
                    FROM "{self.table_config.target_table}" 
                    ORDER BY "contractDate" DESC
                    LIMIT 5;
                """)
                
                sample_records = cursor.fetchall()
                self.logger.info("Sample of processed records:")
                for record in sample_records:
                    customer_id, client_name, loan_number, currency, outstanding, contract_date = record
                    self.logger.info(f"  {customer_id} | {client_name} | {loan_number} | {outstanding} {currency} | {contract_date}")
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            import traceback
            traceback.print_exc()

def main():
    """Main function"""
    
    print("Complete Inter-Bank Loan Receivable Pipeline - BOT Project")
    print("=" * 60)
    
    # Enable resume mode by default for inter-bank loan receivable processing - starting from 2024
    pipeline = InterBankLoanReceivablePipeline(start_date='2024-01-01', resume=True)
    pipeline.run_complete_pipeline()

if __name__ == "__main__":
    main()