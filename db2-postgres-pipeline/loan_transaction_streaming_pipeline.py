#!/usr/bin/env python3
"""
Loan Transaction Streaming Pipeline - Producer and Consumer run simultaneously
"""

import pika
import psycopg2
import json
import logging
import threading
import time
from dataclasses import dataclass, asdict
from contextlib import contextmanager
from typing import Optional

from config import Config
from db2_connection import DB2Connection

@dataclass
class LoanTransactionRecord:
    reportingDate: str
    loanNumber: str
    transactionDate: str
    loanTransactionType: str
    loanTransactionSubType: Optional[str]
    currency: str
    orgTransactionAmount: float
    usdTransactionAmount: Optional[float]
    tzsTransactionAmount: float

class LoanTransactionStreamingPipeline:
    def __init__(self, batch_size=1000):
        self.config = Config()
        self.db2_conn = DB2Connection()
        self.batch_size = batch_size
        
        # Threading control
        self.producer_finished = threading.Event()
        self.consumer_finished = threading.Event()
        self.stop_consumer = threading.Event()
        
        # Statistics
        self.total_produced = 0
        self.total_consumed = 0
        self.total_available = 0
        self.start_time = time.time()
        
        # Retry settings
        self.max_retries = 3
        self.retry_delay = 2
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        self.logger.info("Loan Transaction STREAMING Pipeline initialized")
        self.logger.info(f"Batch size: {self.batch_size} records per batch")
        self.logger.info("Mode: Streaming (Producer + Consumer simultaneously)")
    
    def get_loan_transaction_query(self, last_trn_date=None, last_trn_snum=None):
        """Get the loan transaction query with cursor-based pagination"""
        
        where_clause = """WHERE gl.EXTERNAL_GLACCOUNT IN (
            '110000001', '110000005', '110010001', '110010012',
            '110020001', '110020002', '110020003', '110020005',
            '110020007', '110020008', '110020009', '110020011',
            '110030001', '110030002', '110030003', '110030004',
            '120000001', '120000005', '120010001', '120020001',
            '120020002', '120010012', '120030002', '120050001',
            '120030003', '120030006', '120050005', '120020007',
            '130000005'
        )
        AND gte.PRF_ACCOUNT_NUMBER IS NOT NULL"""
        
        if last_trn_date and last_trn_snum:
            where_clause += f"""
            AND (gte.TRN_DATE > '{last_trn_date}' 
                 OR (gte.TRN_DATE = '{last_trn_date}' AND gte.TRN_SNUM > {last_trn_snum}))
            """
        
        query = f"""
        SELECT CURRENT_TIMESTAMP AS reportingDate,
               gte.PRF_ACCOUNT_NUMBER AS loanNumber,
               gte.TRN_DATE AS transactionDate,
               
               CASE
                   WHEN gte.JUSTIFIC_DESCR IN ('PAYMENT FROM DEPOSIT ACCOUNT', 'LOAN ACCOUNT PAYMENT')
                       THEN 'Installment payment'
                   WHEN gte.JUSTIFIC_DESCR IN ('LOAN DRAWDOWN WITH COMMISSION', 'LOAN DRAWDOWN WITH NO COMMISSION')
                       THEN 'Loan disbursement'
                   WHEN gte.JUSTIFIC_DESCR IN ('PRE.FULL PAYMENT OF LOAN(FX)', 'LOANS WRITE OFF GLSYNC', 
                                               'LOAN ACCOUNT CLOSING(FX)')
                       THEN 'Loan payoff'
                   WHEN gte.JUSTIFIC_DESCR IN ('LOAN ACC CLASSIFICATION SYNC WITH GL', 'JOURNAL CREDIT',
                                               'DR PRINCIPAL (CR REVERSAL) (JOURNAL) (1)', 'JOURNAL DEBIT', 
                                               'G/L CREDIT', 'G/L DEBIT')
                       THEN 'Reversal'
                   WHEN gte.JUSTIFIC_DESCR IN ('PRE.PAYMENT OF NEXT INSTALL(FX)', 'PRE.PARTIAL PAY OF LOAN(FX)')
                       THEN 'Prepayments'
                   WHEN gte.JUSTIFIC_DESCR = 'LOAN DRAWDOWN' THEN 'Loan disbursement'
                   WHEN gte.JUSTIFIC_DESCR = 'AMORTIZATION INSTALLMENT CREATION' THEN 'Interest Accruals'
                   WHEN gte.JUSTIFIC_DESCR = 'FX LOAN MULTI PAYMENTS' THEN 'Installment payment'
                   WHEN gte.JUSTIFIC_DESCR IN ('SHARING CREDIT BALANCE', 'FUND TRANSFERS (INTRA BANK)', 
                                               'DEPOSIT CASH', 'CR FROM MOBILE BANKING-MOB TO ACC')
                       THEN 'Deposit'
                   WHEN gte.JUSTIFIC_DESCR = 'MULTIPURPOSE JUSTIFICATION' THEN 'Loan administration fees'
                   WHEN gte.JUSTIFIC_DESCR = 'ARREARS CAPITALIZATION' THEN 'Interest capitalization'
                   WHEN gte.JUSTIFIC_DESCR = 'JD-TRANSFER TO ACCOUNT' THEN 'Withdrawal'
                   ELSE 'Installment payment'
               END AS loanTransactionType,
               
               CASE
                   WHEN gte.JUSTIFIC_DESCR = 'LOAN DRAWDOWN WITH COMMISSION' THEN 'New disbursement'
                   WHEN gte.JUSTIFIC_DESCR = 'PRE.FULL PAYMENT OF LOAN(FX)' THEN NULL
                   WHEN gte.JUSTIFIC_DESCR = 'LOAN RESTRUCTURING FX' THEN 'Restructuring'
                   WHEN gte.JUSTIFIC_DESCR = 'LOAN ENHANCEMENT FX' THEN 'Enhancement'
                   ELSE NULL
               END AS loanTransactionSubType,
               
               gte.CURRENCY_SHORT_DES AS currency,
               gte.DC_AMOUNT AS orgTransactionAmount,
               
               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN gte.DC_AMOUNT
                   ELSE NULL
               END AS usdTransactionAmount,
               
               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN gte.DC_AMOUNT * 2500.00
                   ELSE gte.DC_AMOUNT
               END AS tzsTransactionAmount,
               
               gte.TRN_DATE as cursor_trn_date,
               gte.TRN_SNUM as cursor_trn_snum
               
        FROM GLI_TRX_EXTRACT AS gte
        LEFT JOIN LOAN_ACCOUNT AS la ON la.CUST_ID = gte.CUST_ID
        LEFT JOIN GLG_ACCOUNT AS gl ON gl.ACCOUNT_ID = gte.FK_GLG_ACCOUNTACCO
        
        {where_clause}
        ORDER BY gte.TRN_DATE ASC, gte.TRN_SNUM ASC
        FETCH FIRST {self.batch_size} ROWS ONLY
        """
        
        return query
    
    def get_total_count_query(self):
        """Get total count of available loan transaction records"""
        return """
        SELECT COUNT(*) as total_count
        FROM GLI_TRX_EXTRACT AS gte
        LEFT JOIN GLG_ACCOUNT AS gl ON gl.ACCOUNT_ID = gte.FK_GLG_ACCOUNTACCO
        WHERE gl.EXTERNAL_GLACCOUNT IN (
            '110000001', '110000005', '110010001', '110010012',
            '110020001', '110020002', '110020003', '110020005',
            '110020007', '110020008', '110020009', '110020011',
            '110030001', '110030002', '110030003', '110030004',
            '120000001', '120000005', '120010001', '120020001',
            '120020002', '120010012', '120030002', '120050001',
            '120030003', '120030006', '120050005', '120020007',
            '130000005'
        )
        AND gte.PRF_ACCOUNT_NUMBER IS NOT NULL
        """
    
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
    
    def setup_rabbitmq_connection(self, max_retries=3):
        """Setup RabbitMQ connection with retry logic"""
        for attempt in range(max_retries):
            try:
                credentials = pika.PlainCredentials(
                    self.config.message_queue.rabbitmq_user,
                    self.config.message_queue.rabbitmq_password
                )
                parameters = pika.ConnectionParameters(
                    host=self.config.message_queue.rabbitmq_host,
                    port=self.config.message_queue.rabbitmq_port,
                    credentials=credentials,
                    heartbeat=600,
                    blocked_connection_timeout=300,
                )
                connection = pika.BlockingConnection(parameters)
                channel = connection.channel()
                return connection, channel
                
            except Exception as e:
                self.logger.warning(f"RabbitMQ connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    raise
    
    def setup_rabbitmq_queue(self):
        """Setup RabbitMQ queue for loan transactions"""
        try:
            connection, channel = self.setup_rabbitmq_connection()
            channel.queue_declare(queue='loan_transaction_queue', durable=True)
            connection.close()
            self.logger.info("RabbitMQ queue 'loan_transaction_queue' setup complete")
        except Exception as e:
            self.logger.error(f"Failed to setup RabbitMQ: {e}")
            raise
    
    def process_record(self, row):
        """Process a single record"""
        # Remove cursor fields (last two)
        row_data = row[:-2]
        
        return LoanTransactionRecord(
            reportingDate=str(row_data[0]) if row_data[0] else None,
            loanNumber=str(row_data[1]).strip() if row_data[1] else None,
            transactionDate=str(row_data[2]) if row_data[2] else None,
            loanTransactionType=str(row_data[3]) if row_data[3] else None,
            loanTransactionSubType=str(row_data[4]) if row_data[4] else None,
            currency=str(row_data[5]) if row_data[5] else None,
            orgTransactionAmount=float(row_data[6]) if row_data[6] else 0.0,
            usdTransactionAmount=float(row_data[7]) if row_data[7] else None,
            tzsTransactionAmount=float(row_data[8]) if row_data[8] else 0.0
        )
    
    def insert_to_postgres(self, record: LoanTransactionRecord, cursor):
        """Insert record to PostgreSQL"""
        insert_sql = """
        INSERT INTO "loanTransaction" (
            "reportingDate", "loanNumber", "transactionDate",
            "loanTransactionType", "loanTransactionSubType",
            currency, "orgTransactionAmount", "usdTransactionAmount",
            "tzsTransactionAmount"
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        """
        
        cursor.execute(insert_sql, (
            record.reportingDate,
            record.loanNumber,
            record.transactionDate,
            record.loanTransactionType,
            record.loanTransactionSubType,
            record.currency,
            record.orgTransactionAmount,
            record.usdTransactionAmount,
            record.tzsTransactionAmount
        ))

    
    def producer_thread(self):
        """Producer thread - reads from DB2 and sends to RabbitMQ"""
        try:
            self.logger.info("Producer thread started")
            
            # Get total count
            with self.db2_conn.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(self.get_total_count_query())
                self.total_available = cursor.fetchone()[0]
            
            self.logger.info(f"Total loan transaction records available: {self.total_available:,}")
            
            # Setup RabbitMQ
            connection, channel = self.setup_rabbitmq_connection()
            
            # Process batches
            batch_number = 1
            last_trn_date = None
            last_trn_snum = None
            last_progress_report = time.time()
            
            while True:
                batch_start_time = time.time()
                
                # Fetch batch
                rows = None
                for attempt in range(self.max_retries):
                    try:
                        with self.db2_conn.get_connection() as conn:
                            cursor = conn.cursor()
                            batch_query = self.get_loan_transaction_query(last_trn_date, last_trn_snum)
                            cursor.execute(batch_query)
                            rows = cursor.fetchall()
                        break
                    except Exception as e:
                        self.logger.warning(f"DB2 query attempt {attempt + 1} failed: {e}")
                        if attempt < self.max_retries - 1:
                            time.sleep(self.retry_delay)
                        else:
                            raise
                
                if not rows:
                    self.logger.info("No more records to process")
                    break
                
                # Process and publish
                batch_published = 0
                for row in rows:
                    last_trn_date = row[-2]
                    last_trn_snum = row[-1]
                    
                    record = self.process_record(row)
                    message = json.dumps(asdict(record), default=str)
                    
                    # Publish
                    published = False
                    for attempt in range(self.max_retries):
                        try:
                            channel.basic_publish(
                                exchange='',
                                routing_key='loan_transaction_queue',
                                body=message,
                                properties=pika.BasicProperties(delivery_mode=2)
                            )
                            published = True
                            break
                        except Exception as e:
                            self.logger.warning(f"RabbitMQ publish attempt {attempt + 1} failed: {e}")
                            if attempt < self.max_retries - 1:
                                try:
                                    connection.close()
                                except:
                                    pass
                                connection, channel = self.setup_rabbitmq_connection()
                                time.sleep(self.retry_delay)
                    
                    if published:
                        batch_published += 1
                        self.total_produced += 1
                
                batch_time = time.time() - batch_start_time
                progress_percent = self.total_produced / self.total_available * 100 if self.total_available > 0 else 0
                
                self.logger.info(f"Producer: Batch {batch_number:,} - {len(rows)} records, {batch_published} published ({progress_percent:.2f}% complete, {batch_time:.1f}s)")
                
                # Progress report every 5 minutes
                current_time = time.time()
                if current_time - last_progress_report >= 300:
                    elapsed_time = current_time - self.start_time
                    rate = self.total_produced / elapsed_time if elapsed_time > 0 else 0
                    remaining_records = self.total_available - self.total_produced
                    eta_seconds = remaining_records / rate if rate > 0 else 0
                    eta_hours = eta_seconds / 3600
                    
                    self.logger.info(f"PROGRESS REPORT: {self.total_produced:,}/{self.total_available:,} records ({progress_percent:.1f}%) - Rate: {rate:.1f} rec/sec - ETA: {eta_hours:.1f} hours")
                    last_progress_report = current_time
                
                batch_number += 1
                time.sleep(0.1)
            
            connection.close()
            self.logger.info(f"Producer finished: {self.total_produced:,} records published")
            self.producer_finished.set()
            
        except Exception as e:
            self.logger.error(f"Producer error: {e}")
            self.producer_finished.set()
    
    def consumer_thread(self):
        """Consumer thread - processes messages from queue"""
        try:
            self.logger.info("Consumer thread started")
            
            connection, channel = self.setup_rabbitmq_connection()
            last_progress_report = time.time()
            
            def process_message(ch, method, properties, body):
                nonlocal last_progress_report
                try:
                    record_data = json.loads(body)
                    record = LoanTransactionRecord(**record_data)
                    
                    # Insert to PostgreSQL
                    inserted = False
                    for attempt in range(self.max_retries):
                        try:
                            with self.get_postgres_connection() as conn:
                                cursor = conn.cursor()
                                self.insert_to_postgres(record, cursor)
                                conn.commit()
                            inserted = True
                            break
                        except Exception as e:
                            self.logger.warning(f"PostgreSQL insert attempt {attempt + 1} failed: {e}")
                            if attempt < self.max_retries - 1:
                                time.sleep(self.retry_delay)
                    
                    if inserted:
                        self.total_consumed += 1
                        
                        if self.total_consumed % (self.batch_size * 2) == 0:
                            elapsed_time = time.time() - self.start_time
                            rate = self.total_consumed / elapsed_time if elapsed_time > 0 else 0
                            progress_percent = (self.total_consumed / self.total_available * 100) if self.total_available > 0 else 0
                            self.logger.info(f"Consumer: Processed {self.total_consumed:,} records ({progress_percent:.2f}%) - Rate: {rate:.1f} rec/sec")
                        
                        # Detailed progress report every 5 minutes
                        current_time = time.time()
                        if current_time - last_progress_report >= 300:
                            elapsed_time = current_time - self.start_time
                            rate = self.total_consumed / elapsed_time if elapsed_time > 0 else 0
                            remaining_records = self.total_available - self.total_consumed if self.total_available > 0 else 0
                            eta_seconds = remaining_records / rate if rate > 0 else 0
                            eta_hours = eta_seconds / 3600
                            
                            self.logger.info(f"CONSUMER PROGRESS: {self.total_consumed:,}/{self.total_available:,} records - Rate: {rate:.1f} rec/sec - ETA: {eta_hours:.1f} hours")
                            last_progress_report = current_time
                    
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    
                except Exception as e:
                    self.logger.error(f"Consumer error processing message: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            
            channel.basic_qos(prefetch_count=10)
            channel.basic_consume(queue='loan_transaction_queue', on_message_callback=process_message)
            
            while not self.stop_consumer.is_set():
                try:
                    connection.process_data_events(time_limit=1)
                    
                    if self.producer_finished.is_set():
                        method = channel.queue_declare(queue='loan_transaction_queue', durable=True, passive=True)
                        if method.method.message_count == 0:
                            self.logger.info("Consumer: Queue empty, producer finished")
                            break
                        
                except Exception as e:
                    self.logger.error(f"Consumer processing error: {e}")
                    try:
                        connection.close()
                    except:
                        pass
                    connection, channel = self.setup_rabbitmq_connection()
                    channel.basic_qos(prefetch_count=10)
                    channel.basic_consume(queue='loan_transaction_queue', on_message_callback=process_message)
            
            connection.close()
            self.logger.info(f"Consumer finished: {self.total_consumed:,} records processed")
            self.consumer_finished.set()
            
        except Exception as e:
            self.logger.error(f"Consumer error: {e}")
            self.consumer_finished.set()
    
    def run_streaming_pipeline(self):
        """Run the streaming pipeline"""
        self.logger.info("Starting Loan Transaction STREAMING pipeline...")
        
        try:
            self.setup_rabbitmq_queue()
            
            # Start consumer first
            consumer_thread = threading.Thread(target=self.consumer_thread, name="Consumer")
            consumer_thread.start()
            time.sleep(1)
            
            # Start producer
            producer_thread = threading.Thread(target=self.producer_thread, name="Producer")
            producer_thread.start()
            
            # Wait for completion
            producer_thread.join()
            self.logger.info("Producer thread completed")
            
            consumer_thread.join(timeout=60)
            
            if consumer_thread.is_alive():
                self.stop_consumer.set()
                consumer_thread.join(timeout=30)
            
            # Final statistics
            total_time = time.time() - self.start_time
            avg_rate = self.total_consumed / total_time if total_time > 0 else 0
            success_rate = (self.total_consumed / self.total_produced * 100) if self.total_produced > 0 else 0
            
            self.logger.info(f"""
            ==========================================
            Loan Transaction Pipeline Summary:
            ==========================================
            Total available records: {self.total_available:,}
            Records produced: {self.total_produced:,}
            Records consumed: {self.total_consumed:,}
            Success rate: {success_rate:.1f}%
            Total processing time: {total_time/3600:.2f} hours
            Average rate: {avg_rate:.1f} records/second
            ==========================================
            """)
            
        except Exception as e:
            self.logger.error(f"Pipeline error: {e}")
            raise

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Loan Transaction Streaming Pipeline')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for processing')
    
    args = parser.parse_args()
    
    pipeline = LoanTransactionStreamingPipeline(batch_size=args.batch_size)
    
    try:
        pipeline.run_streaming_pipeline()
    except KeyboardInterrupt:
        pipeline.logger.info("Pipeline stopped by user")
    except Exception as e:
        pipeline.logger.error(f"Pipeline failed: {e}")
        import sys
        sys.exit(1)

if __name__ == "__main__":
    main()
