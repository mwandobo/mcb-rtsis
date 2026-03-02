#!/usr/bin/env python3
"""
Inter-Bank Loan Receivable Streaming Pipeline - Producer and Consumer run simultaneously
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
from decimal import Decimal

from config import Config
from db2_connection import DB2Connection

@dataclass
class InterBankLoanReceivableRecord:
    reportingDate: str
    borrowersInstitutionCode: str
    borrowerCountry: Optional[str]
    relationshipType: str
    ratingStatus: int
    externalRatingCorrespondentBorrower: str
    gradesUnratedBorrower: str
    loanNumber: str
    loanType: str
    issueDate: str
    loanMaturityDate: Optional[str]
    currency: str
    orgLoanAmount: Optional[Decimal]
    usdLoanAmount: Optional[Decimal]
    tzsLoanAmount: Optional[Decimal]
    interestRate: Optional[Decimal]
    orgAccruedInterestAmount: Optional[Decimal]
    usdAccruedInterestAmount: Optional[Decimal]
    tzsAccruedInterestAmount: Optional[Decimal]
    orgSuspendedInterest: Optional[Decimal]
    usdSuspendedInterest: Optional[Decimal]
    tzsSuspendedInterest: Optional[Decimal]
    pastDueDays: int
    allowanceProbableLoss: int
    botProvision: int
    assetClassificationCategory: str


class InterBankLoanReceivableStreamingPipeline:
    def __init__(self, batch_size=500):
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
        
        self.logger.info("Inter-Bank Loan Receivable STREAMING Pipeline initialized")
        self.logger.info(f"Batch size: {self.batch_size} records per batch")
        self.logger.info("Mode: Streaming (Producer + Consumer simultaneously)")
    
    def get_query(self, last_loan_number=None):
        """Get the inter-bank loan receivable query with cursor-based pagination"""
        
        where_clause = "WHERE gte.FK_GLG_ACCOUNTACCO IN ('7.0.5.19.0001', '7.0.5.19.0002', '7.0.5.19.0003')"
        
        if last_loan_number:
            where_clause += f" AND la.ACC_SN > '{last_loan_number}'"
        
        query = f"""
        select CURRENT_TIMESTAMP                                                       AS reportingDate,
               LTRIM(RTRIM(c.CUST_ID))                                                 AS borrowersInstitutionCode,
               CASE
                   WHEN cl.COUNTRY_CODE = 'TZ' THEN 'TANZANIA, UNITED REPUBLIC OF' END as borrowerCountry,
               'Domestic banks unrelated'                                              as relationshipType,
               CAST(0 AS SMALLINT)                                                     as ratingStatus,
               CASE
                   WHEN la.ACC_EXP_DT IS NULL THEN 'UNRATED'
                   WHEN DAYS(la.ACC_EXP_DT) - DAYS(CURRENT_DATE) <= 90
                       THEN 'SHORT_TERM_UNRATED'
                   ELSE 'UNRATED'
                   END                                                                 AS externalRatingCorrespondentBorrower,
               'Grade B'                                                               AS gradesUnratedBorrower,
               la.ACC_SN                                                               AS loanNumber,
               'Interbank call loans in Tanzania'                                      AS loanType,
               la.ACC_OPEN_DT                                                          as issueDate,
               la.ACC_EXP_DT                                                           AS loanMaturityDate,
               gte.CURRENCY_SHORT_DES                                                  as currency,
               la.ACC_LIMIT_AMN                                                        AS orgLoanAmount,
               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'USD'
                       THEN DECIMAL(gte.DC_AMOUNT, 18, 2)
                   WHEN gte.CURRENCY_SHORT_DES <> 'USD'
                       THEN DECIMAL(la.ACC_LIMIT_AMN / fx.rate, 18, 2)
                   END                                                                 AS usdLoanAmount,
               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'USD'
                       THEN DECIMAL(la.ACC_LIMIT_AMN * fx.rate, 18, 2)
                   ELSE DECIMAL(la.ACC_LIMIT_AMN, 18, 2)
                   END                                                                 AS tzsLoanAmount,
               la.INTER_RATE_SPRD                                                      AS interestRate,
               la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL      AS orgAccruedInterestAmount,
               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'USD'
                       THEN DECIMAL(gte.DC_AMOUNT, 18, 2)
                   WHEN gte.CURRENCY_SHORT_DES <> 'USD'
                       THEN DECIMAL(la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL / fx.rate, 18, 2)
                   END                                                                 AS usdAccruedInterestAmount,
               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'USD'
                       THEN DECIMAL(la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL * fx.rate, 18, 2)
                   ELSE DECIMAL(la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL, 18, 2)
                   END                                                                 AS tzsAccruedInterestAmount,
               (la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL)                         AS orgSuspendedInterest,
               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'USD'
                       THEN DECIMAL(gte.DC_AMOUNT, 18, 2)
                   WHEN gte.CURRENCY_SHORT_DES <> 'USD'
                       THEN DECIMAL((la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL) / fx.rate, 18, 2)
                   END                                                                 AS usdSuspendedInterest,
               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'USD'
                       THEN DECIMAL((la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL) * fx.rate, 18, 2)
                   ELSE DECIMAL((la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL), 18, 2)
                   END                                                                 AS tzsSuspendedInterest,
               CASE
                   WHEN la.ACC_STATUS = '1'
                       AND la.OV_EXP_DT IS NOT NULL
                       AND CURRENT_DATE > la.OV_EXP_DT
                       THEN DAYS(CURRENT_DATE) - DAYS(la.OV_EXP_DT)
                   ELSE 0
                   END                                                                 AS pastDueDays,
               0                                                                       AS allowanceProbableLoss,
               0                                                                       AS botProvision,
               'Current'                                                               AS assetClassificationCategory,
               la.ACC_SN                                                               AS cursor_loan_number
        from GLI_TRX_EXTRACT as gte
                 INNER JOIN (SELECT la.*,
                                    ROW_NUMBER() OVER (
                                        PARTITION BY la.CUST_ID, la.FK_LOANFK_PRODUCTI
                                        ORDER BY la.TMSTAMP DESC, la.ACC_OPEN_DT DESC
                                        ) AS rn
                             FROM LOAN_ACCOUNT la) la
                            ON gte.ID_PRODUCT = la.FK_LOANFK_PRODUCTI
                                AND la.CUST_ID = gte.CUST_ID
                                AND la.rn = 1
                 LEFT JOIN CUSTOMER as c ON la.CUST_ID = c.CUST_ID
                 LEFT JOIN other_id id ON (CASE WHEN (id.serial_no IS NULL) THEN '1' ELSE id.main_flag END = '1' AND
                                           id.fk_customercust_id = c.cust_id)
                 LEFT JOIN generic_detail id_country ON (id.fkgh_has_been_issu = id_country.fk_generic_headpar AND
                                                         id.fkgd_has_been_issu = id_country.serial_num)
                 LEFT JOIN COUNTRIES_LOOKUP cl ON cl.COUNTRY_NAME = id_country.description
                 LEFT JOIN CURRENCY curr ON curr.SHORT_DESCR = gte.CURRENCY_SHORT_DES
                 LEFT JOIN (SELECT fr.fk_currencyid_curr,
                                   fr.rate
                            FROM fixing_rate fr
                            WHERE (fr.fk_currencyid_curr, fr.activation_date, fr.activation_time) IN
                                  (SELECT fk_currencyid_curr,
                                          activation_date,
                                          MAX(activation_time)
                                   FROM fixing_rate
                                   WHERE activation_date = (SELECT MAX(b.activation_date)
                                                            FROM fixing_rate b
                                                            WHERE b.activation_date <= CURRENT_DATE)
                                   GROUP BY fk_currencyid_curr, activation_date)) fx
                           ON fx.fk_currencyid_curr = curr.ID_CURRENCY
        {where_clause}
        ORDER BY la.ACC_SN ASC
        FETCH FIRST {self.batch_size} ROWS ONLY
        """
        
        return query

    def get_total_count_query(self):
        """Get total count of available records"""
        return """
        SELECT COUNT(*) as total_count
        from GLI_TRX_EXTRACT as gte
                 INNER JOIN (SELECT la.*,
                                    ROW_NUMBER() OVER (
                                        PARTITION BY la.CUST_ID, la.FK_LOANFK_PRODUCTI
                                        ORDER BY la.TMSTAMP DESC, la.ACC_OPEN_DT DESC
                                        ) AS rn
                             FROM LOAN_ACCOUNT la) la
                            ON gte.ID_PRODUCT = la.FK_LOANFK_PRODUCTI
                                AND la.CUST_ID = gte.CUST_ID
                                AND la.rn = 1
        WHERE gte.FK_GLG_ACCOUNTACCO IN ('7.0.5.19.0001', '7.0.5.19.0002', '7.0.5.19.0003')
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
        """Setup RabbitMQ queue"""
        try:
            connection, channel = self.setup_rabbitmq_connection()
            channel.queue_declare(queue='inter_bank_loan_receivable_queue', durable=True)
            connection.close()
            self.logger.info("RabbitMQ queue 'inter_bank_loan_receivable_queue' setup complete")
        except Exception as e:
            self.logger.error(f"Failed to setup RabbitMQ: {e}")
            raise

    def process_record(self, row):
        """Process a single record"""
        # Remove cursor field (last one)
        row_data = row[:-1]
        
        return InterBankLoanReceivableRecord(
            reportingDate=str(row_data[0]) if row_data[0] else None,
            borrowersInstitutionCode=str(row_data[1]).strip() if row_data[1] else None,
            borrowerCountry=str(row_data[2]).strip() if row_data[2] else None,
            relationshipType=str(row_data[3]).strip() if row_data[3] else None,
            ratingStatus=int(row_data[4]) if row_data[4] is not None else 0,
            externalRatingCorrespondentBorrower=str(row_data[5]).strip() if row_data[5] else None,
            gradesUnratedBorrower=str(row_data[6]).strip() if row_data[6] else None,
            loanNumber=str(row_data[7]).strip() if row_data[7] else None,
            loanType=str(row_data[8]).strip() if row_data[8] else None,
            issueDate=str(row_data[9]) if row_data[9] else None,
            loanMaturityDate=str(row_data[10]) if row_data[10] else None,
            currency=str(row_data[11]).strip() if row_data[11] else None,
            orgLoanAmount=Decimal(str(row_data[12])) if row_data[12] is not None else None,
            usdLoanAmount=Decimal(str(row_data[13])) if row_data[13] is not None else None,
            tzsLoanAmount=Decimal(str(row_data[14])) if row_data[14] is not None else None,
            interestRate=Decimal(str(row_data[15])) if row_data[15] is not None else None,
            orgAccruedInterestAmount=Decimal(str(row_data[16])) if row_data[16] is not None else None,
            usdAccruedInterestAmount=Decimal(str(row_data[17])) if row_data[17] is not None else None,
            tzsAccruedInterestAmount=Decimal(str(row_data[18])) if row_data[18] is not None else None,
            orgSuspendedInterest=Decimal(str(row_data[19])) if row_data[19] is not None else None,
            usdSuspendedInterest=Decimal(str(row_data[20])) if row_data[20] is not None else None,
            tzsSuspendedInterest=Decimal(str(row_data[21])) if row_data[21] is not None else None,
            pastDueDays=int(row_data[22]) if row_data[22] is not None else 0,
            allowanceProbableLoss=int(row_data[23]) if row_data[23] is not None else 0,
            botProvision=int(row_data[24]) if row_data[24] is not None else 0,
            assetClassificationCategory=str(row_data[25]).strip() if row_data[25] else None
        )
    
    def insert_to_postgres(self, record: InterBankLoanReceivableRecord, cursor):
        """Insert record to PostgreSQL"""
        insert_sql = """
        INSERT INTO "interBankLoanReceivable" (
            "reportingDate", "borrowersInstitutionCode", "borrowerCountry", "relationshipType",
            "ratingStatus", "externalRatingCorrespondentBorrower", "gradesUnratedBorrower",
            "loanNumber", "loanType", "issueDate", "loanMaturityDate", currency,
            "orgLoanAmount", "usdLoanAmount", "tzsLoanAmount", "interestRate",
            "orgAccruedInterestAmount", "usdAccruedInterestAmount", "tzsAccruedInterestAmount",
            "orgSuspendedInterest", "usdSuspendedInterest", "tzsSuspendedInterest",
            "pastDueDays", "allowanceProbableLoss", "botProvision", "assetClassificationCategory"
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        cursor.execute(insert_sql, (
            record.reportingDate,
            record.borrowersInstitutionCode,
            record.borrowerCountry,
            record.relationshipType,
            record.ratingStatus,
            record.externalRatingCorrespondentBorrower,
            record.gradesUnratedBorrower,
            record.loanNumber,
            record.loanType,
            record.issueDate,
            record.loanMaturityDate,
            record.currency,
            record.orgLoanAmount,
            record.usdLoanAmount,
            record.tzsLoanAmount,
            record.interestRate,
            record.orgAccruedInterestAmount,
            record.usdAccruedInterestAmount,
            record.tzsAccruedInterestAmount,
            record.orgSuspendedInterest,
            record.usdSuspendedInterest,
            record.tzsSuspendedInterest,
            record.pastDueDays,
            record.allowanceProbableLoss,
            record.botProvision,
            record.assetClassificationCategory
        ))

    def producer_thread(self):
        """Producer thread - reads from DB2 and sends to RabbitMQ"""
        try:
            self.logger.info("Producer thread started")
            
            # Get total count
            with self.db2_conn.get_connection(log_connection=True) as conn:
                cursor = conn.cursor()
                cursor.execute(self.get_total_count_query())
                self.total_available = cursor.fetchone()[0]
            
            self.logger.info(f"Total inter-bank loan receivable records available: {self.total_available:,}")
            
            # Setup RabbitMQ
            connection, channel = self.setup_rabbitmq_connection()
            
            # Process batches
            batch_number = 1
            last_loan_number = None
            
            while True:
                batch_start_time = time.time()
                
                # Fetch batch
                rows = None
                for attempt in range(self.max_retries):
                    try:
                        with self.db2_conn.get_connection(log_connection=False) as conn:
                            cursor = conn.cursor()
                            batch_query = self.get_query(last_loan_number)
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
                    last_loan_number = row[-1]  # cursor_loan_number
                    
                    record = self.process_record(row)
                    message = json.dumps(asdict(record), default=str)
                    
                    # Publish
                    published = False
                    for attempt in range(self.max_retries):
                        try:
                            channel.basic_publish(
                                exchange='',
                                routing_key='inter_bank_loan_receivable_queue',
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
            self.logger.info("Consumer thread started - waiting for messages...")
            
            connection, channel = self.setup_rabbitmq_connection()
            
            def process_message(ch, method, properties, body):
                try:
                    record_data = json.loads(body)
                    record = InterBankLoanReceivableRecord(**record_data)
                    
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
                        
                        if self.total_consumed % 10 == 0:
                            elapsed_time = time.time() - self.start_time
                            rate = self.total_consumed / elapsed_time if elapsed_time > 0 else 0
                            progress_percent = (self.total_consumed / self.total_available * 100) if self.total_available > 0 else 0
                            self.logger.info(f"Consumer: Processed {self.total_consumed:,} records ({progress_percent:.2f}%) - Rate: {rate:.1f} rec/sec")
                    
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    
                except Exception as e:
                    self.logger.error(f"Consumer error processing message: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            
            channel.basic_qos(prefetch_count=10)
            channel.basic_consume(queue='inter_bank_loan_receivable_queue', on_message_callback=process_message)
            
            while not self.stop_consumer.is_set():
                try:
                    connection.process_data_events(time_limit=1)
                    
                    if self.producer_finished.is_set():
                        method = channel.queue_declare(queue='inter_bank_loan_receivable_queue', durable=True, passive=True)
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
                    channel.basic_consume(queue='inter_bank_loan_receivable_queue', on_message_callback=process_message)
            
            connection.close()
            self.logger.info(f"Consumer finished: {self.total_consumed:,} records processed")
            self.consumer_finished.set()
            
        except Exception as e:
            self.logger.error(f"Consumer error: {e}")
            self.consumer_finished.set()

    def run_streaming_pipeline(self):
        """Run the streaming pipeline"""
        self.logger.info("Starting Inter-Bank Loan Receivable STREAMING pipeline...")
        
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
            Inter-Bank Loan Receivable Pipeline Summary:
            ==========================================
            Total available records: {self.total_available:,}
            Records produced: {self.total_produced:,}
            Records consumed: {self.total_consumed:,}
            Success rate: {success_rate:.1f}%
            Total processing time: {total_time/60:.2f} minutes
            Average rate: {avg_rate:.1f} records/second
            ==========================================
            """)
            
        except Exception as e:
            self.logger.error(f"Pipeline error: {e}")
            raise

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Inter-Bank Loan Receivable Streaming Pipeline')
    parser.add_argument('--batch-size', type=int, default=500, help='Batch size for processing')
    
    args = parser.parse_args()
    
    pipeline = InterBankLoanReceivableStreamingPipeline(batch_size=args.batch_size)
    
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
