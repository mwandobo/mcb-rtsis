#!/usr/bin/env python3
"""
Overdraft Streaming Pipeline - Producer and Consumer run simultaneously
"""

import pika
import psycopg2
import json
import logging
import threading
import time
from dataclasses import asdict
from contextlib import contextmanager

from config import Config
from db2_connection import DB2Connection
from processors.overdraft_processor import OverdraftProcessor, OverdraftRecord

class OverdraftStreamingPipeline:
    def __init__(self, batch_size=50):
        self.config = Config()
        self.db2_conn = DB2Connection()
        self.processor = OverdraftProcessor()
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
        self.retry_delay = 5  # seconds
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        self.logger.info("Overdraft STREAMING Pipeline initialized")
        self.logger.info(f"Batch size: {self.batch_size} records per batch (optimized with cursor pagination)")
        self.logger.info("Mode: Streaming (Producer + Consumer simultaneously)")
        self.logger.info(f"Retry settings: {self.max_retries} retries with {self.retry_delay}s delay")
    
    def get_overdraft_query(self, last_cust_id=None, last_account_number=None):
        """Get the overdraft query with cursor-based pagination for better performance"""
        
        # Use cursor-based pagination instead of ROW_NUMBER() for much better performance
        where_clause = "WHERE gte.FK_GLG_ACCOUNTACCO IN ('1.1.0.05.0001', '1.1.0.05.0002', '1.1.0.05.0005')"
        
        if last_cust_id and last_account_number:
            where_clause += f"""
            AND (gte.CUST_ID > '{last_cust_id}' 
                 OR (gte.CUST_ID = '{last_cust_id}' AND LTRIM(RTRIM(pa.ACCOUNT_NUMBER)) > '{last_account_number}'))
            """
        
        query = f"""
        SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHH24MI') AS reportingDate,
               gte.CUST_ID,
               LTRIM(RTRIM(pa.ACCOUNT_NUMBER)) AS accountNumber,
               LTRIM(RTRIM(id.ID_NO)) AS customerIdentificationNumber,
               TRIM(
                       COALESCE(TRIM(c.FIRST_NAME), '') ||
                       CASE
                           WHEN c.MIDDLE_NAME IS NOT NULL AND TRIM(c.MIDDLE_NAME) <> ''
                               THEN ' ' || c.MIDDLE_NAME
                           ELSE ''
                           END ||
                       CASE
                           WHEN c.SURNAME IS NOT NULL AND TRIM(c.SURNAME) <> ''
                               THEN ' ' || c.SURNAME
                           ELSE ''
                           END
               ) AS clientName,
               ctl.CUSTOMER_TYPE as clientType,
               cl.COUNTRY_CODE as borrowerCountry,
               CAST(0 AS SMALLINT) as ratingStatus,
               CASE c.CUST_TYPE WHEN 2 THEN 'Unrated' END as crRatingBorrower,
               CASE c.CUST_TYPE WHEN 2 THEN 'Grade B' END as gradesUnratedBanks,
               null as groupCode,
               null as relatedEntityName,
               null as relatedParty,
               null as relationshipCategory,
               p.DESCRIPTION as loanProductType,
               gte.ID_PRODUCT as ID_PRODUCT,
               'OtherServices' as overdraftEconomicActivity,

               CASE la.ACC_STATUS
                   WHEN '1' THEN 'Existing'
                   WHEN '2' THEN 'TerminatedInAdvanceCorrectly'
                   WHEN '3' THEN 'TerminatedAccordingTheContract'
                   WHEN '4' THEN 'TerminatedInAdvanceIncorrectly'
                   WHEN '5' THEN 'TerminatedInAdvanceIncorrectly'
                   WHEN '6' THEN 'TerminatedInAdvanceIncorrectly'
                   ELSE 'TerminatedAccordingTheContract'
                   END AS loanPhase,
               'NotSpecified' AS transferStatus,

               CASE p.DESCRIPTION
                   WHEN 'STAFF CURRENT ACCOUNT OVERDRAFT' THEN 'StaffLoan'
                   WHEN 'OVERDRAFT-CORPORATE' THEN 'Business'
                   WHEN 'NGO/CLUB ACCOUNT OVERDRAFT' THEN 'Development'
                   ELSE 'Other'
                   END AS purposeOtherLoans,
               la.ACC_OPEN_DT as contractDate,
               la.FK_UNITCODE as branchCode,
               TRIM(
                       COALESCE(TRIM(be.FIRST_NAME), '') ||
                       CASE
                           WHEN TRIM(be.FATHER_NAME) IS NOT NULL AND TRIM(be.FATHER_NAME) <> ''
                               THEN ' ' || TRIM(be.FATHER_NAME)
                           ELSE ''
                           END ||
                       CASE
                           WHEN TRIM(be.LAST_NAME) IS NOT NULL AND TRIM(be.LAST_NAME) <> ''
                               THEN ' ' || TRIM(be.LAST_NAME)
                           ELSE ''
                           END
               ) AS loanOfficer,
               TRIM(
                       COALESCE(TRIM(be.FIRST_NAME), '') ||
                       CASE
                           WHEN TRIM(be.FATHER_NAME) IS NOT NULL AND TRIM(be.FATHER_NAME) <> ''
                               THEN ' ' || TRIM(be.FATHER_NAME)
                           ELSE ''
                           END ||
                       CASE
                           WHEN TRIM(be.LAST_NAME) IS NOT NULL AND TRIM(be.LAST_NAME) <> ''
                               THEN ' ' || TRIM(be.LAST_NAME)
                           ELSE ''
                           END
               ) AS loanSupervisor,

               gte.CURRENCY_SHORT_DES as currency,
               ag.AGR_LIMIT as orgSanctionedAmount,
               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'USD'
                       THEN ag.AGR_LIMIT
                   ELSE NULL
                   END AS usdSanctionedAmount,
               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'USD'
                       THEN ag.AGR_LIMIT * 2500
                   ELSE
                       ag.AGR_LIMIT
                   END AS tzsSanctionedAmount,
               la.TOT_DRAWDOWN_AMN as orgUtilisedAmount,
               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'USD'
                       THEN la.TOT_DRAWDOWN_AMN
                   ELSE NULL
                   END AS usdUtilisedAmount,
               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'USD'
                       THEN la.TOT_DRAWDOWN_AMN * 2500
                   ELSE
                       la.TOT_DRAWDOWN_AMN
                   END AS tzsUtilisedAmount,
               gte.DC_AMOUNT as orgCrUsageLast30DaysAmount,
               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'USD'
                       THEN gte.DC_AMOUNT
                   ELSE NULL
                   END AS usdCrUsageLast30DaysAmount,
               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'USD'
                       THEN gte.DC_AMOUNT * 2500
                   ELSE
                       gte.DC_AMOUNT
                   END AS tzsCrUsageLast30DaysAmount,
               la.DRAWDOWN_FST_DT AS disbursementDate,
               la.ACC_EXP_DT AS expiryDate,
               COALESCE(la.OV_EXP_DT, la.ACC_EXP_DT) AS realEndDate,
               (la.NRM_CAP_BAL + la.OV_CAP_BAL)
                   + (la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL) AS orgOutstandingAmount,

               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN (la.NRM_CAP_BAL + la.OV_CAP_BAL)
                       + (la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL)
                   ELSE NULL
                   END AS usdOutstandingAmount,
               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN (la.NRM_CAP_BAL + la.OV_CAP_BAL)
                       + (la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL) * 2500
                   ELSE (la.NRM_CAP_BAL + la.OV_CAP_BAL)
                       + (la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL)
                   END AS tzsOutstandingAmount,
               (
                   la.NRM_CAP_BAL + la.OV_CAP_BAL
                   ) AS orgOutstandingPrincipalAmount,
               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'USD'
                       THEN (la.NRM_CAP_BAL + la.OV_CAP_BAL)
                   ELSE NULL
                   END AS usdOutstandingPrincipalAmount,
               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'USD'
                       THEN (la.NRM_CAP_BAL + la.OV_CAP_BAL) * 2500
                   ELSE (la.NRM_CAP_BAL + la.OV_CAP_BAL)
                   END AS tzsOutstandingPrincipalAmount,
               la.DRAWDOWN_FST_DT AS latestCustomerCreditDate,
               la.DRAWDOWN_FST_AMN AS latestCreditAmount,
               la.INTER_RATE_SPRD AS primeLendingRate,
               la.MORATOR_NRM_RATE AS annualInterestRate,
               null AS collateralPledged,
               null AS orgCollateralValue,
               null AS usdCollateralValue,
               null AS tzsCollateralValue,
               0 AS restructuredLoans,
               la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL AS orgAccruedInterestAmount,
               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL
                   ELSE NULL
                   END AS usdAccruedInterestAmount,
               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN
                       (la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL) *
                       2500
                   ELSE la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL
                   END AS tzsAccruedInterestAmount,

               la.OV_RL_PNL_INT_BAL + la.OV_URL_PNL_INT_BAL AS orgPenaltyChargedAmount,
               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN la.OV_RL_PNL_INT_BAL + la.OV_URL_PNL_INT_BAL
                   ELSE NULL
                   END AS usdPenaltyChargedAmount,
               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN (la.OV_RL_PNL_INT_BAL + la.OV_URL_PNL_INT_BAL) * 2500
                   ELSE la.OV_RL_PNL_INT_BAL + la.OV_URL_PNL_INT_BAL
                   END AS tzsPenaltyChargedAmount,
               COALESCE(la.OV_RL_PNL_INT_BAL, 0) AS orgPenaltyPaidAmount,
               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN COALESCE(la.OV_RL_PNL_INT_BAL, 0)
                   ELSE NULL
                   END AS usdPenaltyPaidAmount,
               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN COALESCE(la.OV_RL_PNL_INT_BAL, 0) * 2500
                   ELSE COALESCE(la.OV_RL_PNL_INT_BAL, 0)
                   END AS tzsPenaltyPaidAmount,


               la.TOT_COMMISSION_AMN AS orgLoanFeesChargedAmount,
               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN la.TOT_COMMISSION_AMN
                   ELSE NULL
                   END AS usdLoanFeesChargedAmount,
               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN la.TOT_COMMISSION_AMN * 2500
                   ELSE la.TOT_COMMISSION_AMN
                   END AS tzsLoanFeesChargedAmount,

               la.TOT_EXPENSE_AMN AS orgLoanFeesPaidAmount,
               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN la.TOT_EXPENSE_AMN
                   ELSE NULL
                   END AS usdLoanFeesPaidAmount,
               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN la.TOT_EXPENSE_AMN * 2500
                   ELSE la.TOT_EXPENSE_AMN
                   END AS tzsLoanFeesPaidAmount,

               0.00 AS orgTotMonthlyPaymentAmount,
               0.00 AS usdTotMonthlyPaymentAmount,
               0.00 AS tzsTotMonthlyPaymentAmount,

               la.TOT_NRM_INT_AMN + la.TOT_PNL_INT_AMN AS orgInterestPaidTotal,
               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN la.TOT_NRM_INT_AMN + la.TOT_PNL_INT_AMN
                   ELSE NULL
                   END AS usdInterestPaidTotal,
               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN (la.TOT_NRM_INT_AMN + la.TOT_PNL_INT_AMN) * 2500
                   ELSE la.TOT_NRM_INT_AMN + la.TOT_PNL_INT_AMN
                   END AS tzsInterestPaidTotal,
               'Current' AS assetClassificationCategory,
               'Other financial Corporations' AS sectorSnaClassification,
               la.ACC_STATUS AS negStatusContract,
               'N/A' AS customerRole,
               0 AS allowanceProbableLoss,
               0 AS botProvision,
               
               gte.CUST_ID as cursor_cust_id,
               LTRIM(RTRIM(pa.ACCOUNT_NUMBER)) as cursor_account_number

        FROM GLI_TRX_EXTRACT gte
                 LEFT JOIN CUSTOMER c
                           ON gte.CUST_ID = c.CUST_ID
                 LEFT JOIN CUSTOMER_TYPES_LOOKUP ctl ON ctl.CUSTOMER_TYPE_CODE = c.CUST_TYPE
                 LEFT JOIN PROFITS_ACCOUNT pa
                           ON pa.CUST_ID = gte.CUST_ID
                 LEFT JOIN PRODUCT p
                           ON p.ID_PRODUCT = gte.ID_PRODUCT
                 LEFT JOIN other_id id ON (CASE WHEN (id.serial_no IS NULL) THEN '1' ELSE id.main_flag END = '1' AND
                                           id.fk_customercust_id = c.cust_id)

                 LEFT JOIN cust_address ca
                           ON (ca.fk_customercust_id = c.cust_id AND ca.communication_addr = '1' AND
                               ca.entry_status = '1')
                 LEFT JOIN generic_detail id_country ON (id.fkgh_has_been_issu = id_country.fk_generic_headpar AND
                                                         id.fkgd_has_been_issu = id_country.serial_num)
                 LEFT JOIN COUNTRIES_LOOKUP cl ON cl.COUNTRY_NAME = id_country.description


                 LEFT JOIN LOAN_ACCOUNT la
                           ON gte.ID_PRODUCT = la.FK_LOANFK_PRODUCTI
                               AND la.CUST_ID = gte.CUST_ID

                 JOIN AGREEMENT ag
                      ON ag.FK_UNITCODE = la.FK_AGREEMENTFK_UNI
                          AND ag.AGR_YEAR = la.FK_AGREEMENTAGR_YE
                          AND ag.AGR_SN = la.FK_AGREEMENTAGR_SN
                          AND ag.AGR_MEMBERSHIP_SN = la.FK_AGREEMENTAGR_ME

                 LEFT JOIN BANKEMPLOYEE be
                           ON be.STAFF_NO = la.USR
        {where_clause}
        ORDER BY gte.CUST_ID ASC, LTRIM(RTRIM(pa.ACCOUNT_NUMBER)) ASC
        FETCH FIRST {self.batch_size} ROWS ONLY
        """
        
        return query
    
    def get_total_count_query(self):
        """Get total count of available overdraft records"""
        return """
        SELECT COUNT(*) as total_count
        FROM GLI_TRX_EXTRACT gte
                 LEFT JOIN CUSTOMER c ON gte.CUST_ID = c.CUST_ID
                 LEFT JOIN PROFITS_ACCOUNT pa ON pa.CUST_ID = gte.CUST_ID
                 LEFT JOIN PRODUCT p ON p.ID_PRODUCT = gte.ID_PRODUCT
                 LEFT JOIN LOAN_ACCOUNT la ON gte.ID_PRODUCT = la.FK_LOANFK_PRODUCTI AND la.CUST_ID = gte.CUST_ID
                 JOIN AGREEMENT ag ON ag.FK_UNITCODE = la.FK_AGREEMENTFK_UNI
                          AND ag.AGR_YEAR = la.FK_AGREEMENTAGR_YE
                          AND ag.AGR_SN = la.FK_AGREEMENTAGR_SN
                          AND ag.AGR_MEMBERSHIP_SN = la.FK_AGREEMENTAGR_ME
        WHERE gte.FK_GLG_ACCOUNTACCO IN ('1.1.0.05.0001', '1.1.0.05.0002', '1.1.0.05.0005')
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
                    heartbeat=600,  # 10 minutes heartbeat
                    blocked_connection_timeout=300,  # 5 minutes timeout
                )
                connection = pika.BlockingConnection(parameters)
                channel = connection.channel()
                return connection, channel
                
            except Exception as e:
                self.logger.warning(f"RabbitMQ connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    self.logger.info(f"Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                else:
                    self.logger.error(f"Failed to connect to RabbitMQ after {max_retries} attempts")
                    raise
    
    def setup_rabbitmq_queue(self):
        """Setup RabbitMQ queue for overdraft"""
        try:
            connection, channel = self.setup_rabbitmq_connection()
            
            # Declare queue with durability
            channel.queue_declare(queue='overdraft_queue', durable=True)
            
            connection.close()
            self.logger.info("RabbitMQ queue 'overdraft_queue' setup complete")
            
        except Exception as e:
            self.logger.error(f"Failed to setup RabbitMQ: {e}")
            raise
    
    def producer_thread(self):
        """Producer thread - reads from DB2 and sends to RabbitMQ with cursor-based pagination"""
        try:
            self.logger.info("Producer thread started")
            
            # Get total count first
            with self.db2_conn.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(self.get_total_count_query())
                self.total_available = cursor.fetchone()[0]
            
            self.logger.info(f"Total overdraft records available: {self.total_available:,}")
            estimated_batches = (self.total_available + self.batch_size - 1) // self.batch_size
            self.logger.info(f"Estimated batches to process: {estimated_batches:,}")
            
            # Setup RabbitMQ connection with retry
            connection, channel = self.setup_rabbitmq_connection()
            
            # Process batches using cursor-based pagination
            batch_number = 1
            last_cust_id = None
            last_account_number = None
            last_progress_report = time.time()
            
            while True:
                batch_start_time = time.time()
                
                # Fetch batch with retry logic using cursor pagination
                rows = None
                for attempt in range(self.max_retries):
                    try:
                        with self.db2_conn.get_connection() as conn:
                            cursor = conn.cursor()
                            batch_query = self.get_overdraft_query(last_cust_id, last_account_number)
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
                
                # Process and publish with retry logic
                batch_published = 0
                for row in rows:
                    # Extract cursor values for next batch (last two fields)
                    last_cust_id = row[-2]  # cursor_cust_id
                    last_account_number = row[-1]  # cursor_account_number
                    
                    # Remove cursor fields before processing
                    row_without_cursor = row[:-2]
                    record = self.processor.process_record(row_without_cursor, 'overdraft')
                    
                    if self.processor.validate_record(record):
                        message = json.dumps(asdict(record), default=str)
                        
                        # Publish with retry
                        published = False
                        for attempt in range(self.max_retries):
                            try:
                                channel.basic_publish(
                                    exchange='',
                                    routing_key='overdraft_queue',
                                    body=message,
                                    properties=pika.BasicProperties(delivery_mode=2)
                                )
                                published = True
                                break
                            except Exception as e:
                                self.logger.warning(f"RabbitMQ publish attempt {attempt + 1} failed: {e}")
                                if attempt < self.max_retries - 1:
                                    # Reconnect and retry
                                    try:
                                        connection.close()
                                    except:
                                        pass
                                    connection, channel = self.setup_rabbitmq_connection()
                                    time.sleep(self.retry_delay)
                                else:
                                    self.logger.error(f"Failed to publish message after {self.max_retries} attempts")
                        
                        if published:
                            batch_published += 1
                            self.total_produced += 1
                
                batch_time = time.time() - batch_start_time
                progress_percent = self.total_produced / self.total_available * 100 if self.total_available > 0 else 0
                
                self.logger.info(f"Producer: Batch {batch_number:,} - {len(rows)} records, {batch_published} published ({progress_percent:.2f}% complete, {batch_time:.1f}s)")
                
                # Progress report every 5 minutes
                current_time = time.time()
                if current_time - last_progress_report >= 300:  # 5 minutes
                    elapsed_time = current_time - self.start_time
                    rate = self.total_produced / elapsed_time if elapsed_time > 0 else 0
                    remaining_records = self.total_available - self.total_produced
                    eta_seconds = remaining_records / rate if rate > 0 else 0
                    eta_hours = eta_seconds / 3600
                    
                    self.logger.info(f"PROGRESS REPORT: {self.total_produced:,}/{self.total_available:,} records ({progress_percent:.1f}%) - Rate: {rate:.1f} rec/sec - ETA: {eta_hours:.1f} hours")
                    last_progress_report = current_time
                
                # Move to next batch
                batch_number += 1
                
                # Small delay to prevent overwhelming
                time.sleep(0.1)
            
            connection.close()
            self.logger.info(f"Producer finished: {self.total_produced:,} records published out of {self.total_available:,} available")
            self.producer_finished.set()
            
        except Exception as e:
            self.logger.error(f"Producer error: {e}")
            self.producer_finished.set()
    
    def consumer_thread(self):
        """Consumer thread - processes messages from queue with retry logic"""
        try:
            self.logger.info("Consumer thread started")
            
            # Setup RabbitMQ connection with retry
            connection, channel = self.setup_rabbitmq_connection()
            
            # Initialize progress tracking variable
            last_progress_report = time.time()
            
            def process_message(ch, method, properties, body):
                nonlocal last_progress_report  # Allow modification of the outer variable
                try:
                    record_data = json.loads(body)
                    record = OverdraftRecord(**record_data)
                    
                    # Insert to PostgreSQL with retry
                    inserted = False
                    for attempt in range(self.max_retries):
                        try:
                            with self.get_postgres_connection() as conn:
                                cursor = conn.cursor()
                                self.processor.insert_to_postgres(record, cursor)
                                conn.commit()
                            inserted = True
                            break
                        except Exception as e:
                            self.logger.warning(f"PostgreSQL insert attempt {attempt + 1} failed: {e}")
                            if attempt < self.max_retries - 1:
                                time.sleep(self.retry_delay)
                            else:
                                self.logger.error(f"Failed to insert record after {self.max_retries} attempts")
                    
                    if inserted:
                        self.total_consumed += 1
                        
                        # Progress monitoring
                        if self.total_consumed % (self.batch_size * 2) == 0:  # Every 2 batches
                            elapsed_time = time.time() - self.start_time
                            rate = self.total_consumed / elapsed_time if elapsed_time > 0 else 0
                            progress_percent = (self.total_consumed / self.total_available * 100) if self.total_available > 0 else 0
                            
                            self.logger.info(f"Consumer: Processed {self.total_consumed:,} records ({progress_percent:.2f}% of total) - Rate: {rate:.1f} rec/sec")
                        
                        # Detailed progress report every 5 minutes
                        current_time = time.time()
                        if current_time - last_progress_report >= 300:  # 5 minutes
                            elapsed_time = current_time - self.start_time
                            rate = self.total_consumed / elapsed_time if elapsed_time > 0 else 0
                            remaining_records = self.total_available - self.total_consumed if self.total_available > 0 else 0
                            eta_seconds = remaining_records / rate if rate > 0 else 0
                            eta_hours = eta_seconds / 3600
                            
                            self.logger.info(f"CONSUMER PROGRESS: {self.total_consumed:,}/{self.total_available:,} records - Rate: {rate:.1f} rec/sec - ETA: {eta_hours:.1f} hours")
                            last_progress_report = current_time
                    
                    # Acknowledge message
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    
                except Exception as e:
                    self.logger.error(f"Consumer error processing message: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            
            # Set QoS for controlled processing
            channel.basic_qos(prefetch_count=10)
            channel.basic_consume(queue='overdraft_queue', on_message_callback=process_message)
            
            # Keep consuming until producer is done and queue is empty
            while not self.stop_consumer.is_set():
                try:
                    connection.process_data_events(time_limit=1)
                    
                    # Check if we should stop
                    if self.producer_finished.is_set():
                        # Producer is done, check if queue is empty
                        method = channel.queue_declare(queue='overdraft_queue', durable=True, passive=True)
                        if method.method.message_count == 0:
                            self.logger.info("Consumer: Queue empty, producer finished")
                            break
                        
                except Exception as e:
                    self.logger.error(f"Consumer processing error: {e}")
                    # Try to reconnect
                    try:
                        connection.close()
                    except:
                        pass
                    connection, channel = self.setup_rabbitmq_connection()
                    channel.basic_qos(prefetch_count=10)
                    channel.basic_consume(queue='overdraft_queue', on_message_callback=process_message)
            
            connection.close()
            self.logger.info(f"Consumer finished: {self.total_consumed:,} records processed")
            self.consumer_finished.set()
            
        except Exception as e:
            self.logger.error(f"Consumer error: {e}")
            self.consumer_finished.set()
    
    def run_streaming_pipeline(self):
        """Run the streaming pipeline with simultaneous producer and consumer"""
        self.logger.info("Starting Overdraft STREAMING pipeline...")
        
        try:
            # Setup queue
            self.setup_rabbitmq_queue()
            
            # Start consumer thread first
            consumer_thread = threading.Thread(target=self.consumer_thread, name="Consumer")
            consumer_thread.start()
            
            # Small delay to let consumer start
            time.sleep(1)
            
            # Start producer thread
            producer_thread = threading.Thread(target=self.producer_thread, name="Producer")
            producer_thread.start()
            
            # Wait for producer to finish
            producer_thread.join()
            self.logger.info("Producer thread completed")
            
            # Wait for consumer to finish processing remaining messages
            consumer_thread.join(timeout=60)  # Wait up to 60 seconds
            
            if consumer_thread.is_alive():
                self.logger.info("Stopping consumer thread...")
                self.stop_consumer.set()
                consumer_thread.join(timeout=30)
            
            # Final statistics
            total_time = time.time() - self.start_time
            avg_rate = self.total_consumed / total_time if total_time > 0 else 0
            success_rate = (self.total_consumed / self.total_produced * 100) if self.total_produced > 0 else 0
            
            self.logger.info(f"""
            ==========================================
            Overdraft Pipeline Summary:
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
    
    parser = argparse.ArgumentParser(description='Overdraft Streaming Pipeline')
    parser.add_argument('--batch-size', type=int, default=50, help='Batch size for processing')
    parser.add_argument('--mode', choices=['producer', 'consumer', 'streaming'], default='streaming',
                       help='Pipeline mode: producer only, consumer only, or full streaming')
    
    args = parser.parse_args()
    
    # Create pipeline
    pipeline = OverdraftStreamingPipeline(batch_size=args.batch_size)
    
    try:
        if args.mode == 'producer':
            pipeline.producer_thread()
        elif args.mode == 'consumer':
            pipeline.consumer_thread()
        else:  # streaming
            pipeline.run_streaming_pipeline()
            
    except KeyboardInterrupt:
        pipeline.logger.info("Pipeline stopped by user")
    except Exception as e:
        pipeline.logger.error(f"Pipeline failed: {e}")
        import sys
        sys.exit(1)

if __name__ == "__main__":
    main()