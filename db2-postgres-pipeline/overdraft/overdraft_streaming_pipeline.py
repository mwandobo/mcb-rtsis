#!/usr/bin/env python3
"""
Overdraft Streaming Pipeline - Producer and Consumer run simultaneously
Uses overdraft-v3.sql query for comprehensive overdraft data extraction
"""

import pika
import psycopg2
import psycopg2.extras
import json
import logging
import threading
import time
from dataclasses import dataclass, asdict
from contextlib import contextmanager
from typing import Optional, List
from datetime import datetime
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config
from db2_connection import DB2Connection


@dataclass
class OverdraftRecord:
    """Data class for overdraft records based on overdraft-v3.sql"""
    reportingDate: str
    accountNumber: str
    customerIdentificationNumber: str
    clientName: str
    clientType: Optional[str]
    borrowerCountry: str
    ratingStatus: int
    crRatingBorrower: str
    gradesUnratedBanks: str
    groupCode: Optional[str]
    relatedEntityName: Optional[str]
    relatedParty: Optional[str]
    relationshipCategory: Optional[str]
    loanProductType: str
    overdraftEconomicActivity: str
    loanPhase: str
    transferStatus: str
    purposeOtherLoans: str
    contractDate: str
    branchCode: str
    loanOfficer: str
    loanSupervisor: str
    currency: str
    orgSanctionedAmount: Optional[float]
    usdSanctionedAmount: Optional[float]
    tzsSanctionedAmount: Optional[float]
    orgUtilisedAmount: Optional[float]
    usdUtilisedAmount: Optional[float]
    tzsUtilisedAmount: Optional[float]
    orgCrUsageLast30DaysAmount: Optional[float]
    usdCrUsageLast30DaysAmount: Optional[float]
    tzsCrUsageLast30DaysAmount: Optional[float]
    disbursementDate: str
    expiryDate: str
    realEndDate: str
    orgOutstandingAmount: Optional[float]
    usdOutstandingAmount: Optional[float]
    tzsOutstandingAmount: Optional[float]
    orgOutstandingPrincipalAmount: Optional[float]
    usdOutstandingPrincipalAmount: Optional[float]
    tzsOutstandingPrincipalAmount: Optional[float]
    latestCustomerCreditDate: str
    latestCreditAmount: Optional[float]
    primeLendingRate: Optional[float]
    annualInterestRate: Optional[float]
    collateralPledged: Optional[str]  # JSON string
    restructuredLoans: int
    pastDueDays: int
    pastDueAmount: Optional[float]
    orgAccruedInterestAmount: Optional[float]
    usdAccruedInterestAmount: Optional[float]
    tzsAccruedInterestAmount: Optional[float]
    orgPenaltyChargedAmount: Optional[float]
    usdPenaltyChargedAmount: Optional[float]
    tzsPenaltyChargedAmount: Optional[float]
    orgPenaltyPaidAmount: Optional[float]
    usdPenaltyPaidAmount: Optional[float]
    tzsPenaltyPaidAmount: Optional[float]
    orgLoanFeesChargedAmount: Optional[float]
    usdLoanFeesChargedAmount: Optional[float]
    tzsLoanFeesChargedAmount: Optional[float]
    orgLoanFeesPaidAmount: Optional[float]
    usdLoanFeesPaidAmount: Optional[float]
    tzsLoanFeesPaidAmount: Optional[float]
    orgTotMonthlyPaymentAmount: float
    usdTotMonthlyPaymentAmount: float
    tzsTotMonthlyPaymentAmount: float
    orgInterestPaidTotal: Optional[float]
    usdInterestPaidTotal: Optional[float]
    tzsInterestPaidTotal: Optional[float]
    assetClassificationCategory: str
    sectorSnaClassification: str
    negStatusContract: str
    customerRole: str
    allowanceProbableLoss: int
    botProvision: int


class OverdraftStreamingPipeline:
    def __init__(self, batch_size=50, consumer_batch_size=100):
        self.config = Config()
        self.db2_conn = DB2Connection()
        self.batch_size = batch_size
        self.consumer_batch_size = consumer_batch_size
        
        # Threading control
        self.producer_finished = threading.Event()
        self.consumer_finished = threading.Event()
        self.stop_consumer = threading.Event()
        
        # Thread-safe statistics
        self._stats_lock = threading.Lock()
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
        self.logger.info(f"Batch size: {self.batch_size} records per batch")
        self.logger.info(f"Consumer batch size: {self.consumer_batch_size} records per flush")
        self.logger.info("Mode: Streaming (Producer + Consumer simultaneously)")
        self.logger.info(f"Retry settings: {self.max_retries} retries with {self.retry_delay}s delay")
    
    def get_overdraft_query(self, last_customer_id=None, last_account_number=None):
        """Get the overdraft query using overdraft-v3.sql with cursor-based pagination"""
        
        # Read the overdraft-v3.sql file
        sql_file_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'sqls', 'overdraft-v3.sql')
        
        with open(sql_file_path, 'r') as f:
            base_query = f.read()
        
        # Build cursor-based pagination condition
        cursor_condition = ""
        if last_customer_id and last_account_number:
            cursor_condition = f"""
            AND (customerIdentificationNumber > '{last_customer_id}' 
                 OR (customerIdentificationNumber = '{last_customer_id}' AND accountNumber > '{last_account_number}'))
            """
        
        # Insert cursor condition into the final WHERE clause
        if cursor_condition:
            base_query = base_query.replace(
                "WHERE LENGTH(TRIM(TRANSLATE(t.loanOfficer, '', '0123456789'))) = LENGTH(TRIM(t.loanOfficer))",
                f"WHERE LENGTH(TRIM(TRANSLATE(t.loanOfficer, '', '0123456789'))) = LENGTH(TRIM(t.loanOfficer)) {cursor_condition}"
            )
        
        # Add ORDER BY and FETCH FIRST for pagination
        query = base_query.rstrip(';') + f"""
        ORDER BY customerIdentificationNumber ASC, accountNumber ASC
        FETCH FIRST {self.batch_size} ROWS ONLY
        """
        
        return query
    
    def get_total_count_query(self):
        """Get total count of available overdraft records"""
        return """
        SELECT COUNT(*) as total_count
        FROM (
            SELECT TRIM(
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
                   ) AS loanSupervisor
            FROM GLI_TRX_EXTRACT gte
                     LEFT JOIN CUSTOMER c ON gte.CUST_ID = c.CUST_ID
                     LEFT JOIN CUSTOMER_TYPES_LOOKUP ctl ON ctl.CUSTOMER_TYPE_CODE = c.CUST_TYPE
                     JOIN PROFITS_ACCOUNT pa ON pa.CUST_ID = c.CUST_ID
                              AND pa.PRFT_SYSTEM = 4
                              AND pa.ACCOUNT_NUMBER IS NOT NULL
                              AND pa.PRODUCT_ID IN (40030, 40034, 40035, 40036, 40037, 40040)
                     LEFT JOIN PRODUCT p ON p.ID_PRODUCT = gte.ID_PRODUCT
                     LEFT JOIN other_id id ON (CASE WHEN (id.serial_no IS NULL) THEN '1' ELSE id.main_flag END = '1'
                                               AND id.fk_customercust_id = c.cust_id)
                     LEFT JOIN cust_address ca ON ca.fk_customercust_id = c.cust_id
                                   AND ca.communication_addr = '1'
                                   AND ca.entry_status = '1'
                     LEFT JOIN generic_detail id_country ON id.fkgh_has_been_issu = id_country.fk_generic_headpar
                                   AND id.fkgd_has_been_issu = id_country.serial_num
                     LEFT JOIN COUNTRIES_LOOKUP cl ON cl.COUNTRY_NAME = id_country.description
                     INNER JOIN (SELECT la.*,
                                        ROW_NUMBER() OVER (
                                            PARTITION BY la.CUST_ID, la.FK_LOANFK_PRODUCTI
                                            ORDER BY la.TMSTAMP DESC, la.ACC_OPEN_DT DESC
                                            ) AS rn
                                 FROM LOAN_ACCOUNT la) la
                                ON gte.ID_PRODUCT = la.FK_LOANFK_PRODUCTI
                                    AND la.CUST_ID = c.CUST_ID
                                    AND la.rn = 1
                     JOIN AGREEMENT ag ON ag.FK_UNITCODE = la.FK_AGREEMENTFK_UNI
                              AND ag.AGR_YEAR = la.FK_AGREEMENTAGR_YE
                              AND ag.AGR_SN = la.FK_AGREEMENTAGR_SN
                              AND ag.AGR_MEMBERSHIP_SN = la.FK_AGREEMENTAGR_ME
                     LEFT JOIN BANKEMPLOYEE be ON be.STAFF_NO = la.USR
            WHERE gte.FK_GLG_ACCOUNTACCO IN ('1.1.0.05.0001', '1.1.0.05.0002', '1.1.0.05.0005')
        ) t
        WHERE LENGTH(TRIM(TRANSLATE(t.loanOfficer, '', '0123456789'))) = LENGTH(TRIM(t.loanOfficer))
          AND LENGTH(TRIM(TRANSLATE(t.loanSupervisor, '', '0123456789'))) = LENGTH(TRIM(t.loanSupervisor))
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
        """Setup RabbitMQ queue for overdraft with dead-letter exchange"""
        try:
            connection, channel = self.setup_rabbitmq_connection()
            
            # Declare dead-letter exchange and queue for failed messages
            channel.exchange_declare(exchange='overdraft_dlx', exchange_type='direct', durable=True)
            channel.queue_declare(queue='overdraft_dead_letter', durable=True)
            channel.queue_bind(
                queue='overdraft_dead_letter',
                exchange='overdraft_dlx',
                routing_key='overdraft_queue'
            )
            
            # Declare main queue with dead-letter exchange routing
            try:
                channel.queue_declare(
                    queue='overdraft_queue',
                    durable=True,
                    arguments={
                        'x-dead-letter-exchange': 'overdraft_dlx',
                        'x-dead-letter-routing-key': 'overdraft_queue'
                    }
                )
                self.logger.info("RabbitMQ queues setup complete (main + dead-letter)")
            except Exception:
                self.logger.warning("Queue 'overdraft_queue' already exists with different args.")
                connection, channel = self.setup_rabbitmq_connection()
                channel.queue_declare(queue='overdraft_queue', durable=True)
                self.logger.info("RabbitMQ queue 'overdraft_queue' setup complete (without DLX)")
            
            connection.close()
            
        except Exception as e:
            self.logger.error(f"Failed to setup RabbitMQ: {e}")
            raise
    
    def process_record(self, row):
        """Process a single overdraft record from DB2"""
        try:
            # Helper function to convert date strings
            def convert_date_string(date_str):
                """Convert DDMMYYYYHHMM format to string, handle invalid dates"""
                if not date_str:
                    return None
                
                # Convert to string and clean
                date_str = str(date_str).strip()
                
                # Check for invalid/default dates
                invalid_dates = ['010100011201', '250720161207', '000000000000', '']
                if date_str in invalid_dates or len(date_str) < 8:
                    return None
                
                # Basic validation for date format
                try:
                    if len(date_str) >= 8:
                        day = int(date_str[:2])
                        month = int(date_str[2:4])
                        year = int(date_str[4:8])
                        
                        # Basic range validation
                        if day < 1 or day > 31 or month < 1 or month > 12 or year < 1900 or year > 2100:
                            return None
                            
                        return date_str
                except (ValueError, IndexError):
                    return None
                
                return date_str
            
            # Helper function to safely convert values
            def safe_string(value, max_length=255):
                """Safely convert to string and truncate if needed"""
                if value is None:
                    return None
                str_value = str(value).strip()
                return str_value[:max_length] if len(str_value) > max_length else str_value
            
            def safe_float(value):
                """Safely convert to float"""
                if value is None:
                    return None
                try:
                    return float(value)
                except (ValueError, TypeError):
                    return None
            
            def safe_int(value):
                """Safely convert to int"""
                if value is None:
                    return 0
                try:
                    return int(value)
                except (ValueError, TypeError):
                    return 0
            
            # Map the fields from the SQL query to the dataclass
            record = OverdraftRecord(
                reportingDate=safe_string(row[0]),
                accountNumber=safe_string(row[1]),
                customerIdentificationNumber=safe_string(row[2]),
                clientName=safe_string(row[3]),
                clientType=safe_string(row[4]),
                borrowerCountry=safe_string(row[5]),
                ratingStatus=safe_int(row[6]),
                crRatingBorrower=safe_string(row[7]),
                gradesUnratedBanks=safe_string(row[8]),
                groupCode=safe_string(row[9]),
                relatedEntityName=safe_string(row[10]),
                relatedParty=safe_string(row[11]),
                relationshipCategory=safe_string(row[12]),
                loanProductType=safe_string(row[13]),
                overdraftEconomicActivity=safe_string(row[14]),
                loanPhase=safe_string(row[15]),
                transferStatus=safe_string(row[16]),
                purposeOtherLoans=safe_string(row[17]),
                contractDate=convert_date_string(row[18]),
                branchCode=safe_string(row[19]),
                loanOfficer=safe_string(row[20]),
                loanSupervisor=safe_string(row[21]),
                currency=safe_string(row[22]),
                orgSanctionedAmount=safe_float(row[23]),
                usdSanctionedAmount=safe_float(row[24]),
                tzsSanctionedAmount=safe_float(row[25]),
                orgUtilisedAmount=safe_float(row[26]),
                usdUtilisedAmount=safe_float(row[27]),
                tzsUtilisedAmount=safe_float(row[28]),
                orgCrUsageLast30DaysAmount=safe_float(row[29]),
                usdCrUsageLast30DaysAmount=safe_float(row[30]),
                tzsCrUsageLast30DaysAmount=safe_float(row[31]),
                disbursementDate=convert_date_string(row[32]),
                expiryDate=convert_date_string(row[33]),
                realEndDate=convert_date_string(row[34]),
                orgOutstandingAmount=safe_float(row[35]),
                usdOutstandingAmount=safe_float(row[36]),
                tzsOutstandingAmount=safe_float(row[37]),
                orgOutstandingPrincipalAmount=safe_float(row[38]),
                usdOutstandingPrincipalAmount=safe_float(row[39]),
                tzsOutstandingPrincipalAmount=safe_float(row[40]),
                latestCustomerCreditDate=convert_date_string(row[41]),
                latestCreditAmount=safe_float(row[42]),
                primeLendingRate=safe_float(row[43]),
                annualInterestRate=safe_float(row[44]),
                collateralPledged=safe_string(row[45], 2000),  # JSON can be longer
                restructuredLoans=safe_int(row[46]),
                pastDueDays=safe_int(row[47]),
                pastDueAmount=safe_float(row[48]),
                orgAccruedInterestAmount=safe_float(row[49]),
                usdAccruedInterestAmount=safe_float(row[50]),
                tzsAccruedInterestAmount=safe_float(row[51]),
                orgPenaltyChargedAmount=safe_float(row[52]),
                usdPenaltyChargedAmount=safe_float(row[53]),
                tzsPenaltyChargedAmount=safe_float(row[54]),
                orgPenaltyPaidAmount=safe_float(row[55]),
                usdPenaltyPaidAmount=safe_float(row[56]),
                tzsPenaltyPaidAmount=safe_float(row[57]),
                orgLoanFeesChargedAmount=safe_float(row[58]),
                usdLoanFeesChargedAmount=safe_float(row[59]),
                tzsLoanFeesChargedAmount=safe_float(row[60]),
                orgLoanFeesPaidAmount=safe_float(row[61]),
                usdLoanFeesPaidAmount=safe_float(row[62]),
                tzsLoanFeesPaidAmount=safe_float(row[63]),
                orgTotMonthlyPaymentAmount=safe_float(row[64]) or 0.0,
                usdTotMonthlyPaymentAmount=safe_float(row[65]) or 0.0,
                tzsTotMonthlyPaymentAmount=safe_float(row[66]) or 0.0,
                orgInterestPaidTotal=safe_float(row[67]),
                usdInterestPaidTotal=safe_float(row[68]),
                tzsInterestPaidTotal=safe_float(row[69]),
                assetClassificationCategory=safe_string(row[70]),
                sectorSnaClassification=safe_string(row[71]),
                negStatusContract=safe_string(row[72]),
                customerRole=safe_string(row[73]),
                allowanceProbableLoss=safe_int(row[74]),
                botProvision=safe_int(row[75])
            )
            
            return record
            
        except Exception as e:
            self.logger.error(f"Error processing overdraft record: {e}")
            self.logger.error(f"Row data: {row}")
            self.logger.error(f"Row length: {len(row)}")
            raise
    
    def validate_record(self, record):
        """Validate overdraft record"""
        try:
            # Basic validation
            if not record.accountNumber:
                self.logger.warning("Missing account number")
                return False
            
            if not record.customerIdentificationNumber:
                self.logger.warning("Missing customer identification number")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error validating overdraft record: {e}")
            return False
    
    def producer_thread(self):
        """Producer thread - reads from DB2 and sends to RabbitMQ with cursor-based pagination"""
        try:
            self.logger.info("Producer thread started")
            
            # Get total count first
            with self.db2_conn.get_connection(log_connection=True) as conn:
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
            last_customer_id = None
            last_account_number = None
            last_progress_report = time.time()
            
            while True:
                batch_start_time = time.time()
                
                # Fetch batch with retry logic using cursor pagination
                rows = None
                for attempt in range(self.max_retries):
                    try:
                        with self.db2_conn.get_connection(log_connection=False) as conn:
                            cursor = conn.cursor()
                            batch_query = self.get_overdraft_query(last_customer_id, last_account_number)
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
                    # Extract cursor values from the actual data fields
                    last_customer_id = row[2]  # customerIdentificationNumber
                    last_account_number = row[1]  # accountNumber
                    
                    # Process the full row
                    record = self.process_record(row)
                    
                    if self.validate_record(record):
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
        """Consumer thread - processes messages from queue with batch inserts and persistent connection"""
        pg_conn = None
        try:
            self.logger.info("Consumer thread started")
            
            # Setup persistent PostgreSQL connection
            pg_conn = psycopg2.connect(
                host=self.config.database.pg_host,
                port=self.config.database.pg_port,
                database=self.config.database.pg_database,
                user=self.config.database.pg_user,
                password=self.config.database.pg_password
            )
            self.logger.info("Consumer: Persistent PostgreSQL connection established")
            
            # Setup RabbitMQ connection with retry
            connection, channel = self.setup_rabbitmq_connection()
            
            # Batch insert buffer
            insert_buffer: List[OverdraftRecord] = []
            pending_tags: List[int] = []
            last_flush_time = time.time()
            flush_interval = 5  # seconds
            last_progress_report = time.time()
            
            def flush_buffer(ch):
                """Flush buffered records to PostgreSQL in a single batch insert"""
                nonlocal insert_buffer, pending_tags, last_flush_time, pg_conn
                if not insert_buffer:
                    return
                
                batch_size = len(insert_buffer)
                try:
                    self.insert_batch_to_postgres(insert_buffer, pg_conn)
                    
                    # Batch acknowledge all messages at once
                    if pending_tags:
                        ch.basic_ack(delivery_tag=pending_tags[-1], multiple=True)
                    
                    with self._stats_lock:
                        self.total_consumed += batch_size
                    
                    insert_buffer = []
                    pending_tags = []
                    last_flush_time = time.time()
                    
                except psycopg2.OperationalError as e:
                    self.logger.error(f"PostgreSQL connection lost during batch insert: {e}")
                    # Reconnect PostgreSQL
                    try:
                        pg_conn.close()
                    except Exception:
                        pass
                    pg_conn = psycopg2.connect(
                        host=self.config.database.pg_host,
                        port=self.config.database.pg_port,
                        database=self.config.database.pg_database,
                        user=self.config.database.pg_user,
                        password=self.config.database.pg_password
                    )
                    # Nack all to requeue for retry
                    for tag in pending_tags:
                        try:
                            ch.basic_nack(delivery_tag=tag, requeue=True)
                        except Exception:
                            pass
                    insert_buffer = []
                    pending_tags = []
                    last_flush_time = time.time()
                    
                except Exception as e:
                    self.logger.error(f"Batch insert failed: {e}")
                    try:
                        pg_conn.rollback()
                    except Exception:
                        pass
                    # Nack all messages - they go to dead-letter queue
                    for tag in pending_tags:
                        try:
                            ch.basic_nack(delivery_tag=tag, requeue=False)
                        except Exception:
                            pass
                    insert_buffer = []
                    pending_tags = []
                    last_flush_time = time.time()
            
            def process_message(ch, method, properties, body):
                nonlocal insert_buffer, pending_tags, last_progress_report, last_flush_time
                try:
                    record_data = json.loads(body)
                    record = OverdraftRecord(**record_data)
                    
                    insert_buffer.append(record)
                    pending_tags.append(method.delivery_tag)
                    
                    # Flush if buffer is full or time interval exceeded
                    if len(insert_buffer) >= self.consumer_batch_size or \
                       time.time() - last_flush_time >= flush_interval:
                        flush_buffer(ch)
                    
                    # Progress monitoring
                    with self._stats_lock:
                        consumed = self.total_consumed
                    
                    if consumed > 0 and consumed % self.batch_size == 0:
                        elapsed_time = time.time() - self.start_time
                        rate = consumed / elapsed_time if elapsed_time > 0 else 0
                        progress_percent = (consumed / self.total_available * 100) if self.total_available > 0 else 0
                        
                        self.logger.info(f"Consumer: Processed {consumed:,} records ({progress_percent:.2f}% of total) - Rate: {rate:.1f} rec/sec")
                    
                    # Detailed progress report every 5 minutes
                    current_time = time.time()
                    if current_time - last_progress_report >= 300:
                        elapsed_time = current_time - self.start_time
                        with self._stats_lock:
                            consumed = self.total_consumed
                        rate = consumed / elapsed_time if elapsed_time > 0 else 0
                        remaining_records = self.total_available - consumed if self.total_available > 0 else 0
                        eta_seconds = remaining_records / rate if rate > 0 else 0
                        eta_hours = eta_seconds / 3600
                        
                        self.logger.info(f"CONSUMER PROGRESS: {consumed:,}/{self.total_available:,} records - Rate: {rate:.1f} rec/sec - ETA: {eta_hours:.1f} hours")
                        last_progress_report = current_time
                    
                except Exception as e:
                    self.logger.error(f"Consumer error processing message: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            
            # Set QoS to match consumer batch size for efficient batching
            channel.basic_qos(prefetch_count=self.consumer_batch_size)
            channel.basic_consume(queue='overdraft_queue', on_message_callback=process_message)
            
            # Keep consuming until producer is done and queue is empty
            empty_checks = 0
            max_empty_checks = 3  # Check 3 times before stopping
            
            while not self.stop_consumer.is_set():
                try:
                    connection.process_data_events(time_limit=1)
                    
                    # Flush any remaining buffered records on timeout
                    if insert_buffer and time.time() - last_flush_time >= flush_interval:
                        flush_buffer(channel)
                    
                    # Check if we should stop
                    if self.producer_finished.is_set():
                        # Flush remaining buffer before checking queue
                        flush_buffer(channel)
                        
                        # Producer is done, check if queue is empty
                        queue_state = channel.queue_declare(queue='overdraft_queue', durable=True, passive=True)
                        message_count = queue_state.method.message_count
                        
                        if message_count == 0:
                            empty_checks += 1
                            self.logger.info(f"Consumer: Queue appears empty (check {empty_checks}/{max_empty_checks})")
                            
                            if empty_checks >= max_empty_checks:
                                self.logger.info("Consumer: Queue confirmed empty after multiple checks, producer finished")
                                break
                            else:
                                # Wait a bit before checking again
                                time.sleep(2)
                        else:
                            # Reset counter if we find messages
                            empty_checks = 0
                            self.logger.info(f"Consumer: {message_count} messages still in queue, continuing...")
                        
                except Exception as e:
                    self.logger.error(f"Consumer processing error: {e}")
                    # Try to reconnect RabbitMQ
                    try:
                        connection.close()
                    except Exception:
                        pass
                    connection, channel = self.setup_rabbitmq_connection()
                    channel.basic_qos(prefetch_count=self.consumer_batch_size)
                    channel.basic_consume(queue='overdraft_queue', on_message_callback=process_message)
            
            connection.close()
            with self._stats_lock:
                consumed = self.total_consumed
            self.logger.info(f"Consumer finished: {consumed:,} records processed")
            self.consumer_finished.set()
            
        except Exception as e:
            self.logger.error(f"Consumer error: {e}")
            self.consumer_finished.set()
        finally:
            if pg_conn:
                try:
                    pg_conn.close()
                except Exception:
                    pass
    
    
    def insert_batch_to_postgres(self, records: List[OverdraftRecord], pg_conn):
        """Batch insert overdraft records to PostgreSQL"""
        try:
            cursor = pg_conn.cursor()
            
            # Prepare batch data
            values_list = []
            for record in records:
                # Handle JSONB conversion for collateralPledged
                collateral_json = None
                if record.collateralPledged:
                    try:
                        if isinstance(record.collateralPledged, str):
                            parsed = json.loads(record.collateralPledged)
                            collateral_json = json.dumps(parsed)
                        else:
                            collateral_json = json.dumps(record.collateralPledged)
                    except (json.JSONDecodeError, TypeError):
                        collateral_json = json.dumps({"raw_value": str(record.collateralPledged)})
                
                values_list.append((
                    record.reportingDate, record.accountNumber, record.customerIdentificationNumber,
                    record.clientName, record.clientType, record.borrowerCountry, record.ratingStatus,
                    record.crRatingBorrower, record.gradesUnratedBanks, record.groupCode, record.relatedEntityName,
                    record.relatedParty, record.relationshipCategory, record.loanProductType,
                    record.overdraftEconomicActivity, record.loanPhase, record.transferStatus, record.purposeOtherLoans,
                    record.contractDate, record.branchCode, record.loanOfficer, record.loanSupervisor, record.currency,
                    record.orgSanctionedAmount, record.usdSanctionedAmount, record.tzsSanctionedAmount,
                    record.orgUtilisedAmount, record.usdUtilisedAmount, record.tzsUtilisedAmount,
                    record.orgCrUsageLast30DaysAmount, record.usdCrUsageLast30DaysAmount, record.tzsCrUsageLast30DaysAmount,
                    record.disbursementDate, record.expiryDate, record.realEndDate,
                    record.orgOutstandingAmount, record.usdOutstandingAmount, record.tzsOutstandingAmount,
                    record.orgOutstandingPrincipalAmount, record.usdOutstandingPrincipalAmount, record.tzsOutstandingPrincipalAmount,
                    record.latestCustomerCreditDate, record.latestCreditAmount, record.primeLendingRate, record.annualInterestRate,
                    collateral_json, record.restructuredLoans, record.pastDueDays, record.pastDueAmount, record.orgAccruedInterestAmount, record.usdAccruedInterestAmount, record.tzsAccruedInterestAmount,
                    record.orgPenaltyChargedAmount, record.usdPenaltyChargedAmount, record.tzsPenaltyChargedAmount,
                    record.orgPenaltyPaidAmount, record.usdPenaltyPaidAmount, record.tzsPenaltyPaidAmount,
                    record.orgLoanFeesChargedAmount, record.usdLoanFeesChargedAmount, record.tzsLoanFeesChargedAmount,
                    record.orgLoanFeesPaidAmount, record.usdLoanFeesPaidAmount, record.tzsLoanFeesPaidAmount,
                    record.orgTotMonthlyPaymentAmount, record.usdTotMonthlyPaymentAmount, record.tzsTotMonthlyPaymentAmount,
                    record.orgInterestPaidTotal, record.usdInterestPaidTotal, record.tzsInterestPaidTotal,
                    record.assetClassificationCategory, record.sectorSnaClassification, record.negStatusContract,
                    record.customerRole, record.allowanceProbableLoss, record.botProvision
                ))
            
            insert_query = """
            INSERT INTO overdraft (
                "reportingDate", "accountNumber", "customerIdentificationNumber", "clientName",
                "clientType", "borrowerCountry", "ratingStatus", "crRatingBorrower", "gradesUnratedBanks",
                "groupCode", "relatedEntityName", "relatedParty", "relationshipCategory", "loanProductType",
                "overdraftEconomicActivity", "loanPhase", "transferStatus", "purposeOtherLoans",
                "contractDate", "branchCode", "loanOfficer", "loanSupervisor", "currency",
                "orgSanctionedAmount", "usdSanctionedAmount", "tzsSanctionedAmount",
                "orgUtilisedAmount", "usdUtilisedAmount", "tzsUtilisedAmount",
                "orgCrUsageLast30DaysAmount", "usdCrUsageLast30DaysAmount", "tzsCrUsageLast30DaysAmount",
                "disbursementDate", "expiryDate", "realEndDate",
                "orgOutstandingAmount", "usdOutstandingAmount", "tzsOutstandingAmount",
                "orgOutstandingPrincipalAmount", "usdOutstandingPrincipalAmount", "tzsOutstandingPrincipalAmount",
                "latestCustomerCreditDate", "latestCreditAmount", "primeLendingRate", "annualInterestRate",
                "collateralPledged", "restructuredLoans", "pastDueDays", "pastDueAmount", "orgAccruedInterestAmount", "usdAccruedInterestAmount", "tzsAccruedInterestAmount",
                "orgPenaltyChargedAmount", "usdPenaltyChargedAmount", "tzsPenaltyChargedAmount",
                "orgPenaltyPaidAmount", "usdPenaltyPaidAmount", "tzsPenaltyPaidAmount",
                "orgLoanFeesChargedAmount", "usdLoanFeesChargedAmount", "tzsLoanFeesChargedAmount",
                "orgLoanFeesPaidAmount", "usdLoanFeesPaidAmount", "tzsLoanFeesPaidAmount",
                "orgTotMonthlyPaymentAmount", "usdTotMonthlyPaymentAmount", "tzsTotMonthlyPaymentAmount",
                "orgInterestPaidTotal", "usdInterestPaidTotal", "tzsInterestPaidTotal",
                "assetClassificationCategory", "sectorSnaClassification", "negStatusContract",
                "customerRole", "allowanceProbableLoss", "botProvision"
            ) VALUES %s
            """
            
            psycopg2.extras.execute_values(cursor, insert_query, values_list, page_size=len(values_list))
            pg_conn.commit()
            
        except Exception as e:
            self.logger.error(f"Error batch inserting {len(records)} overdraft records: {e}")
            raise
    
    def insert_to_postgres(self, record, cursor):
        """Insert overdraft record to PostgreSQL"""
        try:
            # Handle JSONB conversion for collateralPledged
            collateral_json = None
            if record.collateralPledged:
                try:
                    # If it's already a valid JSON string, parse and re-serialize to ensure validity
                    import json
                    if isinstance(record.collateralPledged, str):
                        # Try to parse as JSON to validate
                        parsed = json.loads(record.collateralPledged)
                        collateral_json = json.dumps(parsed)
                    else:
                        collateral_json = json.dumps(record.collateralPledged)
                except (json.JSONDecodeError, TypeError):
                    # If not valid JSON, store as simple string in JSON format
                    collateral_json = json.dumps({"raw_value": str(record.collateralPledged)})
            
            insert_query = """
            INSERT INTO overdraft (
                "reportingDate", "accountNumber", "customerIdentificationNumber", "clientName",
                "clientType", "borrowerCountry", "ratingStatus", "crRatingBorrower", "gradesUnratedBanks",
                "groupCode", "relatedEntityName", "relatedParty", "relationshipCategory", "loanProductType",
                "overdraftEconomicActivity", "loanPhase", "transferStatus", "purposeOtherLoans",
                "contractDate", "branchCode", "loanOfficer", "loanSupervisor", "currency",
                "orgSanctionedAmount", "usdSanctionedAmount", "tzsSanctionedAmount",
                "orgUtilisedAmount", "usdUtilisedAmount", "tzsUtilisedAmount",
                "orgCrUsageLast30DaysAmount", "usdCrUsageLast30DaysAmount", "tzsCrUsageLast30DaysAmount",
                "disbursementDate", "expiryDate", "realEndDate",
                "orgOutstandingAmount", "usdOutstandingAmount", "tzsOutstandingAmount",
                "orgOutstandingPrincipalAmount", "usdOutstandingPrincipalAmount", "tzsOutstandingPrincipalAmount",
                "latestCustomerCreditDate", "latestCreditAmount", "primeLendingRate", "annualInterestRate",
                "collateralPledged", "restructuredLoans", "pastDueDays", "pastDueAmount", "orgAccruedInterestAmount", "usdAccruedInterestAmount", "tzsAccruedInterestAmount",
                "orgPenaltyChargedAmount", "usdPenaltyChargedAmount", "tzsPenaltyChargedAmount",
                "orgPenaltyPaidAmount", "usdPenaltyPaidAmount", "tzsPenaltyPaidAmount",
                "orgLoanFeesChargedAmount", "usdLoanFeesChargedAmount", "tzsLoanFeesChargedAmount",
                "orgLoanFeesPaidAmount", "usdLoanFeesPaidAmount", "tzsLoanFeesPaidAmount",
                "orgTotMonthlyPaymentAmount", "usdTotMonthlyPaymentAmount", "tzsTotMonthlyPaymentAmount",
                "orgInterestPaidTotal", "usdInterestPaidTotal", "tzsInterestPaidTotal",
                "assetClassificationCategory", "sectorSnaClassification", "negStatusContract",
                "customerRole", "allowanceProbableLoss", "botProvision"
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s
            )
            """
            
            cursor.execute(insert_query, (
                record.reportingDate, record.accountNumber, record.customerIdentificationNumber,
                record.clientName, record.clientType, record.borrowerCountry, record.ratingStatus,
                record.crRatingBorrower, record.gradesUnratedBanks, record.groupCode, record.relatedEntityName,
                record.relatedParty, record.relationshipCategory, record.loanProductType,
                record.overdraftEconomicActivity, record.loanPhase, record.transferStatus, record.purposeOtherLoans,
                record.contractDate, record.branchCode, record.loanOfficer, record.loanSupervisor, record.currency,
                record.orgSanctionedAmount, record.usdSanctionedAmount, record.tzsSanctionedAmount,
                record.orgUtilisedAmount, record.usdUtilisedAmount, record.tzsUtilisedAmount,
                record.orgCrUsageLast30DaysAmount, record.usdCrUsageLast30DaysAmount, record.tzsCrUsageLast30DaysAmount,
                record.disbursementDate, record.expiryDate, record.realEndDate,
                record.orgOutstandingAmount, record.usdOutstandingAmount, record.tzsOutstandingAmount,
                record.orgOutstandingPrincipalAmount, record.usdOutstandingPrincipalAmount, record.tzsOutstandingPrincipalAmount,
                record.latestCustomerCreditDate, record.latestCreditAmount, record.primeLendingRate, record.annualInterestRate,
                collateral_json, record.restructuredLoans, record.pastDueDays, record.pastDueAmount, record.orgAccruedInterestAmount, record.usdAccruedInterestAmount, record.tzsAccruedInterestAmount,
                record.orgPenaltyChargedAmount, record.usdPenaltyChargedAmount, record.tzsPenaltyChargedAmount,
                record.orgPenaltyPaidAmount, record.usdPenaltyPaidAmount, record.tzsPenaltyPaidAmount,
                record.orgLoanFeesChargedAmount, record.usdLoanFeesChargedAmount, record.tzsLoanFeesChargedAmount,
                record.orgLoanFeesPaidAmount, record.usdLoanFeesPaidAmount, record.tzsLoanFeesPaidAmount,
                record.orgTotMonthlyPaymentAmount, record.usdTotMonthlyPaymentAmount, record.tzsTotMonthlyPaymentAmount,
                record.orgInterestPaidTotal, record.usdInterestPaidTotal, record.tzsInterestPaidTotal,
                record.assetClassificationCategory, record.sectorSnaClassification, record.negStatusContract,
                record.customerRole, record.allowanceProbableLoss, record.botProvision
            ))
            
        except Exception as e:
            self.logger.error(f"Error inserting overdraft record to PostgreSQL: {e}")
            raise
    
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