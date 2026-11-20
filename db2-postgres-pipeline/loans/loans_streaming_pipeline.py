#!/usr/bin/env python3
"""
Loans Streaming Pipeline - Producer and Consumer run simultaneously
Based on loan-information-v10.sql query
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
from decimal import Decimal
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config
from db2_connection import DB2Connection

# Configure logging at module level
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


@dataclass
class LoanRecord:
    """Data class for loan records based on loan-information-v10.sql"""
    reportingDate: str
    customerIdentificationNumber: str
    accountNumber: Optional[str]
    clientName: str
    borrowerCountry: str
    ratingStatus: str
    crRatingBorrower: Optional[str]
    gradesUnratedBanks: str
    borrowerCategory: str
    gender: str
    disability: str
    clientType: str
    clientSubType: Optional[str]
    groupName: Optional[str]
    groupCode: Optional[str]
    relatedParty: str
    relationshipCategory: str
    loanNumber: str
    loanType: str
    loanEconomicActivity: str
    loanPhase: str
    transferStatus: str
    purposeMortgage: Optional[str]
    purposeOtherLoans: Optional[str]
    sourceFundMortgage: str
    amortizationType: str
    branchCode: str
    loanOfficer: str
    loanSupervisor: str
    groupVillageNumber: Optional[str]
    cycleNumber: Optional[int]
    loanInstallment: int
    repaymentFrequency: str
    currency: str
    contractDate: str
    orgSanctionAmount: str
    usdSanctionAmount: Optional[str]
    tzsSanctionAmount: str
    orgDisbursedAmount: str
    usdDisbursedAmount: Optional[str]
    tzsDisbursedAmount: str
    disbursementDate: str
    maturityDate: str
    realEndDate: Optional[str]
    orgOutstandingPrincipalAmount: str
    usdOutstandingPrincipalAmount: Optional[str]
    tzsOutstandingPrincipalAmount: str
    orgInstallmentAmount: str
    usdInstallmentAmount: Optional[str]
    tzsInstallmentAmount: str
    loanInstallmentPaid: int
    gracePeriodPaymentPrincipal: Optional[str]
    primeLendingRate: str
    interestPricingMethod: str
    annualInterestRate: str
    effectiveAnnualInterestRate: Optional[str]
    firstInstallmentPaymentDate: str
    lastPaymentDate: str
    collateralPledged: Optional[str]
    loanFlagType: Optional[str]
    restructuringDate: Optional[str]
    pastDueDays: int
    pastDueAmount: str
    internalRiskGroup: str
    orgAccruedInterestAmount: str
    usdAccruedInterestAmount: Optional[str]
    tzsAccruedInterestAmount: str
    orgPenaltyChargedAmount: str
    usdPenaltyChargedAmount: Optional[str]
    tzsPenaltyChargedAmount: str
    orgPenaltyPaidAmount: Optional[str]
    usdPenaltyPaidAmount: Optional[str]
    tzsPenaltyPaidAmount: Optional[str]
    orgLoanFeesChargedAmount: str
    usdLoanFeesChargedAmount: Optional[str]
    tzsLoanFeesChargedAmount: str
    orgLoanFeesPaidAmount: str
    usdLoanFeesPaidAmount: Optional[str]
    tzsLoanFeesPaidAmount: str
    orgTotMonthlyPaymentAmount: str
    usdTotMonthlyPaymentAmount: Optional[str]
    tzsTotMonthlyPaymentAmount: str
    sectorSnaClassification: str
    assetClassificationCategory: str
    negStatusContract: str
    customerRole: str
    allowanceProbableLoss: str
    botProvision: str
    tradingIntent: str
    orgSuspendedInterest: str
    usdSuspendedInterest: Optional[str]
    tzsSuspendedInterest: str
    tmstamp: str


class LoansStreamingPipeline:
    def __init__(self, batch_size=1000):
        self.config = Config()
        self.db2_conn = DB2Connection()
        self.batch_size = batch_size
        self.consumer_batch_size = 100  # Fixed value
        
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
        
        self.logger = logging.getLogger(__name__)
        
        self.logger.info("Loans STREAMING Pipeline initialized")
        self.logger.info(f"Batch size: {self.batch_size} records per batch")
        self.logger.info(f"Consumer batch size: {self.consumer_batch_size} records per flush")
        self.logger.info("Mode: Streaming (Producer + Consumer simultaneously)")
        self.logger.info(f"Retry settings: {self.max_retries} retries with {self.retry_delay}s delay")
    
    def get_loans_query(self, last_timestamp=None):
        """Get the loans query from loan-information-v10.sql with optional TMSTAMP filter"""
        sql_file_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            'sqls', 'loan-information-v10.sql'
        )
        
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql = f.read().strip()
        
        if last_timestamp:
            if last_timestamp == 'epoch':
                timestamp_value = '1900-01-01-00.00.00.000000'
            else:
                timestamp_value = last_timestamp.strftime('%Y-%m-%d-%H.%M.%S.%f')[:-3]
            sql = sql.replace('{timestamp_filter}', f"WHERE la.TMSTAMP > '{timestamp_value}'")
        else:
            sql = sql.replace('{timestamp_filter}', '')
        
        return sql
    
    def get_last_successful_run(self):
        """Get last successful run timestamp from state manager"""
        try:
            from pipeline_state import PipelineStateManager
            state_manager = PipelineStateManager()
            return state_manager.get_last_successful_run('loans')
        except Exception as e:
            self.logger.warning(f"Could not get last successful run: {e}")
            return None
    
    def get_total_count(self, last_timestamp=None):
        """Get approximate total count of loan records from DB2"""
        try:
            with self.db2_conn.get_connection(log_connection=False) as conn:
                cursor = conn.cursor()
                
                # Simple fast count from base table
                if last_timestamp:
                    if last_timestamp == 'epoch':
                        timestamp_value = '1900-01-01-00.00.00.000000'
                    else:
                        timestamp_value = last_timestamp.strftime('%Y-%m-%d-%H.%M.%S.%f')[:-3]
                    cursor.execute(f"SELECT COUNT(*) FROM LOAN_ACCOUNT WHERE TMSTAMP > '{timestamp_value}'")
                else:
                    cursor.execute("SELECT COUNT(*) FROM LOAN_ACCOUNT")
                
                result = cursor.fetchone()
                count = result[0] if result else 0
                self.logger.info(f"Estimated record count from LOAN_ACCOUNT: {count:,}")
                return count
        except Exception as e:
            self.logger.warning(f"Could not fetch record count, progress % unavailable: {e}")
            return 0
    
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
        """Setup RabbitMQ queue for loans with dead-letter exchange"""
        try:
            connection, channel = self.setup_rabbitmq_connection()
            
            # Declare dead-letter exchange and queue for failed messages
            channel.exchange_declare(exchange='loans_dlx', exchange_type='direct', durable=True)
            channel.queue_declare(queue='loans_dead_letter', durable=True)
            channel.queue_bind(
                queue='loans_dead_letter',
                exchange='loans_dlx',
                routing_key='loans_queue'
            )
            
            # Declare main queue with dead-letter exchange routing
            try:
                channel.queue_declare(
                    queue='loans_queue',
                    durable=True,
                    arguments={
                        'x-dead-letter-exchange': 'loans_dlx',
                        'x-dead-letter-routing-key': 'loans_queue'
                    }
                )
                self.logger.info("RabbitMQ queues setup complete (main + dead-letter)")
            except Exception:
                # Queue may already exist with different arguments
                self.logger.warning(
                    "Queue 'loans_queue' already exists with different args. "
                    "Delete and recreate it to enable dead-letter support."
                )
                connection, channel = self.setup_rabbitmq_connection()
                channel.queue_declare(queue='loans_queue', durable=True)
                self.logger.info("RabbitMQ queue 'loans_queue' setup complete (without DLX)")
            
            connection.close()
            
        except Exception as e:
            self.logger.error(f"Failed to setup RabbitMQ: {e}")
            raise
    
    def process_record(self, row):
        """Process a single loan record from DB2"""
        try:
            # Helper function to safely convert values
            def safe_string(value):
                """Safely convert to string"""
                if value is None:
                    return None
                if isinstance(value, Decimal):
                    return format(value, '.2f')
                return str(value).strip()
            
            def safe_int(value):
                """Safely convert to int"""
                if value is None:
                    return None
                try:
                    return int(value)
                except (ValueError, TypeError):
                    return None
            
            # Map all 93 fields from the SQL query to the dataclass
            record = LoanRecord(
                reportingDate=safe_string(row[0]),
                customerIdentificationNumber=safe_string(row[1]),
                accountNumber=safe_string(row[2]) if row[2] else None,
                clientName=safe_string(row[3]),
                borrowerCountry=safe_string(row[4]),
                ratingStatus=safe_string(row[5]),
                crRatingBorrower=safe_string(row[6]) if row[6] else None,
                gradesUnratedBanks=safe_string(row[7]),
                borrowerCategory=safe_string(row[8]),
                gender=safe_string(row[9]),
                disability=safe_string(row[10]),
                clientType=safe_string(row[11]),
                clientSubType=safe_string(row[12]) if row[12] else None,
                groupName=safe_string(row[13]) if row[13] else None,
                groupCode=safe_string(row[14]) if row[14] else None,
                relatedParty=safe_string(row[15]),
                relationshipCategory=safe_string(row[16]),
                loanNumber=safe_string(row[17]),
                loanType=safe_string(row[18]),
                loanEconomicActivity=safe_string(row[19]),
                loanPhase=safe_string(row[20]),
                transferStatus=safe_string(row[21]),
                purposeMortgage=safe_string(row[22]) if row[22] else None,
                purposeOtherLoans=safe_string(row[23]) if row[23] else None,
                sourceFundMortgage=safe_string(row[24]),
                amortizationType=safe_string(row[25]),
                branchCode=safe_string(row[26]),
                loanOfficer=safe_string(row[27]),
                loanSupervisor=safe_string(row[28]),
                groupVillageNumber=safe_string(row[29]) if row[29] else None,
                cycleNumber=safe_int(row[30]),
                loanInstallment=safe_int(row[31]),
                repaymentFrequency=safe_string(row[32]),
                currency=safe_string(row[33]),
                contractDate=safe_string(row[34]),
                orgSanctionAmount=safe_string(row[35]),
                usdSanctionAmount=safe_string(row[36]) if row[36] else None,
                tzsSanctionAmount=safe_string(row[37]),
                orgDisbursedAmount=safe_string(row[38]),
                usdDisbursedAmount=safe_string(row[39]) if row[39] else None,
                tzsDisbursedAmount=safe_string(row[40]),
                disbursementDate=safe_string(row[41]),
                maturityDate=safe_string(row[42]),
                realEndDate=safe_string(row[43]) if row[43] else None,
                orgOutstandingPrincipalAmount=safe_string(row[44]),
                usdOutstandingPrincipalAmount=safe_string(row[45]) if row[45] else None,
                tzsOutstandingPrincipalAmount=safe_string(row[46]),
                orgInstallmentAmount=safe_string(row[47]),
                usdInstallmentAmount=safe_string(row[48]) if row[48] else None,
                tzsInstallmentAmount=safe_string(row[49]),
                loanInstallmentPaid=safe_int(row[50]),
                gracePeriodPaymentPrincipal=safe_string(row[51]) if row[51] else None,
                primeLendingRate=safe_string(row[52]),
                interestPricingMethod=safe_string(row[53]),
                annualInterestRate=safe_string(row[54]),
                effectiveAnnualInterestRate=safe_string(row[55]) if row[55] else None,
                firstInstallmentPaymentDate=safe_string(row[56]),
                lastPaymentDate=safe_string(row[57]),
                collateralPledged=safe_string(row[58]) if row[58] else None,
                loanFlagType=safe_string(row[59]) if row[59] else None,
                restructuringDate=safe_string(row[60]) if row[60] else None,
                pastDueDays=safe_int(row[61]),
                pastDueAmount=safe_string(row[62]),
                internalRiskGroup=safe_string(row[63]),
                orgAccruedInterestAmount=safe_string(row[64]),
                usdAccruedInterestAmount=safe_string(row[65]) if row[65] else None,
                tzsAccruedInterestAmount=safe_string(row[66]),
                orgPenaltyChargedAmount=safe_string(row[67]),
                usdPenaltyChargedAmount=safe_string(row[68]) if row[68] else None,
                tzsPenaltyChargedAmount=safe_string(row[69]),
                orgPenaltyPaidAmount=safe_string(row[70]),
                usdPenaltyPaidAmount=safe_string(row[71]) if row[71] else None,
                tzsPenaltyPaidAmount=safe_string(row[72]),
                orgLoanFeesChargedAmount=safe_string(row[73]),
                usdLoanFeesChargedAmount=safe_string(row[74]) if row[74] else None,
                tzsLoanFeesChargedAmount=safe_string(row[75]),
                orgLoanFeesPaidAmount=safe_string(row[76]),
                usdLoanFeesPaidAmount=safe_string(row[77]) if row[77] else None,
                tzsLoanFeesPaidAmount=safe_string(row[78]),
                orgTotMonthlyPaymentAmount=safe_string(row[79]),
                usdTotMonthlyPaymentAmount=safe_string(row[80]) if row[80] else None,
                tzsTotMonthlyPaymentAmount=safe_string(row[81]),
                sectorSnaClassification=safe_string(row[82]),
                assetClassificationCategory=safe_string(row[83]),
                negStatusContract=safe_string(row[84]),
                customerRole=safe_string(row[85]),
                allowanceProbableLoss=safe_string(row[86]),
                botProvision=safe_string(row[87]),
                tradingIntent=safe_string(row[88]),
                orgSuspendedInterest=safe_string(row[89]),
                usdSuspendedInterest=safe_string(row[90]) if row[90] else None,
                tzsSuspendedInterest=safe_string(row[91]),
                tmstamp=safe_string(row[92])
            )
            
            return record
            
        except Exception as e:
            self.logger.error(f"Error processing loan record: {e}")
            self.logger.error(f"Row data: {row}")
            self.logger.error(f"Row length: {len(row)}")
            raise
    
    def validate_record(self, record):
        """Validate loan record"""
        try:
            mandatory_fields = [
                'reportingDate', 'customerIdentificationNumber', 'accountNumber',
                'clientName', 'borrowerCountry', 'ratingStatus', 'gradesUnratedBanks',
                'borrowerCategory', 'gender', 'disability', 'clientType',
                'relatedParty', 'relationshipCategory', 'loanNumber', 'loanType',
                'loanEconomicActivity', 'loanPhase', 'transferStatus',
                'sourceFundMortgage', 'amortizationType', 'branchCode', 'loanOfficer',
                'loanSupervisor', 'repaymentFrequency', 'currency', 'contractDate',
                'orgSanctionAmount', 'tzsSanctionAmount', 'orgDisbursedAmount',
                'tzsDisbursedAmount', 'disbursementDate', 'maturityDate',
                'orgOutstandingPrincipalAmount', 'tzsOutstandingPrincipalAmount',
                'orgInstallmentAmount', 'tzsInstallmentAmount', 'loanInstallmentPaid',
                'primeLendingRate', 'interestPricingMethod', 'annualInterestRate',
                'firstInstallmentPaymentDate', 'lastPaymentDate', 'loanFlagType',
                'pastDueDays', 'pastDueAmount', 'internalRiskGroup',
                'orgAccruedInterestAmount', 'tzsAccruedInterestAmount',
                'orgPenaltyChargedAmount', 'tzsPenaltyChargedAmount',
                'orgPenaltyPaidAmount', 'tzsPenaltyPaidAmount',
                'orgLoanFeesChargedAmount', 'tzsLoanFeesChargedAmount',
                'orgLoanFeesPaidAmount', 'tzsLoanFeesPaidAmount',
                'orgTotMonthlyPaymentAmount', 'sectorSnaClassification',
                'assetClassificationCategory', 'negStatusContract', 'customerRole',
                'allowanceProbableLoss', 'botProvision', 'tradingIntent',
                'orgSuspendedInterest', 'tzsSuspendedInterest',
            ]

            hard_failures = {'loanNumber', 'customerIdentificationNumber'}
            is_valid = True

            for field_name in mandatory_fields:
                value = getattr(record, field_name, None)
                if value is None or (isinstance(value, str) and not value.strip()):
                    self.logger.warning(f"Missing mandatory field: {field_name}")
                    if field_name in hard_failures:
                        is_valid = False

            return is_valid

        except Exception as e:
            self.logger.error(f"Error validating loan record: {e}")
            return False

    
    def producer_thread(self, incremental=True):
        """Producer thread - executes query ONCE and streams results via fetchmany()"""
        try:
            self.logger.info("Producer thread started")
            
            last_timestamp = None
            if incremental:
                last_timestamp = self.get_last_successful_run()
                if last_timestamp:
                    self.logger.info(f"Incremental mode: fetching records with TMSTAMP > {last_timestamp}")
                else:
                    last_timestamp = 'epoch'
                    self.logger.info("Incremental mode: first run, fetching all records (epoch)")
            else:
                self.logger.info("Full load mode: fetching all records")
            
            # Get dynamic record count with same filters as main query
            self.total_available = self.get_total_count(last_timestamp)
            
            self.logger.info(f"Total loan records available: {self.total_available:,} (estimated)")
            estimated_batches = (self.total_available + self.batch_size - 1) // self.batch_size
            self.logger.info(f"Estimated batches to process: {estimated_batches:,}")
            
            # Setup RabbitMQ connection with retry
            rmq_connection, channel = self.setup_rabbitmq_connection()
            
            # Execute the query ONCE and stream results
            query = self.get_loans_query(last_timestamp)
            self.logger.info("Executing loans query (single execution, streaming results)...")
            
            with self.db2_conn.get_connection(log_connection=True) as db2_conn:
                db2_cursor = db2_conn.cursor()
                db2_cursor.execute(query)
                self.logger.info("Query executed successfully, streaming results via fetchmany()...")
                
                batch_number = 1
                last_progress_report = time.time()
                
                while True:
                    batch_start_time = time.time()
                    
                    # Fetch next chunk from the already-running query
                    rows = db2_cursor.fetchmany(self.batch_size)
                    
                    if not rows:
                        self.logger.info("No more records to fetch")
                        break
                    
                    # Process and publish
                    batch_published = 0
                    for row in rows:
                        record = self.process_record(row)
                        
                        if self.validate_record(record):
                            message = json.dumps(asdict(record), default=str)
                            
                            # Publish with retry
                            published = False
                            for attempt in range(self.max_retries):
                                try:
                                    channel.basic_publish(
                                        exchange='',
                                        routing_key='loans_queue',
                                        body=message,
                                        properties=pika.BasicProperties(delivery_mode=2)
                                    )
                                    published = True
                                    break
                                except Exception as e:
                                    self.logger.warning(f"RabbitMQ publish attempt {attempt + 1} failed: {e}")
                                    if attempt < self.max_retries - 1:
                                        try:
                                            rmq_connection.close()
                                        except Exception:
                                            pass
                                        rmq_connection, channel = self.setup_rabbitmq_connection()
                                        time.sleep(self.retry_delay)
                                    else:
                                        self.logger.error(f"Failed to publish message after {self.max_retries} attempts")
                            
                            if published:
                                batch_published += 1
                                with self._stats_lock:
                                    self.total_produced += 1
                    
                    batch_time = time.time() - batch_start_time
                    with self._stats_lock:
                        produced = self.total_produced
                    progress_percent = produced / self.total_available * 100 if self.total_available > 0 else 0
                    
                    self.logger.info(f"Producer: Batch {batch_number:,} - {len(rows)} rows fetched, {batch_published} published ({progress_percent:.2f}% complete, {batch_time:.1f}s)")
                    
                    # Progress report every 5 minutes
                    current_time = time.time()
                    if current_time - last_progress_report >= 300:
                        elapsed_time = current_time - self.start_time
                        rate = produced / elapsed_time if elapsed_time > 0 else 0
                        remaining_records = self.total_available - produced
                        eta_seconds = remaining_records / rate if rate > 0 else 0
                        eta_hours = eta_seconds / 3600
                        
                        self.logger.info(f"PROGRESS REPORT: {produced:,}/{self.total_available:,} records ({progress_percent:.1f}%) - Rate: {rate:.1f} rec/sec - ETA: {eta_hours:.1f} hours")
                        last_progress_report = current_time
                    
                    batch_number += 1
            
            rmq_connection.close()
            with self._stats_lock:
                produced = self.total_produced
            self.logger.info(f"Producer finished: {produced:,} records published out of {self.total_available:,} available")
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
            insert_buffer: List[LoanRecord] = []
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
                    record = LoanRecord(**record_data)
                    
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
            channel.basic_consume(queue='loans_queue', on_message_callback=process_message)
            
            # Keep consuming until producer is done and queue is empty
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
                        queue_state = channel.queue_declare(queue='loans_queue', durable=True, passive=True)
                        if queue_state.method.message_count == 0:
                            self.logger.info("Consumer: Queue empty, producer finished")
                            break
                        
                except Exception as e:
                    self.logger.error(f"Consumer processing error: {e}")
                    # Try to reconnect RabbitMQ
                    try:
                        connection.close()
                    except Exception:
                        pass
                    connection, channel = self.setup_rabbitmq_connection()
                    channel.basic_qos(prefetch_count=self.consumer_batch_size)
                    channel.basic_consume(queue='loans_queue', on_message_callback=process_message)
            
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
    
    def insert_batch_to_postgres(self, records: List[LoanRecord], pg_conn):
        """Batch insert loan records to PostgreSQL with duplicate prevention"""
        try:
            cursor = pg_conn.cursor()
            
            insert_query = """
            INSERT INTO "loanInformation" (
                "reportingDate", "customerIdentificationNumber", "accountNumber", "clientName",
                "borrowerCountry", "ratingStatus", "crRatingBorrower", "gradesUnratedBanks",
                "borrowerCategory", "gender", "disability", "clientType", "clientSubType",
                "groupName", "groupCode", "relatedParty", "relationshipCategory", "loanNumber",
                "loanType", "loanEconomicActivity", "loanPhase", "transferStatus",
                "purposeMortgage", "purposeOtherLoans", "sourceFundMortgage", "amortizationType",
                "branchCode", "loanOfficer", "loanSupervisor", "groupVillageNumber",
                "cycleNumber", "loanInstallment", "repaymentFrequency", "currency",
                "contractDate", "orgSanctionAmount", "usdSanctionAmount", "tzsSanctionAmount",
                "orgDisbursedAmount", "usdDisbursedAmount", "tzsDisbursedAmount",
                "disbursementDate", "maturityDate", "realEndDate",
                "orgOutstandingPrincipalAmount", "usdOutstandingPrincipalAmount", "tzsOutstandingPrincipalAmount",
                "orgInstallmentAmount", "usdInstallmentAmount", "tzsInstallmentAmount",
                "loanInstallmentPaid", "gracePeriodPaymentPrincipal", "primeLendingRate",
                "interestPricingMethod", "annualInterestRate", "effectiveAnnualInterestRate",
                "firstInstallmentPaymentDate", "lastPaymentDate", "collateralPledged",
                "loanFlagType", "restructuringDate", "pastDueDays", "pastDueAmount",
                "internalRiskGroup", "orgAccruedInterestAmount", "usdAccruedInterestAmount",
                "tzsAccruedInterestAmount", "orgPenaltyChargedAmount", "usdPenaltyChargedAmount",
                "tzsPenaltyChargedAmount", "orgPenaltyPaidAmount", "usdPenaltyPaidAmount",
                "tzsPenaltyPaidAmount", "orgLoanFeesChargedAmount", "usdLoanFeesChargedAmount",
                "tzsLoanFeesChargedAmount", "orgLoanFeesPaidAmount", "usdLoanFeesPaidAmount",
                "tzsLoanFeesPaidAmount", "orgTotMonthlyPaymentAmount", "usdTotMonthlyPaymentAmount",
                "tzsTotMonthlyPaymentAmount", "sectorSnaClassification", "assetClassificationCategory",
                "negStatusContract", "customerRole", "allowanceProbableLoss", "botProvision",
                "tradingIntent", "orgSuspendedInterest", "usdSuspendedInterest", "tzsSuspendedInterest"
            ) VALUES %s
            ON CONFLICT ("loanNumber") DO NOTHING
            """
            
            values = [
                (
                    r.reportingDate, r.customerIdentificationNumber, r.accountNumber, r.clientName,
                    r.borrowerCountry, r.ratingStatus, r.crRatingBorrower, r.gradesUnratedBanks,
                    r.borrowerCategory, r.gender, r.disability, r.clientType, r.clientSubType,
                    r.groupName, r.groupCode, r.relatedParty, r.relationshipCategory, r.loanNumber,
                    r.loanType, r.loanEconomicActivity, r.loanPhase, r.transferStatus,
                    r.purposeMortgage, r.purposeOtherLoans, r.sourceFundMortgage, r.amortizationType,
                    r.branchCode, r.loanOfficer, r.loanSupervisor, r.groupVillageNumber,
                    r.cycleNumber, r.loanInstallment, r.repaymentFrequency, r.currency,
                    r.contractDate, r.orgSanctionAmount, r.usdSanctionAmount, r.tzsSanctionAmount,
                    r.orgDisbursedAmount, r.usdDisbursedAmount, r.tzsDisbursedAmount,
                    r.disbursementDate, r.maturityDate, r.realEndDate,
                    r.orgOutstandingPrincipalAmount, r.usdOutstandingPrincipalAmount, r.tzsOutstandingPrincipalAmount,
                    r.orgInstallmentAmount, r.usdInstallmentAmount, r.tzsInstallmentAmount,
                    r.loanInstallmentPaid, r.gracePeriodPaymentPrincipal, r.primeLendingRate,
                    r.interestPricingMethod, r.annualInterestRate, r.effectiveAnnualInterestRate,
                    r.firstInstallmentPaymentDate, r.lastPaymentDate, r.collateralPledged,
                    r.loanFlagType, r.restructuringDate, r.pastDueDays, r.pastDueAmount,
                    r.internalRiskGroup, r.orgAccruedInterestAmount, r.usdAccruedInterestAmount,
                    r.tzsAccruedInterestAmount, r.orgPenaltyChargedAmount, r.usdPenaltyChargedAmount,
                    r.tzsPenaltyChargedAmount, r.orgPenaltyPaidAmount, r.usdPenaltyPaidAmount,
                    r.tzsPenaltyPaidAmount, r.orgLoanFeesChargedAmount, r.usdLoanFeesChargedAmount,
                    r.tzsLoanFeesChargedAmount, r.orgLoanFeesPaidAmount, r.usdLoanFeesPaidAmount,
                    r.tzsLoanFeesPaidAmount, r.orgTotMonthlyPaymentAmount, r.usdTotMonthlyPaymentAmount,
                    r.tzsTotMonthlyPaymentAmount, r.sectorSnaClassification, r.assetClassificationCategory,
                    r.negStatusContract, r.customerRole, r.allowanceProbableLoss, r.botProvision,
                    r.tradingIntent, r.orgSuspendedInterest, r.usdSuspendedInterest, r.tzsSuspendedInterest
                )
                for r in records
            ]
            
            psycopg2.extras.execute_values(cursor, insert_query, values, page_size=len(values))
            pg_conn.commit()
            
        except Exception as e:
            self.logger.error(f"Error batch inserting {len(records)} loan records: {e}")
            raise
    
    def ensure_unique_index(self):
        """Ensure unique index on loanNumber exists for ON CONFLICT duplicate prevention"""
        try:
            with self.get_postgres_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_loaninformation_loan_number_unique
                    ON "loanInformation" ("loanNumber")
                """)
                conn.commit()
                self.logger.info("Unique index on loanNumber verified/created")
        except Exception as e:
            self.logger.error(f"Failed to create unique index on loanNumber: {e}")
            raise
    
    def run_streaming_pipeline(self, incremental=True):
        """Run the streaming pipeline with simultaneous producer and consumer"""
        self.logger.info(f"Starting Loans STREAMING pipeline... (incremental={incremental})")
        
        try:
            # Update state to running
            self._update_state('running')

            # Ensure unique index for duplicate prevention
            self.ensure_unique_index()
            
            # Setup queue
            self.setup_rabbitmq_queue()
            
            # Start consumer thread first
            consumer_thread = threading.Thread(target=self.consumer_thread, name="Consumer")
            consumer_thread.start()
            
            # Small delay to let consumer start
            time.sleep(1)
            
            # Start producer thread
            producer_thread = threading.Thread(target=self.producer_thread, name="Producer", args=(incremental,))
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
            Loans Pipeline Summary:
            ==========================================
            Mode: {'Incremental' if incremental else 'Full Load'}
            Total available records: {self.total_available:,}
            Records produced: {self.total_produced:,}
            Records consumed: {self.total_consumed:,}
            Success rate: {success_rate:.1f}%
            Total processing time: {total_time/3600:.2f} hours
            Average rate: {avg_rate:.1f} records/second
            ==========================================
            """)
            
            # Update pipeline state
            self._update_state('completed')
            
        except Exception as e:
            self.logger.error(f"Pipeline error: {e}")
            self._update_state('failed', str(e))
            raise
    
    def _update_state(self, status, error_message=None):
        """Update pipeline state in the state table"""
        try:
            from pipeline_state import PipelineStateManager
            state_manager = PipelineStateManager()
            state_manager.update_run(
                'loans',
                status,
                self.total_consumed,
                error_message
            )
            self.logger.info(f"State updated: {status}, records: {self.total_consumed}")
        except Exception as e:
            self.logger.warning(f"Could not update state: {e}")


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Loans Streaming Pipeline')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for processing')
    parser.add_argument('--mode', choices=['producer', 'consumer', 'streaming'], default='streaming',
                       help='Pipeline mode: producer only, consumer only, or full streaming')
    parser.add_argument('--full-load', action='store_true', help='Perform full load instead of incremental')
    
    args = parser.parse_args()
    
    # Create pipeline
    pipeline = LoansStreamingPipeline(batch_size=args.batch_size)
    
    try:
        if args.mode == 'producer':
            pipeline.producer_thread(incremental=not args.full_load)
        elif args.mode == 'consumer':
            pipeline.consumer_thread()
        else:  # streaming
            pipeline.run_streaming_pipeline(incremental=not args.full_load)
            
    except KeyboardInterrupt:
        pipeline.logger.info("Pipeline stopped by user")
    except Exception as e:
        pipeline.logger.error(f"Pipeline failed: {e}")
        import sys
        sys.exit(1)


if __name__ == "__main__":
    main()
