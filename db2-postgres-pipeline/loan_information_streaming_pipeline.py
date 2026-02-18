#!/usr/bin/env python3
"""
Loan Information Streaming Pipeline - Producer and Consumer run simultaneously
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
from processors.loan_information_processor import LoanInformationProcessor, LoanInformationRecord

class LoanInformationStreamingPipeline:
    def __init__(self, batch_size=1000):
        self.config = Config()
        self.db2_conn = DB2Connection()
        self.processor = LoanInformationProcessor()
        self.batch_size = batch_size
        
        # Threading control
        self.producer_finished = threading.Event()
        self.consumer_finished = threading.Event()
        self.stop_consumer = threading.Event()
        
        # Statistics
        self.total_produced = 0
        self.total_consumed = 0
        self.total_inserted = 0
        self.total_available = 0
        self.start_time = time.time()
        
        # Retry settings
        self.max_retries = 3
        self.retry_delay = 2
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        self.logger.info("Loan Information STREAMING Pipeline initialized (OPTIMIZED)")
        self.logger.info(f"Batch size: {self.batch_size} records per batch")
        self.logger.info("Mode: Streaming (Producer + Consumer simultaneously)")
        self.logger.info(f"Retry settings: {self.max_retries} retries with {self.retry_delay}s delay")
    
    def get_loan_information_query(self):
        """Get the exact loan information query from loan-information-v2.sql"""
        
        query = """
        WITH LatestInstallments AS (SELECT *
                                    FROM (SELECT li.*,
                                                 ROW_NUMBER() OVER (PARTITION BY li.ACC_SN ORDER BY li.INSTALL_SN DESC) AS rn
                                          FROM LOAN_INSTALLMENTS li) t
                                    WHERE t.rn = 1),
             ProfitsAccount AS (SELECT CUST_ID,
                                       MIN(ACCOUNT_NUMBER) AS ACCOUNT_NUMBER
                                FROM PROFITS_ACCOUNT
                                GROUP BY CUST_ID),
             OtherID AS (SELECT fk_customercust_id,
                                MAX(ID_NO)              AS ID_NO,
                                MAX(fkgh_has_been_issu) AS fkgh_has_been_issu,
                                MAX(fkgd_has_been_issu) AS fkgd_has_been_issu
                         FROM other_id
                         WHERE COALESCE(main_flag, '1') = '1'
                         GROUP BY fk_customercust_id),
             CollateralAgg AS (SELECT ac.PRFT_ACCOUNT,
                                      '[' || LISTAGG(
                                              '{' ||
                                                  '"collateralPledged":"Cash",' ||
                                                  '"orgCollateralValue":' || COALESCE(ac.EST_VALUE_AMN, 0) || ',' ||
                                                  '"usdCollateralValue":' ||
                                              CAST(COALESCE(ac.EST_VALUE_AMN, 0) / 2500 AS DECIMAL(15, 2)) || ',' ||
                                                  '"tzsCollateralValue":' || COALESCE(ac.EST_VALUE_AMN, 0) ||
                                                  '}',
                                              ','
                                             ) || ']' AS collateralPledged
                               FROM ACCOUNT_COLLATERAL ac
                               GROUP BY ac.PRFT_ACCOUNT),
             MainQuery AS (SELECT CURRENT_TIMESTAMP                                                                 AS reportingDate,
                                  LTRIM(RTRIM(cust.CUST_ID))                                                        AS customerIdentificationNumber,
                                  LTRIM(RTRIM(pa.ACCOUNT_NUMBER))                                                   AS accountNumber,
                                  LTRIM(RTRIM(cust.FIRST_NAME))                                                     AS clientName,
                                  cl.COUNTRY_CODE                                                                   AS borrowerCountry,
                                  'FALSE'                                                                           AS ratingStatus,
                                  NULL                                                                              AS crRatingBorrower,
                                  'Grade B'                                                                         AS gradesUnratedBanks,
                                  lccd.GENDER                                                                       AS gender,
                                  NULL                                                                              AS disability,
                                  ctl.CUSTOMER_TYPE                                                                 AS clientType,
                                  NULL                                                                              AS clientSubType,
                                  NULL                                                                              AS groupName,
                                  NULL                                                                              AS groupCode,
                                  'No relation'                                                                     AS relatedParty,
                                  'Direct'                                                                          AS relationshipCategory,
                                  pa.ACCOUNT_NUMBER                                                                 AS loanNumber,
                                  CASE
                                      WHEN p.DESCRIPTION LIKE '%BUSINESS%' AND p.DESCRIPTION LIKE '%LOAN%' THEN 'Business Loan'
                                      WHEN p.DESCRIPTION LIKE '%INSURANCE%' AND p.DESCRIPTION LIKE '%LOAN%' THEN 'Business Loan'
                                      WHEN p.DESCRIPTION LIKE '%IPF%' AND p.DESCRIPTION LIKE '%LOAN%' THEN 'Business Loan'
                                      WHEN p.DESCRIPTION LIKE '%KILIMO%' AND p.DESCRIPTION LIKE '%LOAN%' THEN 'Business Loan'
                                      WHEN p.DESCRIPTION LIKE '%MICRO%' AND p.DESCRIPTION LIKE '%LOAN%' THEN 'Business Loan'
                                      WHEN p.DESCRIPTION LIKE '%UNSECURED%' AND p.DESCRIPTION LIKE '%LOAN%' THEN 'Business Loan'
                                      WHEN p.DESCRIPTION LIKE '%MORTGAGE%' AND p.DESCRIPTION LIKE '%LOAN%' THEN 'Mortgage Loan'
                                      ELSE 'Personal loan'
                                      END                                                                           AS loanType,
                                  'OtherServices'                                                                   AS loanEconomicActivity,
                                  'Existing'                                                                        AS loanPhase,
                                  'NotSpecified'                                                                    AS transferStatus,
                                  CASE
                                      WHEN p.DESCRIPTION LIKE '%MORTGAGE%' AND p.DESCRIPTION LIKE '%LOAN%' THEN
                                          CASE
                                              WHEN GG.DESCRIPTION LIKE '%Development%' THEN 'Improvement'
                                              WHEN GG.DESCRIPTION LIKE '%Purchase%' THEN 'Acquisition'
                                              WHEN GG.DESCRIPTION LIKE '%Construct%' THEN 'Construction'
                                              ELSE 'Others'
                                              END
                                      END                                                                           AS purposeMortgage,
                                  GG.DESCRIPTION                                                                    AS purposeOtherLoans,
                                  'Others'                                                                          AS sourceFundMortgage,
                                  'Reducing Method'                                                                 AS amortizationType,
                                  la.FK_UNITCODE                                                                    AS branchCode,
                                  'Athanas'                                                                         AS loanOfficer,
                                  NULL                                                                              AS loanSupervisor,
                                  NULL                                                                              AS groupVillageNumber,
                                  (SELECT COUNT(*)
                                   FROM LOAN_ACCOUNT la2
                                   WHERE la2.CUST_ID = la.CUST_ID
                                     AND la2.ACC_OPEN_DT <= la.ACC_OPEN_DT)                                         AS cycleNumber,
                                  la.INSTALL_COUNT                                                                  AS loanInstallment,
                                  CASE
                                      WHEN la.INSTALL_FREQ = 1 THEN 'Monthly'
                                      ELSE 'Monthly'
                                      END                                                                           AS repaymentFrequency,
                                  cu.SHORT_DESCR                                                                    AS currency,
                                  la.ACC_OPEN_DT                                                                    AS contractDate,
                                  la.ACC_LIMIT_AMN                                                                  AS orgSanctionedAmount,
                                  CASE WHEN cu.SHORT_DESCR = 'USD' THEN la.ACC_LIMIT_AMN ELSE NULL END              AS usdSanctionedAmount,
                                  CASE
                                      WHEN cu.SHORT_DESCR = 'USD' THEN la.ACC_LIMIT_AMN * 2500
                                      ELSE la.ACC_LIMIT_AMN END                                                     AS tzsSanctionedAmount,
                                  la.TOT_DRAWDOWN_AMN                                                               AS orgDisbursedAmount,
                                  CASE WHEN cu.SHORT_DESCR = 'USD' THEN la.TOT_DRAWDOWN_AMN ELSE NULL END           AS usdDisbursedAmount,
                                  CASE
                                      WHEN cu.SHORT_DESCR = 'USD' THEN la.TOT_DRAWDOWN_AMN * 2500
                                      ELSE la.TOT_DRAWDOWN_AMN END                                                  AS tzsDisbursedAmount,
                                  la.DRAWDOWN_FST_DT                                                                AS disbursementDate,
                                  la.ACC_EXP_DT                                                                     AS maturityDate,
                                  COALESCE(la.OV_EXP_DT, la.ACC_EXP_DT)                                             AS realEndDate,
                                  (la.NRM_CAP_BAL + la.OV_CAP_BAL)                                                  AS orgOutstandingPrincipalAmount,
                                  CASE
                                      WHEN cu.SHORT_DESCR = 'USD' THEN (la.NRM_CAP_BAL + la.OV_CAP_BAL)
                                      ELSE NULL END                                                                 AS usdOutstandingPrincipalAmount,
                                  CASE
                                      WHEN cu.SHORT_DESCR = 'USD' THEN (la.NRM_CAP_BAL + la.OV_CAP_BAL) * 2500
                                      ELSE (la.NRM_CAP_BAL + la.OV_CAP_BAL) END                                     AS tzsOutstandingPrincipalAmount,
                                  li.INSTALL_AMN                                                                    AS orgInstallmentAmount,
                                  CASE WHEN cu.SHORT_DESCR = 'USD' THEN li.INSTALL_AMN ELSE NULL END                AS usdInstallmentAmount,
                                  CASE
                                      WHEN cu.SHORT_DESCR = 'USD' THEN li.INSTALL_AMN * 2500
                                      ELSE li.INSTALL_AMN END                                                       AS tzsInstallmentAmount,
                                  la.NRM_INST_CNT                                                                   AS loanInstallmentPaid,
                                  NULL                                                                              AS gracePeriodPaymentPrincipal,
                                  la.MORATOR_NRM_RATE                                                               AS primeLendingRate,
                                  NULL                                                                              AS interestPricingMethod,
                                  la.TOT_NRM_INT_AMN                                                                AS annualInterestRate,
                                  NULL                                                                              AS effectiveAnnualInterestRate,
                                  la.INSTALL_FIRST_DT                                                               AS firstInstallmentPaymentDate,
                                  li.RQ_LST_PAYMENT_DT                                                              AS lastPaymentDate,
                                  ca.collateralPledged,
                                  'Restructured'                                                                    AS loanFlagType,
                                  NULL                                                                              AS restructuringDate,
                                  li.RQ_OVERDUE_DAYS                                                                AS pastDueDays,
                                  la.OV_CAP_BAL                                                                     AS pastDueAmount,
                                  la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL                AS orgAccruedInterestAmount,
                                  CASE
                                      WHEN cu.SHORT_DESCR = 'USD'
                                          THEN (la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL)
                                      ELSE NULL END                                                                 AS usdAccruedInterestAmount,
                                  CASE
                                      WHEN cu.SHORT_DESCR = 'USD' THEN
                                          (la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL) * 2500
                                      ELSE (la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL) END AS tzsAccruedInterestAmount,
                                  la.OV_RL_PNL_INT_BAL + la.OV_URL_PNL_INT_BAL                                      AS orgPenaltyChargedAmount,
                                  CASE
                                      WHEN cu.SHORT_DESCR = 'USD' THEN (la.OV_RL_PNL_INT_BAL + la.OV_URL_PNL_INT_BAL)
                                      ELSE NULL END                                                                 AS usdPenaltyChargedAmount,
                                  CASE
                                      WHEN cu.SHORT_DESCR = 'USD' THEN (la.OV_RL_PNL_INT_BAL + la.OV_URL_PNL_INT_BAL) * 2500
                                      ELSE (la.OV_RL_PNL_INT_BAL + la.OV_URL_PNL_INT_BAL) END                       AS tzsPenaltyChargedAmount,
                                  COALESCE(la.TOT_PNL_INT_AMN, 0)                                                   AS orgPenaltyPaidAmount,
                                  CASE
                                      WHEN cu.SHORT_DESCR = 'USD' THEN COALESCE(la.TOT_PNL_INT_AMN, 0)
                                      ELSE NULL END                                                                 AS usdPenaltyPaidAmount,
                                  CASE
                                      WHEN cu.SHORT_DESCR = 'USD' THEN COALESCE(la.TOT_PNL_INT_AMN, 0) * 2500
                                      ELSE COALESCE(la.TOT_PNL_INT_AMN, 0) END                                      AS tzsPenaltyPaidAmount,
                                  la.TOT_COMMISSION_AMN                                                             AS orgLoanFeesChargedAmount,
                                  CASE WHEN cu.SHORT_DESCR = 'USD' THEN la.TOT_COMMISSION_AMN ELSE NULL END         AS usdLoanFeesChargedAmount,
                                  CASE
                                      WHEN cu.SHORT_DESCR = 'USD' THEN la.TOT_COMMISSION_AMN * 2500
                                      ELSE la.TOT_COMMISSION_AMN END                                                AS tzsLoanFeesChargedAmount,
                                  la.TOT_EXPENSE_AMN                                                                AS orgLoanFeesPaidAmount,
                                  CASE WHEN cu.SHORT_DESCR = 'USD' THEN la.TOT_EXPENSE_AMN ELSE NULL END            AS usdLoanFeesPaidAmount,
                                  CASE
                                      WHEN cu.SHORT_DESCR = 'USD' THEN la.TOT_EXPENSE_AMN * 2500
                                      ELSE la.TOT_EXPENSE_AMN END                                                   AS tzsLoanFeesPaidAmount,
                                  li.INSTALL_AMN                                                                    AS orgTotMonthlyPaymentAmount,
                                  CASE WHEN cu.SHORT_DESCR = 'USD' THEN li.INSTALL_AMN ELSE NULL END                AS usdTotMonthlyPaymentAmount,
                                  CASE
                                      WHEN cu.SHORT_DESCR = 'USD' THEN li.INSTALL_AMN * 2500
                                      ELSE li.INSTALL_AMN END                                                       AS tzsTotMonthlyPaymentAmount,
                                  'Households'                                                                      AS sectorSnaClassification,
                                  'Current'                                                                         AS assetClassificationCategory,
                                  la.ACC_STATUS                                                                     AS negStatusContract,
                                  la.DEP_ACC_TYPE                                                                   AS customerRole,
                                  lai.PROVISION_AMOUNT                                                              AS allowanceProbableLoss,
                                  lai.PROVISION_AMOUNT                                                              AS botProvision,
                                  'Held to Maturity'                                                                AS tradingIntent,
                                  la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL                                     AS orgSuspendedInterest,
                                  CASE
                                      WHEN cu.SHORT_DESCR = 'USD' THEN (la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL)
                                      ELSE NULL END                                                                 AS usdSuspendedInterest,
                                  CASE
                                      WHEN cu.SHORT_DESCR = 'USD' THEN (la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL) * 2500
                                      ELSE (la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL) END                      AS tzsSuspendedInterest
                           FROM LOAN_ACCOUNT la
                                    LEFT JOIN LatestInstallments li ON la.ACC_SN = li.ACC_SN
                                    LEFT JOIN ProfitsAccount pa ON pa.CUST_ID = la.CUST_ID
                                    LEFT JOIN CollateralAgg ca ON ca.PRFT_ACCOUNT = pa.ACCOUNT_NUMBER
                                    LEFT JOIN LOAN_ACCOUNT_INFO lai ON la.FK_UNITCODE = lai.FK_LOAN_ACCOUNTFK
                               AND la.ACC_TYPE = lai.FK0LOAN_ACCOUNTACC
                               AND la.ACC_SN = lai.FK_LOAN_ACCOUNTACC
                                    LEFT JOIN CUSTOMER cust ON la.CUST_ID = cust.CUST_ID
                                    LEFT JOIN OtherID id ON id.fk_customercust_id = cust.CUST_ID
                                    LEFT JOIN LNS_CRD_CUST_DATA lccd ON lccd.CUST_ID = la.CUST_ID
                                    LEFT JOIN CUSTOMER_TYPES_LOOKUP ctl ON ctl.CUSTOMER_TYPE_CODE = cust.CUST_TYPE
                                    LEFT JOIN CURRENCY cu ON cu.ID_CURRENCY = la.FKCUR_IS_CHARGED
                                    LEFT JOIN GENERIC_DETAIL id_country ON id.fkgh_has_been_issu = id_country.fk_generic_headpar
                               AND id.fkgd_has_been_issu = id_country.serial_num
                                    LEFT JOIN COUNTRIES_LOOKUP cl ON cl.COUNTRY_NAME = id_country.description
                                    LEFT JOIN GENERIC_DETAIL GG ON GG.FK_GENERIC_HEADPAR = la.FKGH_HAS_AS_LOAN_P
                               AND GG.SERIAL_NUM = la.FKGD_HAS_AS_LOAN_P
                                    LEFT JOIN PRODUCT p ON la.FK_LOANFK_PRODUCTI = p.ID_PRODUCT)
        SELECT *
        FROM (SELECT mq.*,
                     ROW_NUMBER() OVER (PARTITION BY mq.loanNumber ORDER BY mq.customerIdentificationNumber) AS rn
              FROM MainQuery mq) t
        WHERE t.rn = 1
        ORDER BY t.customerIdentificationNumber
        """
        
        return query
    
    def get_total_count_query(self):
        """Get total count of available loan information records"""
        return """
        WITH ProfitsAccount AS (SELECT CUST_ID,
                                       MIN(ACCOUNT_NUMBER) AS ACCOUNT_NUMBER
                                FROM PROFITS_ACCOUNT
                                GROUP BY CUST_ID),
             MainQuery AS (SELECT la.ACC_SN
                           FROM LOAN_ACCOUNT la
                                    LEFT JOIN ProfitsAccount pa ON pa.CUST_ID = la.CUST_ID
                                    LEFT JOIN CUSTOMER cust ON la.CUST_ID = cust.CUST_ID)
        SELECT COUNT(*) as total_count
        FROM (SELECT mq.*,
                     ROW_NUMBER() OVER (PARTITION BY pa.ACCOUNT_NUMBER ORDER BY cust.CUST_ID) AS rn
              FROM MainQuery mq
                       LEFT JOIN LOAN_ACCOUNT la ON mq.ACC_SN = la.ACC_SN
                       LEFT JOIN ProfitsAccount pa ON pa.CUST_ID = la.CUST_ID
                       LEFT JOIN CUSTOMER cust ON la.CUST_ID = cust.CUST_ID) t
        WHERE t.rn = 1
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
                    self.logger.info(f"Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                else:
                    self.logger.error(f"Failed to connect to RabbitMQ after {max_retries} attempts")
                    raise
    
    def setup_rabbitmq_queue(self):
        """Setup RabbitMQ queue for loan information"""
        try:
            connection, channel = self.setup_rabbitmq_connection()
            channel.queue_declare(queue='loan_information_queue', durable=True)
            connection.close()
            self.logger.info("RabbitMQ queue 'loan_information_queue' setup complete")
        except Exception as e:
            self.logger.error(f"Failed to setup RabbitMQ: {e}")
            raise
    
    def producer_thread(self):
        """Producer thread - reads from DB2 and sends to RabbitMQ"""
        try:
            self.logger.info("Producer thread started")
            
            # Get total count first
            with self.db2_conn.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(self.get_total_count_query())
                self.total_available = cursor.fetchone()[0]
            
            self.logger.info(f"Total loan information records available: {self.total_available:,}")
            
            # Setup RabbitMQ connection
            connection, channel = self.setup_rabbitmq_connection()
            
            # Fetch all records at once (only 13,410 records)
            with self.db2_conn.get_connection() as conn:
                cursor = conn.cursor()
                query = self.get_loan_information_query()
                cursor.execute(query)
                rows = cursor.fetchall()
            
            self.logger.info(f"Retrieved {len(rows):,} records from DB2")
            
            # Process and publish all records
            for i, row in enumerate(rows):
                record = self.processor.process_record(row, 'loan_information')
                
                if self.processor.validate_record(record):
                    message = json.dumps(asdict(record), default=str)
                    
                    # Publish with retry
                    published = False
                    for attempt in range(self.max_retries):
                        try:
                            channel.basic_publish(
                                exchange='',
                                routing_key='loan_information_queue',
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
                            else:
                                self.logger.error(f"Failed to publish message after {self.max_retries} attempts")
                    
                    if published:
                        self.total_produced += 1
                        
                        # Progress report every 1000 records
                        if self.total_produced % 1000 == 0:
                            progress_percent = self.total_produced / len(rows) * 100
                            self.logger.info(f"Producer: {self.total_produced:,}/{len(rows):,} records published ({progress_percent:.1f}%)")
            
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
            
            def process_message(ch, method, properties, body):
                try:
                    record_data = json.loads(body)
                    record = LoanInformationRecord(**record_data)
                    
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
                        self.total_inserted += 1
                        
                        # Progress monitoring every 1000 records
                        if self.total_consumed % 1000 == 0:
                            progress_percent = (self.total_consumed / self.total_available * 100) if self.total_available > 0 else 0
                            self.logger.info(f"Consumer: Inserted {self.total_inserted:,} records ({progress_percent:.1f}% of total)")
                    
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    
                except Exception as e:
                    self.logger.error(f"Consumer error processing message: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            
            channel.basic_qos(prefetch_count=100)
            channel.basic_consume(queue='loan_information_queue', on_message_callback=process_message)
            
            # Keep consuming until producer is done and queue is empty
            while not self.stop_consumer.is_set():
                try:
                    connection.process_data_events(time_limit=1)
                    
                    if self.producer_finished.is_set():
                        method = channel.queue_declare(queue='loan_information_queue', durable=True, passive=True)
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
                    channel.basic_qos(prefetch_count=100)
                    channel.basic_consume(queue='loan_information_queue', on_message_callback=process_message)
            
            connection.close()
            self.logger.info(f"Consumer finished: {self.total_consumed:,} records processed")
            self.consumer_finished.set()
            
        except Exception as e:
            self.logger.error(f"Consumer error: {e}")
            self.consumer_finished.set()
    
    def run_streaming_pipeline(self):
        """Run the streaming pipeline with simultaneous producer and consumer"""
        self.logger.info("Starting Loan Information STREAMING pipeline...")
        
        try:
            self.setup_rabbitmq_queue()
            
            # Start consumer thread first
            consumer_thread = threading.Thread(target=self.consumer_thread, name="Consumer")
            consumer_thread.start()
            
            time.sleep(1)
            
            # Start producer thread
            producer_thread = threading.Thread(target=self.producer_thread, name="Producer")
            producer_thread.start()
            
            # Wait for producer to finish
            producer_thread.join()
            self.logger.info("Producer thread completed")
            
            # Wait for consumer to finish (no timeout - let it process all records)
            consumer_thread.join()
            
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
            Loan Information Pipeline Summary:
            ==========================================
            Total available records: {self.total_available:,}
            Records produced: {self.total_produced:,}
            Records consumed: {self.total_consumed:,}
            Records inserted: {self.total_inserted:,}
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
    pipeline = LoanInformationStreamingPipeline(batch_size=1000)
    
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