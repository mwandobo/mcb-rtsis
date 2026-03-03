#!/usr/bin/env python3
"""
Deposits Streaming Pipeline - Producer and Consumer run simultaneously
Uses deposits-v1.sql query with proper location mapping and account data
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
class DepositsRecord:
    reportingDate: str
    clientIdentificationNumber: str
    accountNumber: str
    accountName: str
    customerCategory: str
    customerCountry: str
    branchCode: str
    clientType: str
    relationshipType: str
    district: str
    region: str
    accountProductName: str
    accountType: str
    accountSubType: Optional[str]
    depositCategory: str
    depositAccountStatus: str
    transactionUniqueRef: str
    timeStamp: str
    serviceChannel: str
    currency: str
    transactionType: str
    orgTransactionAmount: Optional[Decimal]
    usdTransactionAmount: Optional[Decimal]
    tzsTransactionAmount: Optional[Decimal]
    transactionPurposes: Optional[str]
    sectorSnaClassification: str
    lienNumber: Optional[str]
    orgAmountLien: Optional[Decimal]
    usdAmountLien: Optional[Decimal]
    tzsAmountLien: Optional[Decimal]
    contractDate: str
    maturityDate: Optional[str]
    annualInterestRate: int
    interestRateType: str
    orgInterestAmount: int
    usdInterestAmount: int
    tzsInterestAmount: int


class DepositsStreamingPipeline:
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
        
        self.logger.info("Deposits STREAMING Pipeline initialized")
        self.logger.info(f"Batch size: {self.batch_size} records per batch")
        self.logger.info("Mode: Streaming (Producer + Consumer simultaneously)")
    
    def get_query(self, last_cust_id=None):
        """Get the deposits query with cursor-based pagination using deposits-v1.sql"""
        
        where_clause = "WHERE gte.ID_PRODUCT IN (31201, 31202, 31220)"
        
        if last_cust_id:
            where_clause += f" AND gte.CUST_ID > '{last_cust_id}'"
        
        query = f"""
        WITH district_wards AS (SELECT DISTINCT DISTRICT,
                                                WARD,
                                                ROW_NUMBER() OVER (PARTITION BY DISTRICT ORDER BY WARD) AS rn,
                                                COUNT(*) OVER (PARTITION BY DISTRICT)                   AS total_wards
                                FROM bank_location_lookup_v2)
                ,
             region_districts AS (SELECT REGION,
                                         DISTRICT,
                                         ROW_NUMBER() OVER (PARTITION BY REGION ORDER BY DISTRICT) AS rn,
                                         COUNT(*) OVER (PARTITION BY REGION)                       AS total_districts
                                  FROM bank_location_lookup_v2
                                  GROUP BY REGION, DISTRICT)
        SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') AS reportingDate,
               gte.CUST_ID                                       AS clientIdentificationNumber,
               PA.ACCOUNT_NUMBER                                 AS accountNumber,
               wdc.NAME_STANDARD                                 AS accountName,
               'Ordinary'                                        AS customerCategory,
               'TANZANIA, UNITED REPUBLIC OF'                    AS customerCountry,
               pa.MONOTORING_UNIT                                AS branchCode,
               CASE
                   WHEN pa.PRFT_SYSTEM = 4 THEN 'Staff'
                   WHEN pa.PRFT_SYSTEM = 3 THEN 'Individual'
                   END                                           AS clientType,
               'Domestic banks unrelated'                        AS relationshipType,
               COALESCE(
                       loc_district_region.DISTRICT,
                       loc_district_from_ward.DISTRICT,
                       loc_district_from_city.DISTRICT,
                       loc_district_from_region.DISTRICT
               )                                                 AS district,
               COALESCE(
                       loc_region_city.REGION,
                       loc_region_dist.REGION,
                       loc_region_from_district.REGION,
                       loc_region_from_ward.REGION,
                       'Dar es Salaam'
               )                                                 AS region,
               p.DESCRIPTION                                     AS accountProductName,
               CASE
                   WHEN pa.PRFT_SYSTEM = 4 THEN 'Current'
                   ELSE 'Saving'
                   END
                                                                 AS accountType,
               CASE
                   WHEN pa.PRFT_SYSTEM = 4 THEN 'Normal'
                   ELSE NULL
                   END                                           AS accountSubType,
               'Deposit from public'                             AS depositCategory,
               CASE
                   WHEN pa.ACC_STATUS = 1 THEN 'active'
                   WHEN pa.ACC_STATUS = 3 THEN 'closed'
                   ELSE 'inactive'
                   END                                           AS depositAccountStatus,
               VARCHAR(gte.FK_UNITCODETRXUNIT) || '-' ||
               TRIM(gte.FK_USRCODE) || '-' ||
               VARCHAR(gte.LINE_NUM) || '-' ||
               VARCHAR(gte.TRN_DATE) || '-' ||
               VARCHAR(gte.TRN_SNUM)                             AS transactionUniqueRef,
               VARCHAR_FORMAT(gte.TMSTAMP, 'DDMMYYYYHHMM')       AS timeStamp,
               'Branch'                                          AS serviceChannel,
               gte.CURRENCY_SHORT_DES                            AS currency,
               'Deposit'                                         AS transactionType,
               gte.DC_AMOUNT                                     AS orgTransactionAmount,
               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN gte.DC_AMOUNT
                   WHEN gte.CURRENCY_SHORT_DES <> 'USD' THEN DECIMAL(gte.DC_AMOUNT / fx.RATE, 18, 2)
                   ELSE NULL
                   END                                           AS usdTransactionAmount,
               CASE
                   WHEN gte.CURRENCY_SHORT_DES = 'USD'
                       THEN gte.DC_AMOUNT * fx.RATE
                   ELSE
                       gte.DC_AMOUNT
                   END                                           AS tzsTransactionAmount,
               gte.JUSTIFIC_DESCR                                AS transactionPurposes,
               'Households'                                      AS sectorSnaClassification,
               null                                              AS lienNumber,
               null                                              AS orgAmountLien,
               null                                              AS usdAmountLien,
               null                                              AS tzsAmountLien,
               wdc.CUSTOMER_BEGIN_DAT                            AS contractDate,
               null                                              AS maturityDate,
               0                                                 AS annualInterestRate,
               'norminal'                                        AS interestRateType,
               0                                                 AS orgInterestAmount,
               0                                                 AS usdInterestAmount,
               0                                                 AS tzsInterestAmount,
               gte.CUST_ID                                       AS cursor_cust_id
        FROM GLI_TRX_EXTRACT gte
                 LEFT JOIN CURRENCY curr
                           ON curr.SHORT_DESCR = gte.CURRENCY_SHORT_DES
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
                 LEFT JOIN (SELECT *
                            FROM (SELECT wdc.*,
                                         ROW_NUMBER() OVER (PARTITION BY CUST_ID ORDER BY CUSTOMER_BEGIN_DAT DESC) rn
                                  FROM W_DIM_CUSTOMER wdc)
                            WHERE rn = 1) wdc ON wdc.CUST_ID = gte.CUST_ID
                 LEFT JOIN cust_address c_address
                           ON c_address.fk_customercust_id = wdc.cust_id
                               AND c_address.communication_addr = '1'
                               AND c_address.entry_status = '1'
                 LEFT JOIN (SELECT REGION,
                                   ROW_NUMBER() OVER (PARTITION BY REGION ORDER BY REGION) AS rn
                            FROM bank_location_lookup_v2) loc_region_city
                           ON REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(c_address.CITY)), ' ', ''), '-', ''), '_', ''), ',', '')
                                  LIKE '%' || REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(loc_region_city.REGION)), ' ', ''), '-', ''), '_', ''), ',', '') || '%'
                               AND loc_region_city.rn = 1
                 LEFT JOIN (SELECT REGION,
                                   ROW_NUMBER() OVER (PARTITION BY REGION ORDER BY REGION) AS rn
                            FROM bank_location_lookup_v2) loc_region_dist
                           ON loc_region_city.REGION IS NULL
                               AND REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(c_address.REGION)), ' ', ''), '-', ''), '_', ''), ',', '')
                                  LIKE '%' || REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(loc_region_dist.REGION)), ' ', ''), '-', ''), '_', ''), ',', '') || '%'
                               AND loc_region_dist.rn = 1
                 LEFT JOIN (SELECT REGION, DISTRICT,
                                   ROW_NUMBER() OVER (PARTITION BY DISTRICT ORDER BY REGION) AS rn
                            FROM bank_location_lookup_v2) loc_region_from_district
                           ON loc_region_city.REGION IS NULL AND loc_region_dist.REGION IS NULL
                               AND REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(c_address.REGION)), ' ', ''), '-', ''), '_', ''), ',', '')
                                  LIKE '%' || REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(loc_region_from_district.DISTRICT)), ' ', ''), '-', ''), '_', ''), ',', '') || '%'
                               AND loc_region_from_district.rn = 1
                 LEFT JOIN (SELECT REGION, WARD,
                                   ROW_NUMBER() OVER (PARTITION BY WARD ORDER BY REGION) AS rn
                            FROM bank_location_lookup_v2) loc_region_from_ward
                           ON loc_region_city.REGION IS NULL AND loc_region_dist.REGION IS NULL AND loc_region_from_district.REGION IS NULL
                               AND REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(c_address.ADDRESS_1)), ' ', ''), '-', ''), '_', ''), ',', '')
                                  LIKE '%' || REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(loc_region_from_ward.WARD)), ' ', ''), '-', ''), '_', ''), ',', '') || '%'
                               AND loc_region_from_ward.rn = 1
                 LEFT JOIN (SELECT REGION, DISTRICT,
                                   ROW_NUMBER() OVER (PARTITION BY REGION, DISTRICT ORDER BY DISTRICT) AS rn
                            FROM bank_location_lookup_v2) loc_district_region
                           ON loc_district_region.rn = 1
                               AND COALESCE(loc_region_city.REGION, loc_region_dist.REGION, loc_region_from_district.REGION, loc_region_from_ward.REGION) = loc_district_region.REGION
                               AND REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(c_address.REGION)), ' ', ''), '-', ''), '_', ''), ',', '')
                                  LIKE '%' || REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(loc_district_region.DISTRICT)), ' ', ''), '-', ''), '_', ''), ',', '') || '%'
                 LEFT JOIN (SELECT REGION, DISTRICT, WARD,
                                   ROW_NUMBER() OVER (PARTITION BY REGION, DISTRICT ORDER BY DISTRICT) AS rn
                            FROM bank_location_lookup_v2) loc_district_from_ward
                           ON loc_district_region.DISTRICT IS NULL AND loc_district_from_ward.rn = 1
                               AND COALESCE(loc_region_city.REGION, loc_region_dist.REGION, loc_region_from_district.REGION, loc_region_from_ward.REGION) = loc_district_from_ward.REGION
                               AND REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(c_address.ADDRESS_1)), ' ', ''), '-', ''), '_', ''), ',', '')
                                  LIKE '%' || REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(loc_district_from_ward.WARD)), ' ', ''), '-', ''), '_', ''), ',', '') || '%'
                 LEFT JOIN (SELECT REGION, DISTRICT,
                                   ROW_NUMBER() OVER (PARTITION BY REGION, DISTRICT ORDER BY DISTRICT) AS rn
                            FROM bank_location_lookup_v2) loc_district_from_city
                           ON loc_district_region.DISTRICT IS NULL AND loc_district_from_ward.DISTRICT IS NULL AND loc_district_from_city.rn = 1
                               AND COALESCE(loc_region_city.REGION, loc_region_dist.REGION, loc_region_from_district.REGION, loc_region_from_ward.REGION) = loc_district_from_city.REGION
                               AND REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(c_address.ADDRESS_1)), ' ', ''), '-', ''), '_', ''), ',', '')
                                  LIKE '%' || REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(loc_district_from_city.DISTRICT)), ' ', ''), '-', ''), '_', ''), ',', '') || '%'
                 LEFT JOIN (SELECT REGION, DISTRICT,
                                   ROW_NUMBER() OVER (PARTITION BY REGION ORDER BY DISTRICT ) AS rn
                            FROM bank_location_lookup_v2) loc_district_from_region
                           ON loc_district_region.DISTRICT IS NULL AND loc_district_from_ward.DISTRICT IS NULL AND loc_district_from_city.DISTRICT IS NULL
                               AND loc_district_from_region.rn = 1
                               AND loc_district_from_region.REGION = COALESCE(loc_region_city.REGION, loc_region_dist.REGION, loc_region_from_district.REGION, loc_region_from_ward.REGION, 'Dar es Salaam')
                 LEFT JOIN PRODUCT p ON p.ID_PRODUCT = gte.ID_PRODUCT
                 LEFT JOIN (SELECT *
                            FROM (SELECT pa.*,
                                         ROW_NUMBER() OVER (PARTITION BY CUST_ID ORDER BY ACCOUNT_NUMBER) rn
                                  FROM PROFITS_ACCOUNT pa
                                  WHERE PRFT_SYSTEM = 3)
                            WHERE rn = 1) pa ON pa.CUST_ID = gte.CUST_ID
        {where_clause}
        ORDER BY gte.CUST_ID ASC
        FETCH FIRST {self.batch_size} ROWS ONLY
        """
        
        return query

    def get_total_count_query(self):
        """Get total count of available records"""
        return """
        SELECT COUNT(*) as total_count
        FROM GLI_TRX_EXTRACT gte
        WHERE gte.ID_PRODUCT IN (31201, 31202, 31220)
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
            channel.queue_declare(queue='deposits_queue', durable=True)
            connection.close()
            self.logger.info("RabbitMQ queue 'deposits_queue' setup complete")
        except Exception as e:
            self.logger.error(f"Failed to setup RabbitMQ: {e}")
            raise

    def process_record(self, row):
        """Process a single record"""
        # Remove cursor field (last one)
        row_data = row[:-1]
        
        return DepositsRecord(
            reportingDate=str(row_data[0]) if row_data[0] else None,
            clientIdentificationNumber=str(row_data[1]).strip() if row_data[1] else None,
            accountNumber=str(row_data[2]).strip() if row_data[2] else None,
            accountName=str(row_data[3]).strip() if row_data[3] else None,
            customerCategory=str(row_data[4]).strip() if row_data[4] else None,
            customerCountry=str(row_data[5]).strip() if row_data[5] else None,
            branchCode=str(row_data[6]).strip() if row_data[6] else None,
            clientType=str(row_data[7]).strip() if row_data[7] else None,
            relationshipType=str(row_data[8]).strip() if row_data[8] else None,
            district=str(row_data[9]).strip() if row_data[9] else None,
            region=str(row_data[10]).strip() if row_data[10] else None,
            accountProductName=str(row_data[11]).strip() if row_data[11] else None,
            accountType=str(row_data[12]).strip() if row_data[12] else None,
            accountSubType=str(row_data[13]).strip() if row_data[13] else None,
            depositCategory=str(row_data[14]).strip() if row_data[14] else None,
            depositAccountStatus=str(row_data[15]).strip() if row_data[15] else None,
            transactionUniqueRef=str(row_data[16]).strip() if row_data[16] else None,
            timeStamp=str(row_data[17]) if row_data[17] else None,
            serviceChannel=str(row_data[18]).strip() if row_data[18] else None,
            currency=str(row_data[19]).strip() if row_data[19] else None,
            transactionType=str(row_data[20]).strip() if row_data[20] else None,
            orgTransactionAmount=Decimal(str(row_data[21])) if row_data[21] is not None else None,
            usdTransactionAmount=Decimal(str(row_data[22])) if row_data[22] is not None else None,
            tzsTransactionAmount=Decimal(str(row_data[23])) if row_data[23] is not None else None,
            transactionPurposes=str(row_data[24]).strip() if row_data[24] else None,
            sectorSnaClassification=str(row_data[25]).strip() if row_data[25] else None,
            lienNumber=str(row_data[26]).strip() if row_data[26] else None,
            orgAmountLien=Decimal(str(row_data[27])) if row_data[27] is not None else None,
            usdAmountLien=Decimal(str(row_data[28])) if row_data[28] is not None else None,
            tzsAmountLien=Decimal(str(row_data[29])) if row_data[29] is not None else None,
            contractDate=str(row_data[30]) if row_data[30] else None,
            maturityDate=str(row_data[31]) if row_data[31] else None,
            annualInterestRate=int(row_data[32]) if row_data[32] is not None else 0,
            interestRateType=str(row_data[33]).strip() if row_data[33] else None,
            orgInterestAmount=int(row_data[34]) if row_data[34] is not None else 0,
            usdInterestAmount=int(row_data[35]) if row_data[35] is not None else 0,
            tzsInterestAmount=int(row_data[36]) if row_data[36] is not None else 0
        )
    
    def insert_to_postgres(self, record: DepositsRecord, cursor):
        """Insert record to PostgreSQL"""
        insert_sql = """
        INSERT INTO "deposits" (
            "reportingDate", "clientIdentificationNumber", "accountNumber", "accountName",
            "customerCategory", "customerCountry", "branchCode", "clientType", "relationshipType",
            "district", "region", "accountProductName", "accountType", "accountSubType",
            "depositCategory", "depositAccountStatus", "transactionUniqueRef", "timeStamp",
            "serviceChannel", "currency", "transactionType", "orgTransactionAmount",
            "usdTransactionAmount", "tzsTransactionAmount", "transactionPurposes",
            "sectorSnaClassification", "lienNumber", "orgAmountLien", "usdAmountLien",
            "tzsAmountLien", "contractDate", "maturityDate", "annualInterestRate",
            "interestRateType", "orgInterestAmount", "usdInterestAmount", "tzsInterestAmount"
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        cursor.execute(insert_sql, (
            record.reportingDate,
            record.clientIdentificationNumber,
            record.accountNumber,
            record.accountName,
            record.customerCategory,
            record.customerCountry,
            record.branchCode,
            record.clientType,
            record.relationshipType,
            record.district,
            record.region,
            record.accountProductName,
            record.accountType,
            record.accountSubType,
            record.depositCategory,
            record.depositAccountStatus,
            record.transactionUniqueRef,
            record.timeStamp,
            record.serviceChannel,
            record.currency,
            record.transactionType,
            record.orgTransactionAmount,
            record.usdTransactionAmount,
            record.tzsTransactionAmount,
            record.transactionPurposes,
            record.sectorSnaClassification,
            record.lienNumber,
            record.orgAmountLien,
            record.usdAmountLien,
            record.tzsAmountLien,
            record.contractDate,
            record.maturityDate,
            record.annualInterestRate,
            record.interestRateType,
            record.orgInterestAmount,
            record.usdInterestAmount,
            record.tzsInterestAmount
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
            
            self.logger.info(f"Total deposits records available: {self.total_available:,}")
            
            # Setup RabbitMQ
            connection, channel = self.setup_rabbitmq_connection()
            
            # Process batches
            batch_number = 1
            last_cust_id = None
            
            while True:
                batch_start_time = time.time()
                
                # Fetch batch
                rows = None
                for attempt in range(self.max_retries):
                    try:
                        with self.db2_conn.get_connection(log_connection=False) as conn:
                            cursor = conn.cursor()
                            batch_query = self.get_query(last_cust_id)
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
                    last_cust_id = row[-1]  # cursor_cust_id
                    
                    record = self.process_record(row)
                    message = json.dumps(asdict(record), default=str)
                    
                    # Publish
                    published = False
                    for attempt in range(self.max_retries):
                        try:
                            channel.basic_publish(
                                exchange='',
                                routing_key='deposits_queue',
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
                    record = DepositsRecord(**record_data)
                    
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
            channel.basic_consume(queue='deposits_queue', on_message_callback=process_message)
            
            while not self.stop_consumer.is_set():
                try:
                    connection.process_data_events(time_limit=1)
                    
                    if self.producer_finished.is_set():
                        method = channel.queue_declare(queue='deposits_queue', durable=True, passive=True)
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
                    channel.basic_consume(queue='deposits_queue', on_message_callback=process_message)
            
            connection.close()
            self.logger.info(f"Consumer finished: {self.total_consumed:,} records processed")
            self.consumer_finished.set()
            
        except Exception as e:
            self.logger.error(f"Consumer error: {e}")
            self.consumer_finished.set()

    def run_streaming_pipeline(self):
        """Run the streaming pipeline"""
        self.logger.info("Starting Deposits STREAMING pipeline...")
        
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
            Deposits Pipeline Summary:
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
    
    parser = argparse.ArgumentParser(description='Deposits Streaming Pipeline')
    parser.add_argument('--batch-size', type=int, default=500, help='Batch size for processing')
    
    args = parser.parse_args()
    
    pipeline = DepositsStreamingPipeline(batch_size=args.batch_size)
    
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