#!/usr/bin/env python3
"""
Personal Data Corporate Streaming Pipeline - Producer and Consumer run simultaneously
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
class PersonalDataCorporateRecord:
    reportingDate: str
    companyName: str
    customerIdentificationNumber: str
    establishedDate: Optional[str]
    legalForm: str
    negativeClientStatus: str
    registrationCountry: Optional[str]
    registrationNumber: Optional[str]
    taxIdentificationNumber: Optional[str]
    tradeName: str
    parentName: Optional[str]
    parentIncorporationNumber: Optional[str]
    groupId: Optional[str]
    sectorSnaClassification: str
    relatedCustomers: str  # JSON string
    entityName: str
    certificateIncorporation: Optional[str]
    entityRegion: str
    entityDistrict: str
    entityWard: str
    entityStreet: str
    entityHouseNumber: str
    entityPostalCode: str
    groupParentCode: str
    shareOwnedPercentage: Optional[str]
    shareOwnedAmount: Optional[str]


class PersonalDataCorporateStreamingPipeline:
    def __init__(self, batch_size=100):
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
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        self.logger = logging.getLogger(__name__)

        self.logger.info("Personal Data Corporate STREAMING Pipeline initialized")
        self.logger.info(f"Batch size: {self.batch_size} records per batch")
        self.logger.info("Mode: Streaming (Producer + Consumer simultaneously)")

    def get_corporate_query(self, last_customer_id=None):
        """Get the corporate query with cursor-based pagination"""

        where_clause = "WHERE ca2.FK_CUSTOMERCUST_ID <> ca.corporate_cust_id AND rel.FIRST_NAME IS NOT NULL AND TRIM(rel.FIRST_NAME) <> ''"

        if last_customer_id:
            where_clause += f" AND ca.corporate_cust_id > '{last_customer_id}'"

        query = f"""
        WITH corporate_customers AS
                 (SELECT CUST_ID,
                         ID_NO
                  FROM (SELECT CUST_ID,
                               ID_NO,
                               ROW_NUMBER() OVER
                                   (
                                   PARTITION BY CUST_ID
                                   ORDER BY CUST_ID
                                   ) AS rn
                        FROM W_DIM_CUSTOMER
                        WHERE CUST_TYPE_IND = 'Corporate') x
                  WHERE rn = 1),

             corporate_agreements AS
                 (SELECT DISTINCT a.AGR_SN,
                                  ca.FK_CUSTOMERCUST_ID AS corporate_cust_id
                  FROM AGREEMENT a
                           JOIN CUST_ADDRESS ca
                                ON a.FK_CUST_ADDRESSFK = ca.FK_CUSTOMERCUST_ID
                                    AND a.FK_CUST_ADDRESSSER = ca.SERIAL_NUM

                           JOIN corporate_customers cc
                                ON cc.CUST_ID = ca.FK_CUSTOMERCUST_ID

                  WHERE EXISTS
                            (SELECT 1
                             FROM PROFITS_ACCOUNT pa
                             WHERE pa.CUST_ID = ca.FK_CUSTOMERCUST_ID
                               AND pa.PRODUCT_ID = 31704))

        SELECT CURRENT_TIMESTAMP                               AS reportingDate,
               corp.SURNAME                                    AS companyName,
               ca.corporate_cust_id                            AS customerIdentificationNumber,
               corp.DATE_OF_BIRTH                              AS establishedDate,
               'LimitedLiabilityCompanyPrivate'                AS legalForm,
               'NoNegativeStatus'                              AS negativeClientStatus,
               CASE UPPER(TRIM(id_country.description))
                   WHEN 'TANZANIA'
                       THEN 'TANZANIA, UNITED REPUBLIC OF' END AS registrationCountry,
               id.id_no                                        AS registrationNumber,
               cc.ID_NO                                        AS taxIdentificationNumber,
               corp.SURNAME                                    AS tradeName,
               NULL                                            AS parentName,
               NULL                                            AS parentIncorporationNumber,
               NULL                                            AS groupId,
               'Other financial Corporations'                  AS sectorSnaClassification,
               '[' ||
               RTRIM(
                       CAST(
                               XMLSERIALIZE(
                                       XMLAGG(
                                               XMLTEXT(
                                                       '{{' ||
                                                       '"fullName":"' || REPLACE(COALESCE(TRIM(rel.FIRST_NAME) || ' ' ||
                                                                                          COALESCE(TRIM(rel.MIDDLE_NAME) || ' ', '') ||
                                                                                          TRIM(rel.SURNAME), ''), '"', '\\"') ||
                                                       '",' ||
                                                       '"gender":' || CASE TRIM(rel.SEX)
                                                                          WHEN 'M' THEN '"Male"'
                                                                          WHEN 'F' THEN '"Female"'
                                                                          ELSE 'null' END || ',' ||
                                                       '"relationType":"Director",' ||
                                                       '"nationality":' || CASE UPPER(TRIM(id_country.description))
                                                                               WHEN 'TANZANIA'
                                                                                   THEN '"TANZANIA, UNITED REPUBLIC OF"'
                                                                               ELSE 'null' END || ',' ||
                                                       '"appointmentDate":"N/A",' ||
                                                       '"terminationDate":null,' ||
                                                       '"street":null,' ||
                                                       '"country":' || CASE UPPER(TRIM(id_country.description))
                                                                           WHEN 'TANZANIA' THEN '"TANZANIA, UNITED REPUBLIC OF"'
                                                                           ELSE 'null' END || ',' ||
                                                       '"region":' || CASE
                                                                          WHEN TRIM(c_address.CITY) IS NOT NULL
                                                                              THEN '"' || REPLACE(TRIM(c_address.CITY), '"', '\\"') || '"'
                                                                          ELSE 'null' END || ',' ||
                                                       '"district":' || CASE
                                                                            WHEN TRIM(c_address.REGION) IS NOT NULL
                                                                                THEN '"' || REPLACE(TRIM(c_address.REGION), '"', '\\"') || '"'
                                                                            ELSE 'null' END || ',' ||
                                                       '"ward":' || CASE
                                                                        WHEN TRIM(c_address.ADDRESS_1) IS NOT NULL
                                                                            THEN '"' || REPLACE(TRIM(c_address.ADDRESS_1), '"', '\\"') || '"'
                                                                        ELSE 'null' END || ',' ||
                                                       '"zipCode":' || CASE
                                                                           WHEN TRIM(c_address.ZIP_CODE) IS NOT NULL
                                                                               THEN '"' || REPLACE(TRIM(c_address.ZIP_CODE), '"', '\\"') || '"'
                                                                           ELSE 'null' END || ',' ||
                                                       '"primaryRegion":' || CASE
                                                                                 WHEN TRIM(c_address.CITY) IS NOT NULL
                                                                                     THEN '"' || REPLACE(TRIM(c_address.CITY), '"', '\\"') || '"'
                                                                                 ELSE 'null' END || ',' ||
                                                       '"primaryDistrict":' || CASE
                                                                                   WHEN TRIM(c_address.REGION) IS NOT NULL
                                                                                       THEN '"' || REPLACE(TRIM(c_address.REGION), '"', '\\"') || '"'
                                                                                   ELSE 'null' END || ',' ||
                                                       '"primaryWard":' || CASE
                                                                               WHEN TRIM(c_address.ADDRESS_1) IS NOT NULL
                                                                                   THEN '"' || REPLACE(TRIM(c_address.ADDRESS_1), '"', '\\"') || '"'
                                                                               ELSE 'null' END ||
                                                       '}},'
                                               )
                                       ) AS CLOB
                               ) AS VARCHAR(32000)
                       ), ','
               ) || ']'                                        AS related_customers,

               'N/A'                                           AS entityName,
               NULL                                            AS certificateIncorporation,
               'N/A'                                           AS entityRegion,
               'N/A'                                           AS entityDistrict,
               'N/A'                                           AS entityWard,
               'N/A'                                           AS entityStreet,
               'N/A'                                           AS entityHouseNumber,
               'N/A'                                           AS entityPostalCode,
               'N/A'                                           AS groupParentCode,
               NULL                                            AS shareOwnedPercentage,
               NULL                                            AS shareOwnedAmount,
               ca.corporate_cust_id                            AS cursor_customer_id

        FROM corporate_agreements ca

                 JOIN PROFITS.CUSTOMER corp
                      ON corp.CUST_ID = ca.corporate_cust_id

                 JOIN corporate_customers cc
                      ON cc.CUST_ID = corp.CUST_ID

                 JOIN AGREEMENT a2
                      ON a2.AGR_SN = ca.AGR_SN

                 JOIN CUST_ADDRESS ca2
                      ON a2.FK_CUST_ADDRESSFK = ca2.FK_CUSTOMERCUST_ID
                          AND a2.FK_CUST_ADDRESSSER = ca2.SERIAL_NUM

                 JOIN CUSTOMER rel
                      ON rel.CUST_ID = ca2.FK_CUSTOMERCUST_ID

                 LEFT JOIN cust_address c_address
                           ON c_address.fk_customercust_id = corp.cust_id
                               AND c_address.communication_addr = '1'
                               AND c_address.entry_status = '1'

                 LEFT JOIN other_id id
                           ON id.fk_customercust_id = corp.cust_id
                               AND (CASE
                                        WHEN id.serial_no IS NULL
                                            THEN '1'
                                        ELSE id.main_flag
                                   END) = '1'

                 LEFT JOIN generic_detail id_country
                           ON id.fkgh_has_been_issu = id_country.fk_generic_headpar
                               AND id.fkgd_has_been_issu = id_country.serial_num

        {where_clause}

        GROUP BY corp.SURNAME,
                 ca.corporate_cust_id,
                 corp.DATE_OF_BIRTH,
                 id_country.DESCRIPTION,
                 id.ID_NO,
                 cc.ID_NO

        ORDER BY ca.corporate_cust_id ASC
        FETCH FIRST {self.batch_size} ROWS ONLY
        """

        return query

    def get_total_count_query(self):
        """Get total count of available corporate records"""
        return """
        WITH corporate_customers AS
                 (SELECT CUST_ID,
                         ID_NO
                  FROM (SELECT CUST_ID,
                               ID_NO,
                               ROW_NUMBER() OVER
                                   (
                                   PARTITION BY CUST_ID
                                   ORDER BY CUST_ID
                                   ) AS rn
                        FROM W_DIM_CUSTOMER
                        WHERE CUST_TYPE_IND = 'Corporate') x
                  WHERE rn = 1),

             corporate_agreements AS
                 (SELECT DISTINCT a.AGR_SN,
                                  ca.FK_CUSTOMERCUST_ID AS corporate_cust_id
                  FROM AGREEMENT a
                           JOIN CUST_ADDRESS ca
                                ON a.FK_CUST_ADDRESSFK = ca.FK_CUSTOMERCUST_ID
                                    AND a.FK_CUST_ADDRESSSER = ca.SERIAL_NUM

                           JOIN corporate_customers cc
                                ON cc.CUST_ID = ca.FK_CUSTOMERCUST_ID

                  WHERE EXISTS
                            (SELECT 1
                             FROM PROFITS_ACCOUNT pa
                             WHERE pa.CUST_ID = ca.FK_CUSTOMERCUST_ID
                               AND pa.PRODUCT_ID = 31704))

        SELECT COUNT(DISTINCT ca.corporate_cust_id) as total_count
        FROM corporate_agreements ca
                 JOIN AGREEMENT a2 ON a2.AGR_SN = ca.AGR_SN
                 JOIN CUST_ADDRESS ca2
                      ON a2.FK_CUST_ADDRESSFK = ca2.FK_CUSTOMERCUST_ID
                          AND a2.FK_CUST_ADDRESSSER = ca2.SERIAL_NUM
                 JOIN CUSTOMER rel ON rel.CUST_ID = ca2.FK_CUSTOMERCUST_ID
        WHERE ca2.FK_CUSTOMERCUST_ID <> ca.corporate_cust_id
          AND rel.FIRST_NAME IS NOT NULL
          AND TRIM(rel.FIRST_NAME) <> ''
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
                password=self.config.database.pg_password,
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
                    self.config.message_queue.rabbitmq_password,
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
                self.logger.warning(
                    f"RabbitMQ connection attempt {attempt + 1} failed: {e}"
                )
                if attempt < max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    raise

    def setup_rabbitmq_queue(self):
        """Setup RabbitMQ queue for personal data corporate"""
        try:
            connection, channel = self.setup_rabbitmq_connection()
            channel.queue_declare(queue="personal_data_corporate_queue", durable=True)
            connection.close()
            self.logger.info("RabbitMQ queue 'personal_data_corporate_queue' setup complete")
        except Exception as e:
            self.logger.error(f"Failed to setup RabbitMQ: {e}")
            raise

    def process_record(self, row):
        """Process a single record - returns None if record should be skipped"""
        # Remove cursor field (last one)
        row_data = row[:-1]

        return PersonalDataCorporateRecord(
            reportingDate=str(row_data[0]) if row_data[0] else None,
            companyName=str(row_data[1]).strip() if row_data[1] else None,
            customerIdentificationNumber=str(row_data[2]).strip() if row_data[2] else None,
            establishedDate=str(row_data[3]).strip() if row_data[3] else None,
            legalForm=str(row_data[4]).strip() if row_data[4] else None,
            negativeClientStatus=str(row_data[5]).strip() if row_data[5] else None,
            registrationCountry=str(row_data[6]).strip() if row_data[6] else None,
            registrationNumber=str(row_data[7]).strip() if row_data[7] else None,
            taxIdentificationNumber=str(row_data[8]).strip() if row_data[8] else None,
            tradeName=str(row_data[9]).strip() if row_data[9] else None,
            parentName=str(row_data[10]).strip() if row_data[10] else None,
            parentIncorporationNumber=str(row_data[11]).strip() if row_data[11] else None,
            groupId=str(row_data[12]).strip() if row_data[12] else None,
            sectorSnaClassification=str(row_data[13]).strip() if row_data[13] else None,
            relatedCustomers=str(row_data[14]).strip() if row_data[14] else '[]',
            entityName=str(row_data[15]).strip() if row_data[15] else None,
            certificateIncorporation=str(row_data[16]).strip() if row_data[16] else None,
            entityRegion=str(row_data[17]).strip() if row_data[17] else None,
            entityDistrict=str(row_data[18]).strip() if row_data[18] else None,
            entityWard=str(row_data[19]).strip() if row_data[19] else None,
            entityStreet=str(row_data[20]).strip() if row_data[20] else None,
            entityHouseNumber=str(row_data[21]).strip() if row_data[21] else None,
            entityPostalCode=str(row_data[22]).strip() if row_data[22] else None,
            groupParentCode=str(row_data[23]).strip() if row_data[23] else None,
            shareOwnedPercentage=str(row_data[24]).strip() if row_data[24] else None,
            shareOwnedAmount=str(row_data[25]).strip() if row_data[25] else None,
        )

    def insert_to_postgres(self, record: PersonalDataCorporateRecord, cursor):
        """Insert record to PostgreSQL"""
        insert_sql = """
        INSERT INTO "personalDataCorporate" (
            "reportingDate", "companyName", "customerIdentificationNumber", "establishedDate",
            "legalForm", "negativeClientStatus", "registrationCountry", "registrationNumber",
            "taxIdentificationNumber", "tradeName", "parentName", "parentIncorporationNumber",
            "groupId", "sectorSnaClassification", "relatedCustomers", "entityName",
            "certificateIncorporation", "entityRegion", "entityDistrict", "entityWard",
            "entityStreet", "entityHouseNumber", "entityPostalCode", "groupParentCode",
            "shareOwnedPercentage", "shareOwnedAmount"
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT ("customerIdentificationNumber") DO UPDATE SET
            "reportingDate" = EXCLUDED."reportingDate",
            "companyName" = EXCLUDED."companyName",
            "establishedDate" = EXCLUDED."establishedDate",
            "legalForm" = EXCLUDED."legalForm",
            "negativeClientStatus" = EXCLUDED."negativeClientStatus",
            "registrationCountry" = EXCLUDED."registrationCountry",
            "registrationNumber" = EXCLUDED."registrationNumber",
            "taxIdentificationNumber" = EXCLUDED."taxIdentificationNumber",
            "tradeName" = EXCLUDED."tradeName",
            "parentName" = EXCLUDED."parentName",
            "parentIncorporationNumber" = EXCLUDED."parentIncorporationNumber",
            "groupId" = EXCLUDED."groupId",
            "sectorSnaClassification" = EXCLUDED."sectorSnaClassification",
            "relatedCustomers" = EXCLUDED."relatedCustomers",
            "entityName" = EXCLUDED."entityName",
            "certificateIncorporation" = EXCLUDED."certificateIncorporation",
            "entityRegion" = EXCLUDED."entityRegion",
            "entityDistrict" = EXCLUDED."entityDistrict",
            "entityWard" = EXCLUDED."entityWard",
            "entityStreet" = EXCLUDED."entityStreet",
            "entityHouseNumber" = EXCLUDED."entityHouseNumber",
            "entityPostalCode" = EXCLUDED."entityPostalCode",
            "groupParentCode" = EXCLUDED."groupParentCode",
            "shareOwnedPercentage" = EXCLUDED."shareOwnedPercentage",
            "shareOwnedAmount" = EXCLUDED."shareOwnedAmount"
        """

        cursor.execute(
            insert_sql,
            (
                record.reportingDate,
                record.companyName,
                record.customerIdentificationNumber,
                record.establishedDate,
                record.legalForm,
                record.negativeClientStatus,
                record.registrationCountry,
                record.registrationNumber,
                record.taxIdentificationNumber,
                record.tradeName,
                record.parentName,
                record.parentIncorporationNumber,
                record.groupId,
                record.sectorSnaClassification,
                record.relatedCustomers,
                record.entityName,
                record.certificateIncorporation,
                record.entityRegion,
                record.entityDistrict,
                record.entityWard,
                record.entityStreet,
                record.entityHouseNumber,
                record.entityPostalCode,
                record.groupParentCode,
                record.shareOwnedPercentage,
                record.shareOwnedAmount,
            ),
        )

    def producer_thread(self):
        """Producer thread - reads from DB2 and sends to RabbitMQ"""
        try:
            self.logger.info("Producer thread started")

            # Get total count
            with self.db2_conn.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(self.get_total_count_query())
                self.total_available = cursor.fetchone()[0]

            self.logger.info(f"Total corporate records available: {self.total_available:,}")

            # Setup RabbitMQ
            connection, channel = self.setup_rabbitmq_connection()

            # Process batches
            batch_number = 1
            last_customer_id = None
            last_progress_report = time.time()

            while True:
                batch_start_time = time.time()

                # Fetch batch
                rows = None
                for attempt in range(self.max_retries):
                    try:
                        with self.db2_conn.get_connection() as conn:
                            cursor = conn.cursor()
                            batch_query = self.get_corporate_query(last_customer_id)
                            cursor.execute(batch_query)
                            rows = cursor.fetchall()
                        break
                    except Exception as e:
                        self.logger.warning(
                            f"DB2 query attempt {attempt + 1} failed: {e}"
                        )
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
                    last_customer_id = row[-1]  # cursor_customer_id

                    record = self.process_record(row)

                    # Skip record if it's None (validation failed)
                    if record is None:
                        continue

                    message = json.dumps(asdict(record), default=str)

                    # Publish
                    published = False
                    for attempt in range(self.max_retries):
                        try:
                            channel.basic_publish(
                                exchange="",
                                routing_key="personal_data_corporate_queue",
                                body=message,
                                properties=pika.BasicProperties(delivery_mode=2),
                            )
                            published = True
                            break
                        except Exception as e:
                            self.logger.warning(
                                f"RabbitMQ publish attempt {attempt + 1} failed: {e}"
                            )
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
                progress_percent = (
                    self.total_produced / self.total_available * 100
                    if self.total_available > 0
                    else 0
                )

                self.logger.info(
                    f"Producer: Batch {batch_number:,} - {len(rows)} records, {batch_published} published ({progress_percent:.2f}% complete, {batch_time:.1f}s)"
                )

                # Progress report every 5 minutes
                current_time = time.time()
                if current_time - last_progress_report >= 300:
                    elapsed_time = current_time - self.start_time
                    rate = (
                        self.total_produced / elapsed_time if elapsed_time > 0 else 0
                    )
                    remaining_records = self.total_available - self.total_produced
                    eta_seconds = remaining_records / rate if rate > 0 else 0
                    eta_minutes = eta_seconds / 60

                    self.logger.info(
                        f"PROGRESS REPORT: {self.total_produced:,}/{self.total_available:,} records ({progress_percent:.1f}%) - Rate: {rate:.1f} rec/sec - ETA: {eta_minutes:.1f} minutes"
                    )
                    last_progress_report = current_time

                batch_number += 1
                time.sleep(0.1)

            connection.close()
            self.logger.info(
                f"Producer finished: {self.total_produced:,} records published"
            )
            self.producer_finished.set()

        except Exception as e:
            self.logger.error(f"Producer error: {e}")
            self.producer_finished.set()

    def consumer_thread(self):
        """Consumer thread - processes messages from queue"""
        try:
            self.logger.info("Consumer thread started - waiting for messages...")

            connection, channel = self.setup_rabbitmq_connection()
            last_progress_report = time.time()

            def process_message(ch, method, properties, body):
                nonlocal last_progress_report
                try:
                    record_data = json.loads(body)
                    record = PersonalDataCorporateRecord(**record_data)

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
                            self.logger.warning(
                                f"PostgreSQL insert attempt {attempt + 1} failed: {e}"
                            )
                            if attempt < self.max_retries - 1:
                                time.sleep(self.retry_delay)

                    if inserted:
                        self.total_consumed += 1

                        # Log more frequently to show concurrent activity
                        if self.total_consumed % 10 == 0:
                            elapsed_time = time.time() - self.start_time
                            rate = (
                                self.total_consumed / elapsed_time
                                if elapsed_time > 0
                                else 0
                            )
                            progress_percent = (
                                (self.total_consumed / self.total_available * 100)
                                if self.total_available > 0
                                else 0
                            )
                            self.logger.info(
                                f"Consumer: Processed {self.total_consumed:,} records ({progress_percent:.2f}%) - Rate: {rate:.1f} rec/sec"
                            )

                        # Detailed progress report every 5 minutes
                        current_time = time.time()
                        if current_time - last_progress_report >= 300:
                            elapsed_time = current_time - self.start_time
                            rate = (
                                self.total_consumed / elapsed_time
                                if elapsed_time > 0
                                else 0
                            )
                            remaining_records = (
                                self.total_available - self.total_consumed
                                if self.total_available > 0
                                else 0
                            )
                            eta_seconds = remaining_records / rate if rate > 0 else 0
                            eta_minutes = eta_seconds / 60

                            self.logger.info(
                                f"CONSUMER PROGRESS: {self.total_consumed:,}/{self.total_available:,} records - Rate: {rate:.1f} rec/sec - ETA: {eta_minutes:.1f} minutes"
                            )
                            last_progress_report = current_time

                    ch.basic_ack(delivery_tag=method.delivery_tag)

                except Exception as e:
                    self.logger.error(f"Consumer error processing message: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

            channel.basic_qos(prefetch_count=10)
            channel.basic_consume(
                queue="personal_data_corporate_queue", on_message_callback=process_message
            )

            while not self.stop_consumer.is_set():
                try:
                    connection.process_data_events(time_limit=1)

                    if self.producer_finished.is_set():
                        method = channel.queue_declare(
                            queue="personal_data_corporate_queue", durable=True, passive=True
                        )
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
                    channel.basic_consume(
                        queue="personal_data_corporate_queue", on_message_callback=process_message
                    )

            connection.close()
            self.logger.info(
                f"Consumer finished: {self.total_consumed:,} records processed"
            )
            self.consumer_finished.set()

        except Exception as e:
            self.logger.error(f"Consumer error: {e}")
            self.consumer_finished.set()

    def run_streaming_pipeline(self):
        """Run the streaming pipeline"""
        self.logger.info("Starting Personal Data Corporate STREAMING pipeline...")

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
            success_rate = (
                (self.total_consumed / self.total_produced * 100)
                if self.total_produced > 0
                else 0
            )

            self.logger.info(
                f"""
            ==========================================
            Personal Data Corporate Pipeline Summary:
            ==========================================
            Total available records: {self.total_available:,}
            Records produced: {self.total_produced:,}
            Records consumed: {self.total_consumed:,}
            Success rate: {success_rate:.1f}%
            Total processing time: {total_time/60:.2f} minutes
            Average rate: {avg_rate:.1f} records/second
            ==========================================
            """
            )

        except Exception as e:
            self.logger.error(f"Pipeline error: {e}")
            raise


def main():
    """Main function"""
    import argparse

    parser = argparse.ArgumentParser(description="Personal Data Corporate Streaming Pipeline")
    parser.add_argument(
        "--batch-size", type=int, default=100, help="Batch size for processing"
    )

    args = parser.parse_args()

    pipeline = PersonalDataCorporateStreamingPipeline(batch_size=args.batch_size)

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
