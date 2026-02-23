#!/usr/bin/env python3
"""
Personal Data OPTIMIZED Streaming Pipeline - Producer and Consumer run simultaneously
Uses personal-data-optimized.sql query with CTEs for better performance
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
from processors.personal_data_processor import PersonalDataProcessor, PersonalDataRecord

class PersonalDataOptimizedStreamingPipeline:
    def __init__(self, batch_size=1000):  # Increased to 1000 for maximum performance
        self.config = Config()
        self.db2_conn = DB2Connection()
        self.personal_data_processor = PersonalDataProcessor()
        self.batch_size = batch_size
        
        # Threading control
        self.producer_finished = threading.Event()
        self.consumer_finished = threading.Event()
        self.stop_consumer = threading.Event()
        
        # Statistics
        self.total_produced = 0
        self.total_consumed = 0
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        self.logger.info("👤 Personal Data OPTIMIZED STREAMING Pipeline initialized")
        self.logger.info(f"📊 Batch size: {batch_size} records per batch (OPTIMIZED with CTEs - MAX PERFORMANCE)")
        self.logger.info("🔄 Mode: Streaming (Producer + Consumer simultaneously)")
        self.logger.info("⚡ Using personal-data-optimized.sql with CTEs for better performance")
    
    def get_personal_data_optimized_query(self, last_cust_id=None):
        """Get the optimized personal data query using CTEs for better performance"""
        
        if last_cust_id is None:
            # First batch
            where_clause = "WHERE UPPER(TRIM(idt.description)) NOT IN ('OTHER TYPE OF IDENTIFICATION','BIRTH CERTIFICATE') AND c.CUST_TYPE = '1'"
        else:
            # Subsequent batches - use cursor pagination
            where_clause = f"WHERE UPPER(TRIM(idt.description)) NOT IN ('OTHER TYPE OF IDENTIFICATION','BIRTH CERTIFICATE') AND c.CUST_TYPE = '1' AND c.cust_id > '{last_cust_id}'"
        
        query = f"""
        WITH
        /* -------------------------------------------------------
           normalize lookup only once - OPTIMIZED CTEs
        ------------------------------------------------------- */
        loc_base AS
        (
            SELECT
                REGION,
                DISTRICT,
                WARD,
                REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(REGION))  ,' ',''),'-',''),'_',''),',','') AS n_region,
                REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(DISTRICT)),' ',''),'-',''),'_',''),',','') AS n_district,
                REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(WARD))    ,' ',''),'-',''),'_',''),',','') AS n_ward
            FROM bank_location_lookup_v2
        ),

        /* region text */
        loc_region AS
        (
            SELECT
                REGION,
                n_region,
                ROW_NUMBER() OVER (PARTITION BY REGION ORDER BY REGION) rn
            FROM loc_base
        ),

        /* district -> region */
        loc_region_from_district AS
        (
            SELECT
                REGION,
                DISTRICT,
                n_district,
                ROW_NUMBER() OVER (PARTITION BY DISTRICT ORDER BY REGION) rn
            FROM loc_base
        ),

        /* ward -> region */
        loc_region_from_ward AS
        (
            SELECT
                REGION,
                WARD,
                n_ward,
                ROW_NUMBER() OVER (PARTITION BY WARD ORDER BY REGION) rn
            FROM loc_base
        ),

        /* region + district */
        loc_district_region AS
        (
            SELECT
                REGION,
                DISTRICT,
                n_district,
                ROW_NUMBER() OVER (PARTITION BY REGION, DISTRICT ORDER BY DISTRICT) rn
            FROM loc_base
        ),

        /* ward -> district */
        loc_district_from_ward AS
        (
            SELECT
                REGION,
                DISTRICT,
                WARD,
                n_ward,
                ROW_NUMBER() OVER (PARTITION BY REGION, DISTRICT ORDER BY DISTRICT) rn
            FROM loc_base
        ),

        /* city text -> district */
        loc_district_from_city AS
        (
            SELECT
                REGION,
                DISTRICT,
                n_district,
                ROW_NUMBER() OVER (PARTITION BY REGION, DISTRICT ORDER BY DISTRICT) rn
            FROM loc_base
        ),

        /* region -> random district */
        loc_district_from_region AS
        (
            SELECT
                REGION,
                DISTRICT,
                ROW_NUMBER() OVER (PARTITION BY REGION ORDER BY DISTRICT) rn
            FROM loc_base
        ),

        /* -------------------------------------------------------
           district -> wards
        ------------------------------------------------------- */
        district_wards AS
        (
            SELECT DISTINCT
                   DISTRICT,
                   WARD,
                   ROW_NUMBER() OVER (PARTITION BY DISTRICT ORDER BY WARD) AS rn,
                   COUNT(*) OVER (PARTITION BY DISTRICT)                   AS total_wards
            FROM bank_location_lookup_v2
        ),

        /* -------------------------------------------------------
           region -> districts
        ------------------------------------------------------- */
        region_districts AS
        (
            SELECT
                REGION,
                DISTRICT,
                ROW_NUMBER() OVER (PARTITION BY REGION ORDER BY DISTRICT) AS rn,
                COUNT(*) OVER (PARTITION BY REGION)                       AS total_districts
            FROM bank_location_lookup_v2
            GROUP BY REGION, DISTRICT
        )

        SELECT
               CURRENT_TIMESTAMP                                                                                AS reportingDate,
               TRIM(c.cust_id)                                                                                  AS customerIdentificationNumber,
               COALESCE(NULLIF(TRIM(c.first_name), ''), 'N/A')                                                  AS firstName,
               COALESCE(NULLIF(TRIM(c.middle_name), ''), 'N/A')                                                 AS middleNames,
               COALESCE(NULLIF(TRIM(c.surname), ''), 'N/A')                                                     AS otherNames,
               TRIM(
                       CASE
                           WHEN c.cust_type = '1' THEN
                               TRIM(NVL(c.first_name, '')) || ' ' ||
                               TRIM(NVL(c.middle_name, '')) || ' ' ||
                               TRIM(NVL(c.surname, ''))
                           WHEN c.cust_type = '2' THEN TRIM(c.surname)
                           ELSE ''
                       END
               )                                                                                                AS fullNames,

               c.surname                                                                                        AS presentSurname,
               c.surname                                                                                        AS birthSurname,
               CASE
                   WHEN c.sex = 'M' THEN 'Male'
                   WHEN c.sex = 'F' THEN 'Female'
                   ELSE 'Not Applicable'
                   END                                                                                          AS gender,

               CASE UPPER(TRIM(gd_family.description))
                   WHEN 'MARRIED' THEN 'Married'
                   WHEN 'SINGLE' THEN 'Single'
                   WHEN 'DIVORCED' THEN 'Divorced'
                   WHEN 'WIDOWED' THEN 'Widowed'
                   ELSE 'Single'
                   END                                                                                          AS maritalStatus,
               NULL                                                                                             AS numberSpouse,
               CASE UPPER(TRIM(gd_natio.description)) WHEN 'TANZANIAN' THEN 'TANZANIA, UNITED REPUBLIC OF' END  AS nationality,
               CASE UPPER(TRIM(gd_natio.description)) WHEN 'TANZANIAN' THEN 'TANZANIA, UNITED REPUBLIC OF' END  AS citizenship,
               CASE
                   WHEN c.non_resident = '0' THEN 'Resident'
                   ELSE 'Non-Resident'
                   END                                                                                          AS residency,

               gd_proff.description                                                                             AS profession,
               'Households'                                                                                     AS sectorSnaClassification,
               CASE c.CUST_STATUS
                   WHEN '5' THEN 'Disappeared'
                   ELSE 'No Fate'
                   END                                                                                          AS fateStatus,
               'N/A'                                                                                            AS socialStatus,
               CASE UPPER(TRIM(gd_employment.description))
                   WHEN 'EMPLOYED' THEN 'Employed'
                   WHEN 'SALARIED' THEN 'Employed'
                   WHEN 'CUSTOMER SERVICE' THEN 'Self-employed'
                   ELSE 'Unemployed'
                   END                                                                                          AS employmentStatus,
               gd_customer_income.DESCRIPTION                                                                   AS monthlyIncome,
               (c.num_of_children + c.children_above18)                                                         AS numberDependants,
               gd_edulevel.description                                                                          AS educationLevel,
               0.00                                                                                             AS averageMonthlyExpenditure,
               c.blacklisted_ind                                                                                AS negativeClientStatus,
               c.spouse_name                                                                                    AS spousesFullName,
               NULL                                                                                             AS spouseIdentificationType,
               NULL                                                                                             AS spouseIdentificationNumber,
               NULL                                                                                             AS maidenName,
               NULL                                                                                             AS monthlyExpenses,
               c.date_of_birth                                                                                  AS birthDate,
               CASE UPPER(TRIM(id_country.description)) WHEN 'TANZANIA' THEN 'TANZANIA, UNITED REPUBLIC OF' 
                    ELSE id_country.description END                                                             AS birthCountry,
               NULL                                                                                             AS birthPostalCode,
               NULL                                                                                             AS birthHouseNumber,

               COALESCE(
                       loc_region_birth_city.REGION,
                       loc_birth_region_from_district.REGION,
                       loc_birth_region_from_ward.REGION,
                       loc_region_city.REGION,
                       loc_region_dist.REGION,
                       loc_region_from_district2.REGION,
                       loc_region_from_ward2.REGION,
                       'Dar es Salaam'
               )                                                                                                AS birthRegion,

               birth_district_pick.DISTRICT                                                                     AS birthDistrict,
               NULL                                                                                             AS birthWard,
               NULL                                                                                             AS birthStreet,

               CASE UPPER(TRIM(idt.description))
                   WHEN 'COMPANYS REGISTRY NUMBER' THEN 'Certificate of Registration'
                   WHEN 'DRIVING LICENCE' THEN 'DrivingLicense'
                   WHEN 'NATIONAL IDENTITY CARD' THEN 'NationalIdentityCard'
                   WHEN 'PASSPORT' THEN 'Passport'
                   WHEN 'STUDENT ID' THEN 'Student ID'
                   WHEN 'VOTERS ID' THEN 'VotersRegistrationCard'
                   ELSE 'N/A'
                   END                                                                                          AS identificationType,
               id.id_no                                                                                         AS identificationNumber,
               CASE
                   WHEN id.issue_date = DATE '0001-01-01'
                       THEN 'N/A'
                   ELSE TO_CHAR(id.issue_date, 'YYYY-MM-DD')
                   END                                                                                          AS issuanceDate,
               CASE
                   WHEN id.expiry_date = DATE '0001-01-01'
                       THEN 'N/A'
                   ELSE TO_CHAR(id.expiry_date, 'YYYY-MM-DD')
                   END                                                                                          AS expirationDate,
               'N/A'                                                                                            AS issuancePlace,

               CASE UPPER(TRIM(idt.description))
                   WHEN 'COMPANYS REGISTRY NUMBER' THEN 'Business Registrations and Licensing Agency (BRELA)'
                   WHEN 'DRIVING LICENCE' THEN 'Tanzania Revenue Authority (TRA)'
                   WHEN 'NATIONAL IDENTITY CARD' THEN 'National Identification Authority (NIDA)'
                   WHEN 'PASSPORT' THEN 'Immigration Services Department'
                   WHEN 'STUDENT ID' THEN 'Recognized Education Institution'
                   WHEN 'VOTERS ID' THEN 'Independent National Electoral Commission (INEC)'
                   END                                                                                          AS issuingAuthority,
               NULL                                                                                             AS businessName,
               NULL                                                                                             AS establishmentDate,
               NULL                                                                                             AS businessRegistrationNumber,
               NULL                                                                                             AS businessRegistrationDate,
               NULL                                                                                             AS businessLicenseNumber,
               NULL                                                                                             AS taxIdentificationNumber,
               NULL                                                                                             AS employerName,
               NULL                                                                                             AS employerRegion,
               NULL                                                                                             AS employerDistrict,
               NULL                                                                                             AS employerWard,
               NULL                                                                                             AS employerStreet,
               NULL                                                                                             AS employerHouseNumber,
               NULL                                                                                             AS employerPostalCode,
               NULL                                                                                             AS businessNature,
               c.mobile_tel                                                                                     AS mobileNumber,
               c.mobile_tel2                                                                                    AS alternativeMobileNumber,
               c.telephone_1                                                                                    AS fixedLineNumber,
               c_address.fax_no                                                                                 AS faxNumber,
               c.e_mail                                                                                         AS emailAddress,
               c.internet_address                                                                               AS socialMedia,
               ward_selection.WARD                                                                              AS mainAddress,
               NULL                                                                                             AS street,
               NULL                                                                                             AS houseNumber,
               ward_selection.WARD                                                                              AS postalCode,

               COALESCE(
                       loc_region_city.REGION,
                       loc_region_dist.REGION,
                       loc_region_from_district2.REGION,
                       loc_region_from_ward2.REGION,
                       'Dar es Salaam'
               )                                                                                                AS region,

               COALESCE(
                       loc_district_region.DISTRICT,
                       loc_district_from_ward.DISTRICT,
                       loc_district_from_city.DISTRICT,
                       loc_district_from_region.DISTRICT
               )                                                                                                AS district,

               ward_selection.WARD                                                                              AS ward,
               c_country.description                                                                            AS country,
               NULL                                                                                             AS sstreet,
               NULL                                                                                             AS shouseNumber,
               NULL                                                                                             AS spostalCode,
               NULL                                                                                             AS sregion,
               NULL                                                                                             AS sdistrict,
               NULL                                                                                             AS sward,
               NULL                                                                                             AS scountry,
               
               -- Add cust_id for cursor tracking (use same format as customerIdentificationNumber)
               TRIM(c.cust_id)                                                                                  AS cust_id

        FROM customer c

        LEFT JOIN cust_address c_address
               ON c_address.fk_customercust_id = c.cust_id
              AND c_address.communication_addr = '1'
              AND c_address.entry_status = '1'

        /* ---------------- birth place → region (OPTIMIZED) ---------------- */

        LEFT JOIN loc_region loc_region_birth_city
           ON REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(c.BIRTHPLACE)),' ',''),'-',''),'_',''),',','')
              LIKE '%' || loc_region_birth_city.n_region || '%'
          AND loc_region_birth_city.rn = 1

        LEFT JOIN loc_region_from_district loc_birth_region_from_district
           ON loc_region_birth_city.REGION IS NULL
          AND REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(c.BIRTHPLACE)),' ',''),'-',''),'_',''),',','')
              LIKE '%' || loc_birth_region_from_district.n_district || '%'
          AND loc_birth_region_from_district.rn = 1

        LEFT JOIN loc_region_from_ward loc_birth_region_from_ward
           ON loc_region_birth_city.REGION IS NULL
          AND loc_birth_region_from_district.REGION IS NULL
          AND REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(c.BIRTHPLACE)),' ',''),'-',''),'_',''),',','')
              LIKE '%' || loc_birth_region_from_ward.n_ward || '%'
          AND loc_birth_region_from_ward.rn = 1

        /* ---------------- current address → region (OPTIMIZED) ---------------- */

        LEFT JOIN loc_region loc_region_city
           ON REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(c_address.CITY)),' ',''),'-',''),'_',''),',','')
              LIKE '%' || loc_region_city.n_region || '%'
          AND loc_region_city.rn = 1

        LEFT JOIN loc_region loc_region_dist
           ON loc_region_city.REGION IS NULL
          AND REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(c_address.REGION)),' ',''),'-',''),'_',''),',','')
              LIKE '%' || loc_region_dist.n_region || '%'
          AND loc_region_dist.rn = 1

        LEFT JOIN loc_region_from_district loc_region_from_district2
           ON loc_region_city.REGION IS NULL
          AND loc_region_dist.REGION IS NULL
          AND REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(c_address.REGION)),' ',''),'-',''),'_',''),',','')
              LIKE '%' || loc_region_from_district2.n_district || '%'
          AND loc_region_from_district2.rn = 1

        LEFT JOIN loc_region_from_ward loc_region_from_ward2
           ON loc_region_city.REGION IS NULL
          AND loc_region_dist.REGION IS NULL
          AND loc_region_from_district2.REGION IS NULL
          AND REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(c_address.ADDRESS_1)),' ',''),'-',''),'_',''),',','')
              LIKE '%' || loc_region_from_ward2.n_ward || '%'
          AND loc_region_from_ward2.rn = 1

        /* ---------------- region → district (OPTIMIZED) ---------------- */

        LEFT JOIN loc_district_region
           ON loc_district_region.rn = 1
          AND loc_district_region.REGION =
               COALESCE(
                   loc_region_city.REGION,
                   loc_region_dist.REGION,
                   loc_region_from_district2.REGION,
                   loc_region_from_ward2.REGION
               )
          AND REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(c_address.REGION)),' ',''),'-',''),'_',''),',','')
              LIKE '%' || loc_district_region.n_district || '%'

        LEFT JOIN loc_district_from_ward
           ON loc_district_region.DISTRICT IS NULL
          AND loc_district_from_ward.rn = 1
          AND loc_district_from_ward.REGION =
               COALESCE(
                   loc_region_city.REGION,
                   loc_region_dist.REGION,
                   loc_region_from_district2.REGION,
                   loc_region_from_ward2.REGION
               )
          AND REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(c_address.ADDRESS_1)),' ',''),'-',''),'_',''),',','')
              LIKE '%' || loc_district_from_ward.n_ward || '%'

        LEFT JOIN loc_district_from_city
           ON loc_district_region.DISTRICT IS NULL
          AND loc_district_from_ward.DISTRICT IS NULL
          AND loc_district_from_city.rn = 1
          AND loc_district_from_city.REGION =
               COALESCE(
                   loc_region_city.REGION,
                   loc_region_dist.REGION,
                   loc_region_from_district2.REGION,
                   loc_region_from_ward2.REGION
               )
          AND REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(c_address.CITY)),' ',''),'-',''),'_',''),',','')
              LIKE '%' || loc_district_from_city.n_district || '%'

        LEFT JOIN loc_district_from_region
           ON loc_district_region.DISTRICT IS NULL
          AND loc_district_from_ward.DISTRICT IS NULL
          AND loc_district_from_city.DISTRICT IS NULL
          AND loc_district_from_region.rn = 1
          AND loc_district_from_region.REGION =
               COALESCE(
                   loc_region_city.REGION,
                   loc_region_dist.REGION,
                   loc_region_from_district2.REGION,
                   loc_region_from_ward2.REGION,
                   'Dar es Salaam'
               )

        LEFT JOIN district_wards ward_selection
           ON ward_selection.DISTRICT =
              COALESCE(
                  loc_district_region.DISTRICT,
                  loc_district_from_ward.DISTRICT,
                  loc_district_from_city.DISTRICT,
                  loc_district_from_region.DISTRICT,
                  'Dar es Salaam'
              )
          AND ward_selection.rn =
              MOD(ASCII(SUBSTR(TRIM(c.CUST_ID), 1, 1)), ward_selection.total_wards) + 1

        LEFT JOIN region_districts birth_district_pick
           ON birth_district_pick.REGION =
              COALESCE(
                  loc_region_birth_city.REGION,
                  loc_birth_region_from_district.REGION,
                  loc_birth_region_from_ward.REGION,
                  loc_region_city.REGION,
                  loc_region_dist.REGION,
                  loc_region_from_district2.REGION,
                  loc_region_from_ward2.REGION,
                  'Dar es Salaam'
              )
          AND birth_district_pick.rn =
              MOD(
                  ASCII(SUBSTR(TRIM(c.CUST_ID), 1, 1)),
                  birth_district_pick.total_districts
              ) + 1

        /* ---------------- remaining joins (unchanged) ---------------- */

        LEFT JOIN generic_detail c_country
               ON c_address.fkgd_has_country = c_country.serial_num
              AND c_address.fkgh_has_country = c_country.fk_generic_headpar

        LEFT JOIN other_id id
               ON id.fk_customercust_id = c.cust_id
              AND (CASE WHEN id.serial_no IS NULL THEN '1' ELSE id.main_flag END) = '1'

        LEFT JOIN generic_detail id_country
               ON id.fkgh_has_been_issu = id_country.fk_generic_headpar
              AND id.fkgd_has_been_issu = id_country.serial_num

        LEFT JOIN generic_detail idt
               ON idt.fk_generic_headpar = id.fkgh_has_type
              AND idt.serial_num = id.fkgd_has_type

        LEFT JOIN customer_category cc_family
               ON cc_family.fk_customercust_id = c.cust_id
              AND cc_family.fk_categorycategor = 'FAMILY'
              AND cc_family.fk_generic_detafk = 'FALST'
        LEFT JOIN generic_detail gd_family
               ON gd_family.fk_generic_headpar = cc_family.fk_generic_detafk
              AND gd_family.serial_num = cc_family.fk_generic_detaser

        LEFT JOIN customer_category cc_employment
               ON cc_employment.fk_customercust_id = c.cust_id
              AND cc_employment.fk_categorycategor = 'PROFLEVL'
              AND cc_employment.fk_generic_detafk = 'PRFST'
        LEFT JOIN generic_detail gd_employment
               ON gd_employment.fk_generic_headpar = cc_employment.fk_generic_detafk
              AND gd_employment.serial_num = cc_employment.fk_generic_detaser

        LEFT JOIN customer_category cc_natio
               ON cc_natio.fk_customercust_id = c.cust_id
              AND cc_natio.fk_categorycategor = 'NATIONAL'
              AND cc_natio.fk_generic_detafk = 'NATIO'
        LEFT JOIN generic_detail gd_natio
               ON gd_natio.fk_generic_headpar = cc_natio.fk_generic_detafk
              AND gd_natio.serial_num = cc_natio.fk_generic_detaser

        LEFT JOIN customer_category cc_citiz
               ON cc_citiz.fk_customercust_id = c.cust_id
              AND cc_citiz.fk_categorycategor = 'CITIZEN'
              AND cc_citiz.fk_generic_detafk = 'CITIZ'
        LEFT JOIN generic_detail gd_citiz
               ON gd_citiz.fk_generic_headpar = cc_citiz.fk_generic_detafk
              AND gd_citiz.serial_num = cc_citiz.fk_generic_detaser

        LEFT JOIN customer_category cc_proff
               ON cc_proff.fk_customercust_id = c.cust_id
              AND cc_proff.fk_categorycategor = 'PROFES'
              AND cc_proff.fk_generic_detafk = 'PROFF'
        LEFT JOIN generic_detail gd_proff
               ON gd_proff.fk_generic_headpar = cc_proff.fk_generic_detafk
              AND gd_proff.serial_num = cc_proff.fk_generic_detaser

        LEFT JOIN customer_category cc_edulevel
               ON cc_edulevel.fk_customercust_id = c.cust_id
              AND cc_edulevel.fk_categorycategor = 'EDULEVEL'
              AND cc_edulevel.fk_generic_detafk = 'EDULV'
        LEFT JOIN generic_detail gd_edulevel
               ON gd_edulevel.fk_generic_headpar = cc_edulevel.fk_generic_detafk
              AND gd_edulevel.serial_num = cc_edulevel.fk_generic_detaser

        LEFT JOIN customer_category customer_income
               ON customer_income.fk_customercust_id = c.cust_id
              AND customer_income.fk_categorycategor = 'INCLEVEL'
              AND customer_income.fk_generic_detafk = 'INCLV'
        LEFT JOIN generic_detail gd_customer_income
               ON gd_customer_income.fk_generic_headpar = customer_income.fk_generic_detafk
              AND gd_customer_income.serial_num = customer_income.fk_generic_detaser

        {where_clause}
        ORDER BY c.cust_id
        FETCH FIRST {self.batch_size} ROWS ONLY
        """
        
        return query
    
    def get_total_count_query(self):
        """Get total count of available personal data records"""
        
        query = """
        SELECT COUNT(*) as total_count
        FROM customer c
        WHERE c.CUST_TYPE = '1'
        """
        
        return query
    
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
    
    def setup_rabbitmq_queue(self):
        """Setup RabbitMQ queue for personal data"""
        try:
            credentials = pika.PlainCredentials(
                self.config.message_queue.rabbitmq_user,
                self.config.message_queue.rabbitmq_password
            )
            parameters = pika.ConnectionParameters(
                host=self.config.message_queue.rabbitmq_host,
                port=self.config.message_queue.rabbitmq_port,
                credentials=credentials
            )
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            
            # Declare personal data queue first
            channel.queue_declare(queue='personal_data_optimized_queue', durable=True)
            
            # Then try to purge existing queue
            try:
                channel.queue_purge('personal_data_optimized_queue')
                self.logger.info("🧹 Purged existing optimized queue")
            except:
                pass
            
            connection.close()
            self.logger.info("✅ RabbitMQ personal data OPTIMIZED queue ready")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to setup RabbitMQ queue: {e}")
            raise
    
    def producer_thread(self):
        """Producer thread - fetches personal data and publishes to queue"""
        try:
            # Skip count query for now - just start processing
            self.logger.info("🏭 OPTIMIZED Producer thread started")
            self.logger.info("📊 Processing personal data records in batches with OPTIMIZED CTEs...")
            
            # Process batches - cursor-based approach for large dataset
            batch_number = 1
            processed_count = 0
            last_cust_id = None
            
            while True:  # Process until no more data
                # Setup RabbitMQ connection for each batch to avoid timeouts
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
                        blocked_connection_timeout=300  # 5 minutes timeout
                    )
                    connection = pika.BlockingConnection(parameters)
                    channel = connection.channel()
                    
                    # Fetch batch using cursor with OPTIMIZED query
                    with self.db2_conn.get_connection() as conn:
                        cursor = conn.cursor()
                        batch_query = self.get_personal_data_optimized_query(last_cust_id)
                        cursor.execute(batch_query)
                        rows = cursor.fetchall()
                    
                    if not rows:
                        connection.close()
                        break
                    
                    self.logger.info(f"🏭 OPTIMIZED Producer: Batch {batch_number} - {len(rows)} personal data records")
                    
                    # Show sample data for first batch
                    if batch_number == 1:
                        self.logger.info("📋 Sample OPTIMIZED personal data from first batch:")
                        for i, row in enumerate(rows[:3], 1):
                            cust_id = row[1] if row[1] is not None else "N/A"
                            first_name = row[2] if row[2] is not None else "N/A"
                            surname = row[4] if row[4] is not None else "N/A"
                            self.logger.info(f"  {i}. Customer: {cust_id}, Name: {first_name} {surname}")
                    
                    # Process and publish batch
                    batch_published = 0
                    for row in rows:
                        # Update cursor for each row (like agents pipeline)
                        last_cust_id = row[-1]  # cust_id (last column using negative index)
                        
                        record = self.personal_data_processor.process_record(row, 'personalData')
                        
                        if self.personal_data_processor.validate_record(record):
                            message = json.dumps(asdict(record), default=str)
                            channel.basic_publish(
                                exchange='',
                                routing_key='personal_data_optimized_queue',
                                body=message,
                                properties=pika.BasicProperties(delivery_mode=2)
                            )
                            self.total_produced += 1
                            batch_published += 1
                    
                    # Close connection after batch
                    connection.close()
                    
                    self.logger.info(f"🔄 OPTIMIZED Cursor updated: last_cust_id={last_cust_id}")
                    self.logger.info(f"🏭 OPTIMIZED Producer: Published batch {batch_number} ({batch_published} records, {self.total_produced} total)")
                    
                    processed_count += len(rows)
                    batch_number += 1
                    
                    # Break if we got less than batch_size (end of data)
                    if len(rows) < self.batch_size:
                        break
                    
                    # Small delay between batches
                    time.sleep(0.5)
                    
                except Exception as batch_error:
                    self.logger.error(f"❌ OPTIMIZED Batch {batch_number} error: {batch_error}")
                    # Continue to next batch
                    processed_count += self.batch_size  # Skip this batch
                    batch_number += 1
                    time.sleep(2)  # Wait before retry
            
            self.logger.info(f"🏭 OPTIMIZED Producer finished: {self.total_produced} personal data records published")
            self.producer_finished.set()
            
        except Exception as e:
            self.logger.error(f"❌ OPTIMIZED Producer error: {e}")
            import traceback
            traceback.print_exc()
            self.producer_finished.set()
    
    def consumer_thread(self):
        """Consumer thread - processes personal data messages from queue"""
        try:
            self.logger.info("👤 OPTIMIZED Consumer thread started")
            
            # Setup RabbitMQ connection for consumer with better timeout settings
            credentials = pika.PlainCredentials(
                self.config.message_queue.rabbitmq_user,
                self.config.message_queue.rabbitmq_password
            )
            parameters = pika.ConnectionParameters(
                host=self.config.message_queue.rabbitmq_host,
                port=self.config.message_queue.rabbitmq_port,
                credentials=credentials,
                heartbeat=600,  # 10 minutes heartbeat
                blocked_connection_timeout=300  # 5 minutes timeout
            )
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            
            def process_message(ch, method, properties, body):
                try:
                    record_data = json.loads(body)
                    record = PersonalDataRecord(**record_data)
                    
                    # Insert to PostgreSQL
                    with self.get_postgres_connection() as conn:
                        cursor = conn.cursor()
                        self.personal_data_processor.insert_to_postgres(record, cursor)
                        conn.commit()
                    
                    self.total_consumed += 1
                    
                    if self.total_consumed % self.batch_size == 0:
                        self.logger.info(f"👤 OPTIMIZED Consumer: Processed {self.total_consumed} personal data records")
                    
                    # Acknowledge message
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    
                except Exception as e:
                    self.logger.error(f"❌ OPTIMIZED Consumer error processing personal data message: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            
            # Set QoS for controlled processing
            channel.basic_qos(prefetch_count=10)  # Process 10 messages at a time
            channel.basic_consume(queue='personal_data_optimized_queue', on_message_callback=process_message)
            
            # Keep consuming until producer is done and queue is empty
            while not self.stop_consumer.is_set():
                try:
                    connection.process_data_events(time_limit=1)
                    
                    # Check if we should stop
                    if self.producer_finished.is_set():
                        # Producer is done, check if queue is empty
                        try:
                            method = channel.queue_declare(queue='personal_data_optimized_queue', durable=True, passive=True)
                            if method.method.message_count == 0:
                                self.logger.info("👤 OPTIMIZED Consumer: Queue empty, producer finished")
                                break
                        except:
                            # If queue check fails, assume we should stop
                            break
                        
                except Exception as e:
                    self.logger.error(f"❌ OPTIMIZED Consumer processing error: {e}")
                    # Try to reconnect
                    try:
                        connection.close()
                    except:
                        pass
                    time.sleep(2)
                    try:
                        connection = pika.BlockingConnection(parameters)
                        channel = connection.channel()
                        channel.basic_qos(prefetch_count=10)
                        channel.basic_consume(queue='personal_data_optimized_queue', on_message_callback=process_message)
                    except Exception as reconnect_error:
                        self.logger.error(f"❌ Failed to reconnect: {reconnect_error}")
                        break
            
            try:
                connection.close()
            except:
                pass
            self.logger.info(f"👤 OPTIMIZED Consumer finished: {self.total_consumed} personal data records processed")
            self.consumer_finished.set()
            
        except Exception as e:
            self.logger.error(f"❌ OPTIMIZED Consumer error: {e}")
            import traceback
            traceback.print_exc()
            self.consumer_finished.set()
    
    def run_streaming_pipeline(self):
        """Run the OPTIMIZED streaming pipeline with simultaneous producer and consumer"""
        self.logger.info("🚀 Starting OPTIMIZED STREAMING personal data pipeline...")
        
        try:
            # Setup queue
            self.setup_rabbitmq_queue()
            
            # Start consumer thread first
            consumer_thread = threading.Thread(target=self.consumer_thread, name="PersonalData-OPTIMIZED-Consumer")
            consumer_thread.start()
            
            # Small delay to let consumer start
            time.sleep(1)
            
            # Start producer thread
            producer_thread = threading.Thread(target=self.producer_thread, name="PersonalData-OPTIMIZED-Producer")
            producer_thread.start()
            
            # Wait for producer to finish
            producer_thread.join()
            self.logger.info("✅ OPTIMIZED Producer thread completed")
            
            # Wait for consumer to finish processing remaining messages
            consumer_thread.join(timeout=30)  # 30 second timeout
            
            if consumer_thread.is_alive():
                self.logger.warning("⚠️ OPTIMIZED Consumer thread timeout, stopping...")
                self.stop_consumer.set()
                consumer_thread.join(timeout=5)
            
            self.logger.info("✅ OPTIMIZED Consumer thread completed")
            
            self.logger.info(f"📊 OPTIMIZED STREAMING Personal Data Pipeline Results:")
            self.logger.info(f"   Produced: {self.total_produced:,} records")
            self.logger.info(f"   Consumed: {self.total_consumed:,} records")
            
            return self.total_consumed
            
        except Exception as e:
            self.logger.error(f"❌ OPTIMIZED Streaming personal data pipeline failed: {e}")
            raise

def main():
    """Main function"""
    print("👤 PERSONAL DATA OPTIMIZED STREAMING PIPELINE")
    print("=" * 70)
    print("📋 Features:")
    print("  - Producer and Consumer run SIMULTANEOUSLY")
    print("  - Real-time processing as data arrives")
    print("  - Minimal queue accumulation")
    print("  - Batch size: 1000 records per batch (MAX PERFORMANCE)")
    print("  - camelCase table: personalData")
    print("  - camelCase field names")
    print("  - Uses personal-data-optimized.sql query with CTEs")
    print("  - OPTIMIZED location mapping with pre-calculated CTEs")
    print("  - Customer-based cursor pagination")
    print("  - ⚡ PERFORMANCE OPTIMIZED with CTEs")
    print("=" * 70)
    
    pipeline = PersonalDataOptimizedStreamingPipeline(1000)  # Use maximum batch size for performance
    
    try:
        count = pipeline.run_streaming_pipeline()
        
        print("\n" + "=" * 70)
        print("✅ OPTIMIZED STREAMING PERSONAL DATA PIPELINE COMPLETED!")
        print(f"📊 Total personal data records processed: {count:,}")
        print("🔍 Key advantages:")
        print("  - Real-time processing (no queue buildup)")
        print("  - Producer and consumer worked simultaneously")
        print("  - Memory efficient")
        print("  - ⚡ OPTIMIZED with CTEs for better performance")
        print("  - camelCase naming throughout")
        print("  - Unique customer identification")
        print("=" * 70)
        
    except Exception as e:
        print(f"\n❌ OPTIMIZED Streaming personal data pipeline failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()