#!/usr/bin/env python3
"""
Personal Data Streaming Pipeline - Producer and Consumer run simultaneously
Based on personal_data_information-v3.sql
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
class PersonalDataRecord:
    reportingDate: str
    customerIdentificationNumber: str
    firstName: str
    middleNames: str
    otherNames: str
    fullNames: str
    presentSurname: str
    birthSurname: str
    gender: str
    maritalStatus: str
    numberSpouse: Optional[str]
    nationality: Optional[str]
    citizenship: Optional[str]
    residency: str
    profession: Optional[str]
    sectorSnaClassification: str
    fateStatus: str
    socialStatus: str
    employmentStatus: str
    monthlyIncome: Optional[str]
    numberDependants: Optional[int]
    educationLevel: str
    averageMonthlyExpenditure: Decimal
    negativeClientStatus: Optional[str]
    spousesFullName: Optional[str]
    spouseIdentificationType: Optional[str]
    spouseIdentificationNumber: Optional[str]
    maidenName: Optional[str]
    monthlyExpenses: Optional[Decimal]
    birthDate: Optional[str]
    birthCountry: Optional[str]
    birthPostalCode: Optional[str]
    birthHouseNumber: Optional[str]
    birthRegion: str
    birthDistrict: Optional[str]
    birthWard: Optional[str]
    birthStreet: Optional[str]
    identificationType: str
    identificationNumber: str
    issuanceDate: Optional[str]
    expirationDate: Optional[str]
    issuancePlace: str
    issuingAuthority: Optional[str]
    businessName: Optional[str]
    establishmentDate: Optional[str]
    businessRegistrationNumber: Optional[str]
    businessRegistrationDate: Optional[str]
    businessLicenseNumber: Optional[str]
    taxIdentificationNumber: Optional[str]
    employerName: Optional[str]
    employerRegion: Optional[str]
    employerDistrict: Optional[str]
    employerWard: Optional[str]
    employerStreet: Optional[str]
    employerHouseNumber: Optional[str]
    employerPostalCode: Optional[str]
    businessNature: Optional[str]
    mobileNumber: Optional[str]
    alternativeMobileNumber: Optional[str]
    fixedLineNumber: Optional[str]
    faxNumber: Optional[str]
    emailAddress: Optional[str]
    socialMedia: Optional[str]
    mainAddress: str
    street: Optional[str]
    houseNumber: Optional[str]
    postalCode: str
    region: str
    district: Optional[str]
    ward: str
    country: Optional[str]
    sstreet: Optional[str]
    shouseNumber: Optional[str]
    spostalCode: Optional[str]
    sregion: Optional[str]
    sdistrict: Optional[str]
    sward: Optional[str]
    scountry: Optional[str]


class PersonalDataStreamingPipeline:
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
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        self.logger = logging.getLogger(__name__)

        self.logger.info("Personal Data STREAMING Pipeline initialized")
        self.logger.info(f"Batch size: {self.batch_size} records per batch")
        self.logger.info("Mode: Streaming (Producer + Consumer simultaneously)")

    def get_personal_data_query(self, last_cust_id=None):
        """Get the personal data query with cursor-based pagination - V3 with location mapping"""
        
        where_clause = """
        WHERE c.CUST_TYPE = '1'
            AND UPPER(TRIM(idt.description)) NOT IN
              ('OTHER TYPE OF IDENTIFICATION', 'BIRTH CERTIFICATE', 'N/A')
          AND NOT (
            UPPER(TRIM(idt.description)) = 'NATIONAL IDENTITY CARD'
                AND LENGTH(TRIM(id.id_no)) < 20
            )
          AND NOT (
            UPPER(TRIM(idt.description)) = 'DRIVING LICENCE'
                AND LENGTH(TRIM(id.id_no)) < 10
            )
          AND NOT (
            UPPER(TRIM(idt.description)) = 'VOTERS ID'
                AND UPPER(TRIM(id.id_no)) NOT LIKE 'T%'
            )
        """
        
        if last_cust_id:
            where_clause += f" AND c.cust_id > '{last_cust_id}'"
        
        query = f"""
        WITH district_wards AS (SELECT DISTINCT DISTRICT,
                                        WARD,
                                        ROW_NUMBER() OVER (PARTITION BY DISTRICT ORDER BY WARD) AS rn,
                                        COUNT(*) OVER (PARTITION BY DISTRICT)                   AS total_wards
                        FROM bank_location_lookup_v2)
           , region_districts AS (SELECT REGION,
                                         DISTRICT,
                                         ROW_NUMBER() OVER (PARTITION BY REGION ORDER BY DISTRICT) AS rn,
                                         COUNT(*) OVER (PARTITION BY REGION)                       AS total_districts
                                  FROM bank_location_lookup_v2
                                  GROUP BY REGION, DISTRICT)
        SELECT CURRENT_TIMESTAMP                                                                                AS reportingDate,
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
               COALESCE(gd_edulevel.description, 'Basic')                                                       AS educationLevel,
               0.00                                                                                             AS averageMonthlyExpenditure,
               c.blacklisted_ind                                                                                AS negativeClientStatus,
               c.spouse_name                                                                                    AS spousesFullName,
               NULL                                                                                             AS spouseIdentificationType,
               NULL                                                                                             AS spouseIdentificationNumber,
               NULL                                                                                             AS maidenName,
               NULL                                                                                             AS monthlyExpenses,
               c.date_of_birth                                                                                  AS birthDate,
               CASE UPPER(TRIM(id_country.description)) WHEN 'TANZANIA' THEN 'TANZANIA, UNITED REPUBLIC OF' END AS birthCountry,
               NULL                                                                                             AS birthPostalCode,
               NULL                                                                                             AS birthHouseNumber,
               COALESCE(
                       loc_region_birth_city.REGION,
                       loc_birth_region_from_district.REGION,
                       loc_birth_region_from_ward.REGION,
                       loc_region_city.REGION,
                       loc_region_dist.REGION,
                       loc_region_from_district.REGION,
                       loc_region_from_ward.REGION,
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
                   ELSE 'NationalIdentityCard'
                   END                                                                                          AS identificationType,
               id.id_no                                                                                         AS identificationNumber,
               CASE
                   WHEN id.issue_date = DATE '0001-01-01'
                       THEN null
                   ELSE TO_CHAR(id.issue_date, 'YYYY-MM-DD')
                   END                                                                                          AS issuance_date,
               CASE
                   WHEN id.expiry_date = DATE '0001-01-01'
                       THEN null
                   ELSE TO_CHAR(id.expiry_date, 'YYYY-MM-DD')
                   END                                                                                          AS expirationDate,
               'Dar es Salaam'                                                                                  AS issuancePlace,
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
                       loc_region_from_district.REGION,
                       loc_region_from_ward.REGION,
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
               c.cust_id                                                                                        AS cursor_cust_id
        FROM customer c
                 LEFT JOIN cust_address c_address
                           ON c_address.fk_customercust_id = c.cust_id
                               AND c_address.communication_addr = '1'
                               AND c_address.entry_status = '1'
                 LEFT JOIN (SELECT REGION,
                                   ROW_NUMBER() OVER (PARTITION BY REGION ORDER BY REGION) AS rn
                            FROM bank_location_lookup_v2) loc_region_birth_city
                           ON REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(C.BIRTHPLACE)), ' ', ''), '-', ''), '_', ''), ',', '')
                                  LIKE '%' || REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(loc_region_birth_city.REGION)), ' ', ''), '-', ''), '_', ''), ',', '') || '%'
                               AND loc_region_birth_city.rn = 1
                 LEFT JOIN (SELECT REGION, DISTRICT,
                                   ROW_NUMBER() OVER (PARTITION BY DISTRICT ORDER BY REGION) AS rn
                            FROM bank_location_lookup_v2) loc_birth_region_from_district
                           ON loc_region_birth_city.REGION IS NULL
                               AND REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(c.BIRTHPLACE)), ' ', ''), '-', ''), '_', ''), ',', '')
                                  LIKE '%' || REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(loc_birth_region_from_district.DISTRICT)), ' ', ''), '-', ''), '_', ''), ',', '') || '%'
                               AND loc_birth_region_from_district.rn = 1
                 LEFT JOIN (SELECT REGION, WARD,
                                   ROW_NUMBER() OVER (PARTITION BY WARD ORDER BY REGION) AS rn
                            FROM bank_location_lookup_v2) loc_birth_region_from_ward
                           ON loc_region_birth_city.REGION IS NULL
                               AND loc_birth_region_from_district.REGION IS NULL
                               AND REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(c.BIRTHPLACE)), ' ', ''), '-', ''), '_', ''), ',', '')
                                  LIKE '%' || REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(loc_birth_region_from_ward.WARD)), ' ', ''), '-', ''), '_', ''), ',', '') || '%'
                               AND loc_birth_region_from_ward.rn = 1
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
                           ON loc_region_city.REGION IS NULL
                               AND loc_region_dist.REGION IS NULL
                               AND REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(c_address.REGION)), ' ', ''), '-', ''), '_', ''), ',', '')
                                  LIKE '%' || REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(loc_region_from_district.DISTRICT)), ' ', ''), '-', ''), '_', ''), ',', '') || '%'
                               AND loc_region_from_district.rn = 1
                 LEFT JOIN (SELECT REGION, WARD,
                                   ROW_NUMBER() OVER (PARTITION BY WARD ORDER BY REGION) AS rn
                            FROM bank_location_lookup_v2) loc_region_from_ward
                           ON loc_region_city.REGION IS NULL
                               AND loc_region_dist.REGION IS NULL
                               AND loc_region_from_district.REGION IS NULL
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
                           ON loc_district_region.DISTRICT IS NULL
                               AND loc_district_from_ward.rn = 1
                               AND COALESCE(loc_region_city.REGION, loc_region_dist.REGION, loc_region_from_district.REGION, loc_region_from_ward.REGION) = loc_district_from_ward.REGION
                               AND REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(c_address.ADDRESS_1)), ' ', ''), '-', ''), '_', ''), ',', '')
                                  LIKE '%' || REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(loc_district_from_ward.WARD)), ' ', ''), '-', ''), '_', ''), ',', '') || '%'
                 LEFT JOIN (SELECT REGION, DISTRICT,
                                   ROW_NUMBER() OVER (PARTITION BY REGION, DISTRICT ORDER BY DISTRICT) AS rn
                            FROM bank_location_lookup_v2) loc_district_from_city
                           ON loc_district_region.DISTRICT IS NULL
                               AND loc_district_from_ward.DISTRICT IS NULL
                               AND loc_district_from_city.rn = 1
                               AND COALESCE(loc_region_city.REGION, loc_region_dist.REGION, loc_region_from_district.REGION, loc_region_from_ward.REGION) = loc_district_from_city.REGION
                               AND REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(c_address.CITY)), ' ', ''), '-', ''), '_', ''), ',', '')
                                  LIKE '%' || REPLACE(REPLACE(REPLACE(REPLACE(UPPER(TRIM(loc_district_from_city.DISTRICT)), ' ', ''), '-', ''), '_', ''), ',', '') || '%'
                 LEFT JOIN (SELECT REGION, DISTRICT,
                                   ROW_NUMBER() OVER (PARTITION BY REGION ORDER BY DISTRICT ) AS rn
                            FROM bank_location_lookup_v2) loc_district_from_region
                           ON loc_district_region.DISTRICT IS NULL
                               AND loc_district_from_ward.DISTRICT IS NULL
                               AND loc_district_from_city.DISTRICT IS NULL
                               AND loc_district_from_region.rn = 1
                               AND loc_district_from_region.REGION = COALESCE(loc_region_city.REGION, loc_region_dist.REGION, loc_region_from_district.REGION, loc_region_from_ward.REGION, 'Dar es Salaam')
                 LEFT JOIN district_wards ward_selection
                           ON ward_selection.DISTRICT = COALESCE(loc_district_region.DISTRICT, loc_district_from_ward.DISTRICT, loc_district_from_city.DISTRICT, loc_district_from_region.DISTRICT, 'Dar es Salaam')
                               AND ward_selection.rn = MOD(ASCII(SUBSTR(TRIM(c.CUST_ID), 1, 1)), ward_selection.total_wards) + 1
                 LEFT JOIN region_districts birth_district_pick
                           ON birth_district_pick.REGION = COALESCE(loc_region_birth_city.REGION, loc_birth_region_from_district.REGION, loc_birth_region_from_ward.REGION, loc_region_city.REGION, loc_region_dist.REGION, loc_region_from_district.REGION, loc_region_from_ward.REGION, 'Dar es Salaam')
                               AND birth_district_pick.rn = MOD(ASCII(SUBSTR(TRIM(c.CUST_ID), 1, 1)), birth_district_pick.total_districts) + 1
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
        ORDER BY c.cust_id ASC
        FETCH FIRST {self.batch_size} ROWS ONLY
        """
        
        return query
    
    def get_total_count_query(self):
        """Get total count of available personal data records"""
        return """
        SELECT COUNT(*) as total_count
        FROM customer c
                 LEFT JOIN other_id id
                           ON id.fk_customercust_id = c.cust_id
                               AND (CASE WHEN id.serial_no IS NULL THEN '1' ELSE id.main_flag END) = '1'
                 LEFT JOIN generic_detail idt
                           ON idt.fk_generic_headpar = id.fkgh_has_type
                               AND idt.serial_num = id.fkgd_has_type
        WHERE c.CUST_TYPE = '1'
            AND UPPER(TRIM(idt.description)) NOT IN
              ('OTHER TYPE OF IDENTIFICATION', 'BIRTH CERTIFICATE', 'N/A')
          AND NOT (
            UPPER(TRIM(idt.description)) = 'NATIONAL IDENTITY CARD'
                AND LENGTH(TRIM(id.id_no)) < 20
            )
          AND NOT (
            UPPER(TRIM(idt.description)) = 'DRIVING LICENCE'
                AND LENGTH(TRIM(id.id_no)) < 10
            )
          AND NOT (
            UPPER(TRIM(idt.description)) = 'VOTERS ID'
                AND UPPER(TRIM(id.id_no)) NOT LIKE 'T%'
            )
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
        """Setup RabbitMQ queue for personal data"""
        try:
            connection, channel = self.setup_rabbitmq_connection()
            channel.queue_declare(queue="personal_data_queue", durable=True)
            connection.close()
            self.logger.info("RabbitMQ queue 'personal_data_queue' setup complete")
        except Exception as e:
            self.logger.error(f"Failed to setup RabbitMQ: {e}")
            raise

    def process_record(self, row):
        """Process a single record - returns None if record should be skipped"""
        # Remove cursor field (last one)
        row_data = row[:-1]
        
        # Check if we have enough fields
        if len(row_data) < 75:
            self.logger.warning(f"Record has only {len(row_data)} fields, expected at least 75. Skipping.")
            return None
        
        # Check if required fields (region, ward) are NULL
        # Region is at index 67, ward is at index 69 (0-indexed, after removing cursor)
        region = row_data[67] if len(row_data) > 67 else None
        ward = row_data[69] if len(row_data) > 69 else None
        
        if not region or str(region).strip() == '' or str(region).strip().upper() in ('NIL', 'NULL', 'NONE'):
            return None
        
        if not ward or str(ward).strip() == '' or str(ward).strip().upper() in ('NIL', 'NULL', 'NONE'):
            return None

        try:
            return PersonalDataRecord(
                reportingDate=str(row_data[0]) if row_data[0] else None,
                customerIdentificationNumber=str(row_data[1]).strip() if row_data[1] else None,
                firstName=str(row_data[2]).strip() if row_data[2] else 'N/A',
                middleNames=str(row_data[3]).strip() if row_data[3] else 'N/A',
                otherNames=str(row_data[4]).strip() if row_data[4] else 'N/A',
                fullNames=str(row_data[5]).strip() if row_data[5] else None,
                presentSurname=str(row_data[6]).strip() if row_data[6] else None,
                birthSurname=str(row_data[7]).strip() if row_data[7] else None,
                gender=str(row_data[8]).strip() if row_data[8] else 'Not Applicable',
                maritalStatus=str(row_data[9]).strip() if row_data[9] else 'Single',
                numberSpouse=str(row_data[10]).strip() if row_data[10] else None,
                nationality=str(row_data[11]).strip() if row_data[11] else None,
                citizenship=str(row_data[12]).strip() if row_data[12] else None,
                residency=str(row_data[13]).strip() if row_data[13] else 'Resident',
                profession=str(row_data[14]).strip() if row_data[14] else None,
                sectorSnaClassification=str(row_data[15]).strip() if row_data[15] else 'Households',
                fateStatus=str(row_data[16]).strip() if row_data[16] else 'No Fate',
                socialStatus=str(row_data[17]).strip() if row_data[17] else 'N/A',
                employmentStatus=str(row_data[18]).strip() if row_data[18] else 'Unemployed',
                monthlyIncome=str(row_data[19]).strip() if row_data[19] else None,
                numberDependants=int(row_data[20]) if row_data[20] else 0,
                educationLevel=str(row_data[21]).strip() if row_data[21] else 'Basic',
                averageMonthlyExpenditure=Decimal(str(row_data[22])) if row_data[22] else Decimal('0.00'),
                negativeClientStatus=str(row_data[23]).strip() if row_data[23] else None,
                spousesFullName=str(row_data[24]).strip() if row_data[24] else None,
                spouseIdentificationType=str(row_data[25]).strip() if row_data[25] else None,
                spouseIdentificationNumber=str(row_data[26]).strip() if row_data[26] else None,
                maidenName=str(row_data[27]).strip() if row_data[27] else None,
                monthlyExpenses=Decimal(str(row_data[28])) if row_data[28] else None,
                birthDate=str(row_data[29]) if row_data[29] else None,
                birthCountry=str(row_data[30]).strip() if row_data[30] else None,
                birthPostalCode=str(row_data[31]).strip() if row_data[31] else None,
                birthHouseNumber=str(row_data[32]).strip() if row_data[32] else None,
                birthRegion=str(row_data[33]).strip() if row_data[33] else 'Dar es Salaam',
                birthDistrict=str(row_data[34]).strip() if row_data[34] else None,
                birthWard=str(row_data[35]).strip() if row_data[35] else None,
                birthStreet=str(row_data[36]).strip() if row_data[36] else None,
                identificationType=str(row_data[37]).strip() if row_data[37] else 'NationalIdentityCard',
                identificationNumber=str(row_data[38]).strip() if row_data[38] else None,
                issuanceDate=str(row_data[39]).strip() if row_data[39] else None,
                expirationDate=str(row_data[40]).strip() if row_data[40] else None,
                issuancePlace=str(row_data[41]).strip() if row_data[41] else 'Dar es Salaam',
                issuingAuthority=str(row_data[42]).strip() if row_data[42] else None,
                businessName=str(row_data[43]).strip() if row_data[43] else None,
                establishmentDate=str(row_data[44]).strip() if row_data[44] else None,
                businessRegistrationNumber=str(row_data[45]).strip() if row_data[45] else None,
                businessRegistrationDate=str(row_data[46]).strip() if row_data[46] else None,
                businessLicenseNumber=str(row_data[47]).strip() if row_data[47] else None,
                taxIdentificationNumber=str(row_data[48]).strip() if row_data[48] else None,
                employerName=str(row_data[49]).strip() if row_data[49] else None,
                employerRegion=str(row_data[50]).strip() if row_data[50] else None,
                employerDistrict=str(row_data[51]).strip() if row_data[51] else None,
                employerWard=str(row_data[52]).strip() if row_data[52] else None,
                employerStreet=str(row_data[53]).strip() if row_data[53] else None,
                employerHouseNumber=str(row_data[54]).strip() if row_data[54] else None,
                employerPostalCode=str(row_data[55]).strip() if row_data[55] else None,
                businessNature=str(row_data[56]).strip() if row_data[56] else None,
                mobileNumber=str(row_data[57]).strip() if row_data[57] else None,
                alternativeMobileNumber=str(row_data[58]).strip() if row_data[58] else None,
                fixedLineNumber=str(row_data[59]).strip() if row_data[59] else None,
                faxNumber=str(row_data[60]).strip() if row_data[60] else None,
                emailAddress=str(row_data[61]).strip() if row_data[61] else None,
                socialMedia=str(row_data[62]).strip() if row_data[62] else None,
                mainAddress=str(row_data[63]).strip() if row_data[63] else None,
                street=str(row_data[64]).strip() if row_data[64] else None,
                houseNumber=str(row_data[65]).strip() if row_data[65] else None,
                postalCode=str(row_data[66]).strip() if row_data[66] else None,
                region=str(row_data[67]).strip(),  # NOT NULL - validated above
                district=str(row_data[68]).strip() if row_data[68] else None,
                ward=str(row_data[69]).strip(),  # NOT NULL - validated above
                country=str(row_data[70]).strip() if row_data[70] else None,
                sstreet=str(row_data[71]).strip() if row_data[71] else None,
                shouseNumber=str(row_data[72]).strip() if row_data[72] else None,
                spostalCode=str(row_data[73]).strip() if row_data[73] else None,
                sregion=str(row_data[74]).strip() if row_data[74] else None,
                sdistrict=str(row_data[75]).strip() if row_data[75] else None,
                sward=str(row_data[76]).strip() if row_data[76] else None,
                scountry=str(row_data[77]).strip() if row_data[77] else None,
            )
        except IndexError as e:
            self.logger.error(f"IndexError processing record: {e}. Row has {len(row_data)} fields")
            return None
        except Exception as e:
            self.logger.error(f"Error processing record: {e}")
            return None

    def insert_to_postgres(self, record: PersonalDataRecord, cursor):
        """Insert record to PostgreSQL"""
        insert_sql = """
        INSERT INTO "personalData" (
            "reportingDate", "customerIdentificationNumber", "firstName", "middleNames",
            "otherNames", "fullNames", "presentSurname", "birthSurname", gender,
            "maritalStatus", "numberSpouse", nationality, citizenship, residency,
            profession, "sectorSnaClassification", "fateStatus", "socialStatus",
            "employmentStatus", "monthlyIncome", "numberDependants", "educationLevel",
            "averageMonthlyExpenditure", "negativeClientStatus", "spousesFullName",
            "spouseIdentificationType", "spouseIdentificationNumber", "maidenName",
            "monthlyExpenses", "birthDate", "birthCountry", "birthPostalCode",
            "birthHouseNumber", "birthRegion", "birthDistrict", "birthWard",
            "birthStreet", "identificationType", "identificationNumber", "issuanceDate",
            "expirationDate", "issuancePlace", "issuingAuthority", "businessName",
            "establishmentDate", "businessRegistrationNumber", "businessRegistrationDate",
            "businessLicenseNumber", "taxIdentificationNumber", "employerName",
            "employerRegion", "employerDistrict", "employerWard", "employerStreet",
            "employerHouseNumber", "employerPostalCode", "businessNature", "mobileNumber",
            "alternativeMobileNumber", "fixedLineNumber", "faxNumber", "emailAddress",
            "socialMedia", "mainAddress", street, "houseNumber", "postalCode",
            region, district, ward, country, sstreet, "shouseNumber", "spostalCode",
            sregion, sdistrict, sward, scountry
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        cursor.execute(
            insert_sql,
            (
                record.reportingDate, record.customerIdentificationNumber, record.firstName, record.middleNames,
                record.otherNames, record.fullNames, record.presentSurname, record.birthSurname, record.gender,
                record.maritalStatus, record.numberSpouse, record.nationality, record.citizenship, record.residency,
                record.profession, record.sectorSnaClassification, record.fateStatus, record.socialStatus,
                record.employmentStatus, record.monthlyIncome, record.numberDependants, record.educationLevel,
                record.averageMonthlyExpenditure, record.negativeClientStatus, record.spousesFullName,
                record.spouseIdentificationType, record.spouseIdentificationNumber, record.maidenName,
                record.monthlyExpenses, record.birthDate, record.birthCountry, record.birthPostalCode,
                record.birthHouseNumber, record.birthRegion, record.birthDistrict, record.birthWard,
                record.birthStreet, record.identificationType, record.identificationNumber, record.issuanceDate,
                record.expirationDate, record.issuancePlace, record.issuingAuthority, record.businessName,
                record.establishmentDate, record.businessRegistrationNumber, record.businessRegistrationDate,
                record.businessLicenseNumber, record.taxIdentificationNumber, record.employerName,
                record.employerRegion, record.employerDistrict, record.employerWard, record.employerStreet,
                record.employerHouseNumber, record.employerPostalCode, record.businessNature, record.mobileNumber,
                record.alternativeMobileNumber, record.fixedLineNumber, record.faxNumber, record.emailAddress,
                record.socialMedia, record.mainAddress, record.street, record.houseNumber, record.postalCode,
                record.region, record.district, record.ward, record.country, record.sstreet, record.shouseNumber,
                record.spostalCode, record.sregion, record.sdistrict, record.sward, record.scountry
            ),
        )

    def producer_thread(self):
        """Producer thread - reads from DB2 and sends to RabbitMQ"""
        try:
            self.logger.info("Producer thread started")

            # Get total count
            with self.db2_conn.get_connection(log_connection=True) as conn:
                cursor = conn.cursor()
                cursor.execute(self.get_total_count_query())
                self.total_available = cursor.fetchone()[0]

            self.logger.info(f"Total personal data records available: {self.total_available:,}")

            # Setup RabbitMQ
            connection, channel = self.setup_rabbitmq_connection()

            # Process batches
            batch_number = 1
            last_cust_id = None
            last_progress_report = time.time()

            while True:
                batch_start_time = time.time()

                # Fetch batch
                rows = None
                for attempt in range(self.max_retries):
                    try:
                        with self.db2_conn.get_connection(log_connection=False) as conn:
                            cursor = conn.cursor()
                            batch_query = self.get_personal_data_query(last_cust_id)
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
                    last_cust_id = row[-1]  # cursor_cust_id

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
                                routing_key="personal_data_queue",
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
                    record = PersonalDataRecord(**record_data)

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
                        if self.total_consumed % 100 == 0:
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
            channel.basic_consume(queue="personal_data_queue", on_message_callback=process_message)

            while not self.stop_consumer.is_set():
                try:
                    connection.process_data_events(time_limit=1)

                    if self.producer_finished.is_set():
                        method = channel.queue_declare(
                            queue="personal_data_queue", durable=True, passive=True
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
                        queue="personal_data_queue", on_message_callback=process_message
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
        self.logger.info("Starting Personal Data STREAMING pipeline...")

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
            Personal Data Pipeline Summary:
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

    parser = argparse.ArgumentParser(description="Personal Data Streaming Pipeline")
    parser.add_argument(
        "--batch-size", type=int, default=500, help="Batch size for processing"
    )

    args = parser.parse_args()

    pipeline = PersonalDataStreamingPipeline(batch_size=args.batch_size)

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
