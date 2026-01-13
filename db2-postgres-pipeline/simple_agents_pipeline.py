#!/usr/bin/env python3
"""
Simple Agents Pipeline - Faster version without complex location lookups
"""

import ibm_db
import psycopg2
import json
import logging
from config import Config

def run_simple_agents_pipeline():
    """Run a simplified agents pipeline for faster execution"""
    config = Config()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    logger.info("🚀 SIMPLE AGENTS PIPELINE")
    logger.info("=" * 50)
    
    # Simplified query without complex location lookups
    simplified_query = """
    SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') AS reportingDate,
           TRIM(
                   CAST(TRIM(COALESCE(be.FIRST_NAME, '')) AS VARCHAR(100)) ||
                   CASE
                       WHEN TRIM(COALESCE(be.FATHER_NAME, '')) <> ''
                           THEN ' ' || CAST(TRIM(be.FATHER_NAME) AS VARCHAR(100))
                       ELSE ''
                       END ||
                   CASE
                       WHEN TRIM(COALESCE(be.LAST_NAME, '')) <> ''
                           THEN ' ' || CAST(TRIM(be.LAST_NAME) AS VARCHAR(100))
                       ELSE ''
                       END
           ) AS agentName,
           al.AGENT_ID AS agentId,
           null AS tillNumber,
           CASE
               WHEN UPPER(TRIM(al.BUSINESS_FORM)) = 'SOLE PROPRIETORY' THEN 'Sole Proprietor'
               WHEN UPPER(TRIM(al.BUSINESS_FORM)) = 'LIMITED COMPANY' THEN 'Company'
               WHEN UPPER(TRIM(al.BUSINESS_FORM)) = 'PRIVATE COMPANY' THEN 'Company'
               WHEN UPPER(TRIM(al.BUSINESS_FORM)) = 'CO-OPERATIVE SOCIETY' THEN 'Trust'
               WHEN UPPER(TRIM(al.BUSINESS_FORM)) = 'PARTNERSHIP' THEN 'Partnership'
               ELSE TRIM(al.BUSINESS_FORM)
               END AS businessForm,
           'bank' AS agentPrincipal,
           'Selcom' AS agentPrincipalName,
           CASE WHEN be.SEX = 'M' then 'Male' WHEN be.SEX = 'F' then 'female' ELSE 'Not Applicable' END AS gender,
           VARCHAR_FORMAT(COALESCE(be.TMSTAMP, CURRENT_DATE), 'DDMMYYYYHHMM') AS registrationDate,
           null AS closedDate,
           al.CERT_IN_CORPORATION AS certIncorporation,
           'TANZANIA, UNITED REPUBLIC OF' AS nationality,
           CASE
               WHEN be.EMPL_STATUS = '1' THEN 'Active'
               WHEN be.EMPL_STATUS = '0' THEN 'Inactive'
               ELSE 'Suspended'
               END AS agentStatus,
           'super agent' AS agentType,
           null AS accountNumber,
           al.REGION AS region,
           al.DISTRICT AS district,
           COALESCE(al.LOCATION, 'N/A') AS ward,
           'N/A' AS street,
           'N/A' AS houseNumber,
           'N/A' AS postalCode,
           'TANZANIA, UNITED REPUBLIC OF' AS country,
           al.GPS AS gpsCoordinates,
           al.TIN AS agentTaxIdentificationNumber,
           CASE
               WHEN LOCATE(',', al.BUSINESS_LICENCE_ISSUER_AND_DATE) > 0 THEN
                   CASE
                       WHEN LOCATE(' ', SUBSTR(al.BUSINESS_LICENCE_ISSUER_AND_DATE, 1,
                                               LOCATE(',', al.BUSINESS_LICENCE_ISSUER_AND_DATE) - 1)) > 0 THEN
                           TRIM(
                                   SUBSTR(
                                           al.BUSINESS_LICENCE_ISSUER_AND_DATE,
                                           1,
                                           LOCATE(' ', al.BUSINESS_LICENCE_ISSUER_AND_DATE) - 1
                                   )
                           )
                       ELSE
                           TRIM(
                                   SUBSTR(
                                           al.BUSINESS_LICENCE_ISSUER_AND_DATE,
                                           1,
                                           LOCATE(',', al.BUSINESS_LICENCE_ISSUER_AND_DATE) - 1
                                   )
                           )
                       END
               WHEN LOCATE(' ', al.BUSINESS_LICENCE_ISSUER_AND_DATE) > 0 THEN
                   TRIM(
                           SUBSTR(
                                   al.BUSINESS_LICENCE_ISSUER_AND_DATE,
                                   1,
                                   LOCATE(' ', al.BUSINESS_LICENCE_ISSUER_AND_DATE) - 1
                           )
                   )
               ELSE TRIM(al.BUSINESS_LICENCE_ISSUER_AND_DATE)
               END AS businessLicense,
           COALESCE(be.TMSTAMP, CURRENT_TIMESTAMP) AS lastModified
    FROM AGENTS_LIST al
             RIGHT JOIN BANKEMPLOYEE be
                        ON RIGHT(TRIM(al.TERMINAL_ID), 8) = TRIM(be.STAFF_NO)
    WHERE be.STAFF_NO IS NOT NULL
      AND be.STAFF_NO = TRIM(be.STAFF_NO)
      AND be.EMPL_STATUS = 1
      AND be.STAFF_NO NOT LIKE 'ATMUSER%'
      AND be.STAFF_NO NOT LIKE '993%'
      AND be.STAFF_NO NOT LIKE '999%'
      AND be.STAFF_NO NOT LIKE '900%'
      AND be.STAFF_NO NOT LIKE 'IAP%'
      AND be.STAFF_NO NOT LIKE 'MCB%'
      AND be.STAFF_NO NOT LIKE 'MIP%'
      AND be.STAFF_NO NOT LIKE 'MOB%'
      AND be.STAFF_NO NOT LIKE 'MWL%'
      AND be.STAFF_NO NOT LIKE 'OWP%'
      AND be.STAFF_NO NOT LIKE 'PI0%'
      AND be.STAFF_NO NOT LIKE 'POS%'
      AND be.STAFF_NO NOT LIKE 'STP%'
      AND be.STAFF_NO NOT LIKE 'TER%'
      AND be.STAFF_NO NOT LIKE 'EIC%'
      AND be.STAFF_NO NOT LIKE 'GEP%'
      AND be.STAFF_NO NOT LIKE 'EYU%'
      AND be.STAFF_NO NOT LIKE 'GLA%'
      AND be.STAFF_NO NOT LIKE 'SYS%'
      AND be.STAFF_NO NOT LIKE 'MLN%'
      AND be.STAFF_NO NOT LIKE 'PET%'
      AND be.STAFF_NO NOT LIKE 'VRT%'
    ORDER BY be.TMSTAMP, al.AGENT_ID
    FETCH FIRST 1000 ROWS ONLY
    """
    
    try:
        # Connect to DB2
        db2_conn_str = f"DATABASE={config.database.db2_database};HOSTNAME={config.database.db2_host};PORT={config.database.db2_port};PROTOCOL=TCPIP;UID={config.database.db2_user};PWD={config.database.db2_password};"
        db2_conn = ibm_db.connect(db2_conn_str, "", "")
        logger.info("✅ Connected to DB2")
        
        # Connect to PostgreSQL
        pg_conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        pg_cursor = pg_conn.cursor()
        logger.info("✅ Connected to PostgreSQL")
        
        # Execute query
        logger.info("🔍 Executing simplified agents query...")
        stmt = ibm_db.exec_immediate(db2_conn, simplified_query)
        
        # Process results
        agents_processed = 0
        while ibm_db.fetch_row(stmt):
            # Extract data
            reporting_date = ibm_db.result(stmt, 0)
            agent_name = ibm_db.result(stmt, 1)
            agent_id = ibm_db.result(stmt, 2)
            till_number = ibm_db.result(stmt, 3)
            business_form = ibm_db.result(stmt, 4)
            agent_principal = ibm_db.result(stmt, 5)
            agent_principal_name = ibm_db.result(stmt, 6)
            gender = ibm_db.result(stmt, 7)
            registration_date = ibm_db.result(stmt, 8)
            closed_date = ibm_db.result(stmt, 9)
            cert_incorporation = ibm_db.result(stmt, 10)
            nationality = ibm_db.result(stmt, 11)
            agent_status = ibm_db.result(stmt, 12)
            agent_type = ibm_db.result(stmt, 13)
            account_number = ibm_db.result(stmt, 14)
            region = ibm_db.result(stmt, 15)
            district = ibm_db.result(stmt, 16)
            ward = ibm_db.result(stmt, 17)
            street = ibm_db.result(stmt, 18)
            house_number = ibm_db.result(stmt, 19)
            postal_code = ibm_db.result(stmt, 20)
            country = ibm_db.result(stmt, 21)
            gps_coordinates = ibm_db.result(stmt, 22)
            agent_tax_id = ibm_db.result(stmt, 23)
            business_license = ibm_db.result(stmt, 24)
            last_modified = ibm_db.result(stmt, 25)
            
            # Insert into PostgreSQL using upsert
            upsert_sql = """
            INSERT INTO agents (
                "reportingDate", "agentName", "agentId", "tillNumber", "businessForm",
                "agentPrincipal", "agentPrincipalName", "gender", "registrationDate", "closedDate",
                "certIncorporation", "nationality", "agentStatus", "agentType", "accountNumber",
                "region", "district", "ward", "street", "houseNumber",
                "postalCode", "country", "gpsCoordinates", "agentTaxIdentificationNumber", "businessLicense",
                "lastModified"
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT ("agentId") DO UPDATE SET
                "reportingDate" = EXCLUDED."reportingDate",
                "agentName" = EXCLUDED."agentName",
                "tillNumber" = EXCLUDED."tillNumber",
                "businessForm" = EXCLUDED."businessForm",
                "agentPrincipal" = EXCLUDED."agentPrincipal",
                "agentPrincipalName" = EXCLUDED."agentPrincipalName",
                "gender" = EXCLUDED."gender",
                "registrationDate" = EXCLUDED."registrationDate",
                "closedDate" = EXCLUDED."closedDate",
                "certIncorporation" = EXCLUDED."certIncorporation",
                "nationality" = EXCLUDED."nationality",
                "agentStatus" = EXCLUDED."agentStatus",
                "agentType" = EXCLUDED."agentType",
                "accountNumber" = EXCLUDED."accountNumber",
                "region" = EXCLUDED."region",
                "district" = EXCLUDED."district",
                "ward" = EXCLUDED."ward",
                "street" = EXCLUDED."street",
                "houseNumber" = EXCLUDED."houseNumber",
                "postalCode" = EXCLUDED."postalCode",
                "country" = EXCLUDED."country",
                "gpsCoordinates" = EXCLUDED."gpsCoordinates",
                "agentTaxIdentificationNumber" = EXCLUDED."agentTaxIdentificationNumber",
                "businessLicense" = EXCLUDED."businessLicense",
                "lastModified" = EXCLUDED."lastModified",
                "updated_at" = CURRENT_TIMESTAMP
            """
            
            pg_cursor.execute(upsert_sql, (
                reporting_date, agent_name, agent_id, till_number, business_form,
                agent_principal, agent_principal_name, gender, registration_date, closed_date,
                cert_incorporation, nationality, agent_status, agent_type, account_number,
                region, district, ward, street, house_number,
                postal_code, country, gps_coordinates, agent_tax_id, business_license,
                last_modified
            ))
            
            agents_processed += 1
            if agents_processed % 100 == 0:
                logger.info(f"📊 Processed {agents_processed} agents...")
                pg_conn.commit()
        
        # Final commit
        pg_conn.commit()
        
        # Check final count
        pg_cursor.execute('SELECT COUNT(*) FROM agents')
        total_count = pg_cursor.fetchone()[0]
        
        logger.info(f"✅ Pipeline completed successfully!")
        logger.info(f"📊 Processed {agents_processed} agents from DB2")
        logger.info(f"📊 Total agents in PostgreSQL: {total_count}")
        
        # Show sample records
        pg_cursor.execute("""
            SELECT "agentName", "agentId", "businessForm", "agentStatus", "region", "district", "ward"
            FROM agents 
            ORDER BY "lastModified" DESC 
            LIMIT 5
        """)
        
        sample_records = pg_cursor.fetchall()
        logger.info("📋 Sample records:")
        for record in sample_records:
            agent_name, agent_id, business_form, agent_status, region, district, ward = record
            logger.info(f"  - {agent_name} (ID: {agent_id}, Status: {agent_status}, Location: {region}, {district}, {ward})")
        
        # Close connections
        pg_cursor.close()
        pg_conn.close()
        ibm_db.close(db2_conn)
        
    except Exception as e:
        logger.error(f"❌ Error in simple agents pipeline: {e}")
        raise

if __name__ == "__main__":
    run_simple_agents_pipeline()