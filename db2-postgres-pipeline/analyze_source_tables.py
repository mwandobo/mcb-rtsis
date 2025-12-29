#!/usr/bin/env python3
"""
Analyze Source Tables for Individual Information Data
Learn from customer view structure to map data directly from source tables
"""

from db2_connection import DB2Connection
import logging
import csv

def analyze_source_tables():
    logging.basicConfig(level=logging.INFO)
    db2_conn = DB2Connection()
    
    # Based on customer view analysis, these are the key source tables
    source_tables_info = {
        "CUSTOMER": {
            "description": "Main customer table",
            "key_fields": ["CUST_ID", "FIRST_NAME", "MIDDLE_NAME", "SURNAME", "SEX", "DATE_OF_BIRTH", 
                          "MOBILE_TEL", "E_MAIL", "EMPLOYER", "SPOUSE_NAME", "BIRTHPLACE"]
        },
        "CUSTOMER_CATEGORY": {
            "description": "Customer categories and classifications",
            "key_fields": ["FK_CUSTOMERCUST_ID", "FK_CATEGORYCATEGOR", "FK_GENERIC_DETAFK", "FK_GENERIC_DETASER"]
        },
        "CUST_ADDRESS": {
            "description": "Customer addresses",
            "key_fields": ["FK_CUSTOMERCUST_ID", "ADDRESS_TYPE", "ADDRESS_1", "REGION", "ZIP_CODE", "COMMUNICATION_ADDR"]
        },
        "OTHER_ID": {
            "description": "Customer identification documents",
            "key_fields": ["FK_CUSTOMERCUST_ID", "ID_NO", "ISSUE_DATE", "EXPIRY_DATE", "MAIN_FLAG"]
        },
        "OTHER_AFM": {
            "description": "Customer tax information",
            "key_fields": ["FK_CUSTOMERCUST_ID", "AFM_NO", "MAIN_FLAG"]
        },
        "GENERIC_DETAIL": {
            "description": "Lookup values",
            "key_fields": ["FK_GENERIC_HEADPAR", "SERIAL_NUM", "DESCRIPTION"]
        }
    }
    
    # Individual data mapping based on view analysis
    individual_data_mapping = {
        "firstName": {"table": "CUSTOMER", "column": "FIRST_NAME"},
        "middleNames": {"table": "CUSTOMER", "column": "MIDDLE_NAME"},
        "presentSurname": {"table": "CUSTOMER", "column": "SURNAME"},
        "gender": {"table": "CUSTOMER", "column": "SEX"},
        "birthDate": {"table": "CUSTOMER", "column": "DATE_OF_BIRTH"},
        "mobileNumber": {"table": "CUSTOMER", "column": "MOBILE_TEL"},
        "emailAddress": {"table": "CUSTOMER", "column": "E_MAIL"},
        "employerName": {"table": "CUSTOMER", "column": "EMPLOYER"},
        "spousesFullName": {"table": "CUSTOMER", "column": "SPOUSE_NAME"},
        "birthStreet": {"table": "CUSTOMER", "column": "BIRTHPLACE"},
        "nationality": {"table": "CUSTOMER_CATEGORY", "category": "NATIONAL"},
        "citizenship": {"table": "CUSTOMER_CATEGORY", "category": "CITIZEN"},
        "profession": {"table": "CUSTOMER_CATEGORY", "category": "PROFES"},
        "educationLevel": {"table": "CUSTOMER_CATEGORY", "category": "EDULEVEL"},
        "maritalStatus": {"table": "CUSTOMER_CATEGORY", "category": "FAMILY"},
        "identificationNumber": {"table": "OTHER_ID", "column": "ID_NO"},
        "issuanceDate": {"table": "OTHER_ID", "column": "ISSUE_DATE"},
        "expirationDate": {"table": "OTHER_ID", "column": "EXPIRY_DATE"},
        "taxIdentificationNumber": {"table": "OTHER_AFM", "column": "AFM_NO"},
        "street": {"table": "CUST_ADDRESS", "column": "ADDRESS_1"},
        "region": {"table": "CUST_ADDRESS", "column": "REGION"},
        "postalCode": {"table": "CUST_ADDRESS", "column": "ZIP_CODE"}
    }
    
    try:
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            print("🔍 ANALYZING SOURCE TABLES FOR INDIVIDUAL INFORMATION")
            print("=" * 100)
            
            results = []
            
            # Analyze each source table
            for table_name, table_info in source_tables_info.items():
                print(f"\n{'='*80}")
                print(f"ANALYZING: {table_name}")
                print(f"Description: {table_info['description']}")
                print(f"{'='*80}")
                
                try:
                    # Get row count
                    count_query = f"SELECT COUNT(*) FROM {table_name}"
                    cursor.execute(count_query)
                    row_count = cursor.fetchone()[0]
                    
                    print(f"  📊 Total rows: {row_count:,}")
                    
                    if row_count == 0:
                        print(f"  ⚠ Table is empty")
                        continue
                    
                    # Get table structure
                    structure_query = f"""
                    SELECT COLNAME, TYPENAME, LENGTH, NULLS
                    FROM SYSCAT.COLUMNS
                    WHERE TABNAME = '{table_name}' AND TABSCHEMA = 'PROFITS'
                    ORDER BY COLNO
                    """
                    cursor.execute(structure_query)
                    columns = cursor.fetchall()
                    
                    print(f"  📋 Columns: {len(columns)}")
                    
                    # Check key fields
                    print(f"\n  🔍 Key Fields Analysis:")
                    for key_field in table_info['key_fields']:
                        # Check if field exists
                        field_exists = any(col[0] == key_field for col in columns)
                        if field_exists:
                            # Get sample data
                            try:
                                sample_query = f"""
                                SELECT {key_field}, COUNT(*) as cnt
                                FROM {table_name}
                                WHERE {key_field} IS NOT NULL
                                GROUP BY {key_field}
                                FETCH FIRST 3 ROWS ONLY
                                """
                                cursor.execute(sample_query)
                                samples = cursor.fetchall()
                                
                                if samples:
                                    sample_values = [str(s[0])[:30] for s in samples]
                                    total_count = sum([s[1] for s in samples])
                                    print(f"    ✓ {key_field}: {sample_values} (Total: {total_count:,})")
                                else:
                                    print(f"    ⚠ {key_field}: No data")
                            except Exception as e:
                                print(f"    ✗ {key_field}: Error - {str(e)[:50]}")
                        else:
                            print(f"    ✗ {key_field}: Column not found")
                    
                    # Special analysis for CUSTOMER_CATEGORY
                    if table_name == "CUSTOMER_CATEGORY":
                        print(f"\n  📂 Category Types Analysis:")
                        try:
                            cat_query = """
                            SELECT FK_CATEGORYCATEGOR, COUNT(*) as cnt
                            FROM CUSTOMER_CATEGORY
                            GROUP BY FK_CATEGORYCATEGOR
                            ORDER BY cnt DESC
                            """
                            cursor.execute(cat_query)
                            categories = cursor.fetchall()
                            
                            for cat_type, count in categories:
                                print(f"    📋 {cat_type}: {count:,} records")
                                
                                # Check if this category is relevant for individual data
                                relevant_fields = [field for field, info in individual_data_mapping.items() 
                                                 if info.get('category') == cat_type]
                                if relevant_fields:
                                    print(f"      → Maps to: {', '.join(relevant_fields)}")
                        except Exception as e:
                            print(f"    ✗ Category analysis error: {e}")
                    
                    # Special analysis for CUST_ADDRESS
                    if table_name == "CUST_ADDRESS":
                        print(f"\n  🏠 Address Types Analysis:")
                        try:
                            addr_query = """
                            SELECT ADDRESS_TYPE, COMMUNICATION_ADDR, COUNT(*) as cnt
                            FROM CUST_ADDRESS
                            GROUP BY ADDRESS_TYPE, COMMUNICATION_ADDR
                            ORDER BY cnt DESC
                            """
                            cursor.execute(addr_query)
                            addr_types = cursor.fetchall()
                            
                            for addr_type, comm_addr, count in addr_types:
                                addr_desc = f"Type {addr_type}"
                                if comm_addr == '1':
                                    addr_desc += " (Communication)"
                                print(f"    🏠 {addr_desc}: {count:,} records")
                        except Exception as e:
                            print(f"    ✗ Address analysis error: {e}")
                
                except Exception as e:
                    print(f"  ✗ Error analyzing {table_name}: {e}")
            
            # Test individual data mapping
            print(f"\n{'='*100}")
            print("🧪 TESTING INDIVIDUAL DATA MAPPING")
            print(f"{'='*100}")
            
            # Test with a sample customer
            try:
                # Get a sample customer ID
                cursor.execute("SELECT CUST_ID FROM CUSTOMER WHERE ENTRY_STATUS = '1' FETCH FIRST 1 ROWS ONLY")
                sample_cust = cursor.fetchone()
                
                if sample_cust:
                    cust_id = sample_cust[0]
                    print(f"\n🧪 Testing with Customer ID: {cust_id}")
                    
                    # Test each mapping
                    for field_name, mapping_info in individual_data_mapping.items():
                        table = mapping_info['table']
                        
                        if 'column' in mapping_info:
                            # Direct column mapping
                            column = mapping_info['column']
                            try:
                                if table == "CUSTOMER":
                                    test_query = f"SELECT {column} FROM {table} WHERE CUST_ID = {cust_id}"
                                elif table in ["OTHER_ID", "OTHER_AFM"]:
                                    test_query = f"SELECT {column} FROM {table} WHERE FK_CUSTOMERCUST_ID = {cust_id}"
                                elif table == "CUST_ADDRESS":
                                    test_query = f"SELECT {column} FROM {table} WHERE FK_CUSTOMERCUST_ID = {cust_id} AND COMMUNICATION_ADDR = '1'"
                                else:
                                    continue
                                
                                cursor.execute(test_query)
                                result = cursor.fetchone()
                                
                                if result and result[0]:
                                    print(f"    ✓ {field_name}: {str(result[0])[:50]}")
                                else:
                                    print(f"    ⚠ {field_name}: No data")
                            except Exception as e:
                                print(f"    ✗ {field_name}: Error - {str(e)[:50]}")
                        
                        elif 'category' in mapping_info:
                            # Category mapping
                            category = mapping_info['category']
                            try:
                                test_query = f"""
                                SELECT gd.DESCRIPTION
                                FROM CUSTOMER_CATEGORY cc
                                JOIN GENERIC_DETAIL gd ON cc.FK_GENERIC_DETAFK = gd.FK_GENERIC_HEADPAR 
                                                      AND cc.FK_GENERIC_DETASER = gd.SERIAL_NUM
                                WHERE cc.FK_CUSTOMERCUST_ID = {cust_id} 
                                  AND cc.FK_CATEGORYCATEGOR = '{category}'
                                """
                                cursor.execute(test_query)
                                result = cursor.fetchone()
                                
                                if result and result[0]:
                                    print(f"    ✓ {field_name}: {str(result[0])[:50]}")
                                else:
                                    print(f"    ⚠ {field_name}: No data")
                            except Exception as e:
                                print(f"    ✗ {field_name}: Error - {str(e)[:50]}")
            
            except Exception as e:
                print(f"  ✗ Testing error: {e}")
            
            # Generate comprehensive SQL
            print(f"\n{'='*100}")
            print("📝 GENERATING COMPREHENSIVE SQL MAPPING")
            print(f"{'='*100}")
            
            sql_content = generate_comprehensive_sql()
            
            with open("individual_data_comprehensive_mapping.sql", 'w', encoding='utf-8') as f:
                f.write(sql_content)
            
            print(f"✓ Comprehensive SQL saved to: individual_data_comprehensive_mapping.sql")
            
            print(f"\n{'='*100}")
            print("✅ SOURCE TABLE ANALYSIS COMPLETE!")
            print(f"{'='*100}")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

def generate_comprehensive_sql():
    """Generate comprehensive SQL for individual data mapping"""
    
    return """-- Comprehensive Individual Information Mapping
-- Direct mapping from source tables based on CUST_DETAILS_FULL_VW analysis

SELECT 
    -- REPORTING INFORMATION
    VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') AS reportingDate,
    CAST(c.CUST_ID AS VARCHAR(50)) AS customerIdentificationNumber,
    
    -- PERSONAL PARTICULARS
    c.FIRST_NAME AS firstName,
    c.MIDDLE_NAME AS middleNames,
    CAST(NULL AS VARCHAR(100)) AS otherNames,  -- Not available
    TRIM(
        COALESCE(c.FIRST_NAME, '') || ' ' || 
        COALESCE(c.MIDDLE_NAME, '') || ' ' || 
        COALESCE(c.SURNAME, '')
    ) AS fullNames,
    c.SURNAME AS presentSurname,
    c.MOTHER_SURNAME AS birthSurname,
    CASE 
        WHEN c.SEX = 'M' THEN 'Male'
        WHEN c.SEX = 'F' THEN 'Female'
        ELSE 'NotSpecified'
    END AS gender,
    family_cat.DESCRIPTION AS maritalStatus,
    CAST(NULL AS INTEGER) AS numberSpouse,  -- Not available
    national_cat.DESCRIPTION AS nationality,
    citizen_cat.DESCRIPTION AS citizenship,
    CASE 
        WHEN c.NON_RESIDENT = '0' THEN 'Resident'
        WHEN c.NON_RESIDENT = '1' THEN 'Non-Resident'
        ELSE 'Unknown'
    END AS residency,
    profes_cat.DESCRIPTION AS profession,
    indcode_cat.DESCRIPTION AS sectorSnaClassification,
    CASE 
        WHEN c.ENTRY_STATUS = '0' THEN 'Deceased'
        ELSE 'Active'
    END AS fateStatus,
    proflevl_cat.DESCRIPTION AS socialStatus,
    profcat_cat.DESCRIPTION AS employmentStatus,
    c.SALARY_AMN AS monthlyIncome,
    c.NUM_OF_CHILDREN AS numberDependants,
    edulevel_cat.DESCRIPTION AS educationLevel,
    CAST(NULL AS DECIMAL(15,2)) AS averageMonthlyExpenditure,  -- Not available
    CASE 
        WHEN c.BLACKLISTED_IND = '1' THEN 'Blacklisted'
        WHEN c.AML_STATUS IS NOT NULL THEN c.AML_STATUS
        ELSE 'Clean'
    END AS negativeClientStatus,
    
    -- SPOUSE INFORMATION
    c.SPOUSE_NAME AS spousesFullName,
    CAST(NULL AS VARCHAR(50)) AS spouseIdentificationType,  -- Not available
    CAST(NULL AS VARCHAR(50)) AS spouseIdentificationNumber,  -- Not available
    c.MOTHER_SURNAME AS maidenName,
    CAST(NULL AS DECIMAL(15,2)) AS monthlyExpenses,  -- Not available
    
    -- BIRTH INFORMATION
    VARCHAR_FORMAT(c.DATE_OF_BIRTH, 'DDMMYYYYHHMM') AS birthDate,
    bcountry_cat.DESCRIPTION AS birthCountry,
    CAST(NULL AS VARCHAR(20)) AS birthPostalCode,  -- Not available
    CAST(NULL AS VARCHAR(20)) AS birthHouseNumber,  -- Not available
    region_cat.DESCRIPTION AS birthRegion,
    CAST(NULL AS VARCHAR(100)) AS birthDistrict,  -- Not available
    CAST(NULL AS VARCHAR(100)) AS birthWard,  -- Not available
    c.BIRTHPLACE AS birthStreet,
    
    -- IDENTIFICATION INFORMATION
    id_type.DESCRIPTION AS identificationType,
    oid.ID_NO AS identificationNumber,
    VARCHAR_FORMAT(oid.ISSUE_DATE, 'DDMMYYYYHHMM') AS issuanceDate,
    VARCHAR_FORMAT(oid.EXPIRY_DATE, 'DDMMYYYYHHMM') AS expirationDate,
    id_country.DESCRIPTION AS issuancePlace,
    CAST(NULL AS VARCHAR(200)) AS issuingAuthority,  -- Not available
    
    -- BUSINESS INFORMATION
    CASE 
        WHEN c.CUST_TYPE = '2' THEN c.SURNAME  -- Company name for corporate
        ELSE NULL
    END AS businessName,
    VARCHAR_FORMAT(c.CUSTOMER_BEGIN_DAT, 'DDMMYYYYHHMM') AS establishmentDate,
    c.CHAMBER_ID AS businessRegistrationNumber,
    VARCHAR_FORMAT(c.CERTIFIC_DATE, 'DDMMYYYYHHMM') AS businessRegistrationDate,
    CAST(NULL AS VARCHAR(50)) AS businessLicenseNumber,  -- Not available
    afm.AFM_NO AS taxIdentificationNumber,
    
    -- EMPLOYER INFORMATION
    c.EMPLOYER AS employerName,
    CAST(NULL AS VARCHAR(100)) AS employerRegion,  -- Not available
    CAST(NULL AS VARCHAR(100)) AS employerDistrict,  -- Not available
    CAST(NULL AS VARCHAR(100)) AS employerWard,  -- Not available
    c.EMPLOYER_ADDRESS AS employerStreet,
    CAST(NULL AS VARCHAR(20)) AS employerHouseNumber,  -- Not available
    CAST(NULL AS VARCHAR(20)) AS employerPostalCode,  -- Not available
    activity_cat.DESCRIPTION AS businessNature,
    
    -- INDIVIDUAL CONTACTS
    c.MOBILE_TEL AS mobileNumber,
    c.MOBILE_TEL2 AS alternativeMobileNumber,
    c.TELEPHONE_1 AS fixedLineNumber,
    addr_comm.FAX_NO AS faxNumber,
    c.E_MAIL AS emailAddress,
    c.INTERNET_ADDRESS AS socialMedia,
    addr_comm.ADDRESS_1 AS mainAddress,
    
    -- PRIMARY ADDRESS (Communication Address)
    addr_comm.ADDRESS_1 AS street,
    CAST(NULL AS VARCHAR(20)) AS houseNumber,  -- Not available
    addr_comm.ZIP_CODE AS postalCode,
    addr_comm.REGION AS region,
    CAST(NULL AS VARCHAR(100)) AS district,  -- Not available
    CAST(NULL AS VARCHAR(100)) AS ward,  -- Not available
    comm_country.DESCRIPTION AS country,
    
    -- SECONDARY ADDRESS (Work Address)
    addr_work.ADDRESS_1 AS secondaryStreet,
    CAST(NULL AS VARCHAR(20)) AS secondaryHouseNumber,  -- Not available
    addr_work.ZIP_CODE AS secondaryPostalCode,
    addr_work.REGION AS secondaryRegion,
    CAST(NULL AS VARCHAR(100)) AS secondaryDistrict,  -- Not available
    CAST(NULL AS VARCHAR(100)) AS secondaryWard,  -- Not available
    work_country.DESCRIPTION AS secondaryCountry

FROM CUSTOMER c

-- Customer Categories with proper joins
LEFT JOIN (
    SELECT cc.FK_CUSTOMERCUST_ID, gd.DESCRIPTION
    FROM CUSTOMER_CATEGORY cc
    JOIN GENERIC_DETAIL gd ON cc.FK_GENERIC_DETAFK = gd.FK_GENERIC_HEADPAR 
                          AND cc.FK_GENERIC_DETASER = gd.SERIAL_NUM
    WHERE cc.FK_CATEGORYCATEGOR = 'FAMILY'
) family_cat ON family_cat.FK_CUSTOMERCUST_ID = c.CUST_ID

LEFT JOIN (
    SELECT cc.FK_CUSTOMERCUST_ID, gd.DESCRIPTION
    FROM CUSTOMER_CATEGORY cc
    JOIN GENERIC_DETAIL gd ON cc.FK_GENERIC_DETAFK = gd.FK_GENERIC_HEADPAR 
                          AND cc.FK_GENERIC_DETASER = gd.SERIAL_NUM
    WHERE cc.FK_CATEGORYCATEGOR = 'NATIONAL'
) national_cat ON national_cat.FK_CUSTOMERCUST_ID = c.CUST_ID

LEFT JOIN (
    SELECT cc.FK_CUSTOMERCUST_ID, gd.DESCRIPTION
    FROM CUSTOMER_CATEGORY cc
    JOIN GENERIC_DETAIL gd ON cc.FK_GENERIC_DETAFK = gd.FK_GENERIC_HEADPAR 
                          AND cc.FK_GENERIC_DETASER = gd.SERIAL_NUM
    WHERE cc.FK_CATEGORYCATEGOR = 'CITIZEN'
) citizen_cat ON citizen_cat.FK_CUSTOMERCUST_ID = c.CUST_ID

LEFT JOIN (
    SELECT cc.FK_CUSTOMERCUST_ID, gd.DESCRIPTION
    FROM CUSTOMER_CATEGORY cc
    JOIN GENERIC_DETAIL gd ON cc.FK_GENERIC_DETAFK = gd.FK_GENERIC_HEADPAR 
                          AND cc.FK_GENERIC_DETASER = gd.SERIAL_NUM
    WHERE cc.FK_CATEGORYCATEGOR = 'PROFES'
) profes_cat ON profes_cat.FK_CUSTOMERCUST_ID = c.CUST_ID

LEFT JOIN (
    SELECT cc.FK_CUSTOMERCUST_ID, gd.DESCRIPTION
    FROM CUSTOMER_CATEGORY cc
    JOIN GENERIC_DETAIL gd ON cc.FK_GENERIC_DETAFK = gd.FK_GENERIC_HEADPAR 
                          AND cc.FK_GENERIC_DETASER = gd.SERIAL_NUM
    WHERE cc.FK_CATEGORYCATEGOR = 'INDCODE'
) indcode_cat ON indcode_cat.FK_CUSTOMERCUST_ID = c.CUST_ID

LEFT JOIN (
    SELECT cc.FK_CUSTOMERCUST_ID, gd.DESCRIPTION
    FROM CUSTOMER_CATEGORY cc
    JOIN GENERIC_DETAIL gd ON cc.FK_GENERIC_DETAFK = gd.FK_GENERIC_HEADPAR 
                          AND cc.FK_GENERIC_DETASER = gd.SERIAL_NUM
    WHERE cc.FK_CATEGORYCATEGOR = 'PROFLEVL'
) proflevl_cat ON proflevl_cat.FK_CUSTOMERCUST_ID = c.CUST_ID

LEFT JOIN (
    SELECT cc.FK_CUSTOMERCUST_ID, gd.DESCRIPTION
    FROM CUSTOMER_CATEGORY cc
    JOIN GENERIC_DETAIL gd ON cc.FK_GENERIC_DETAFK = gd.FK_GENERIC_HEADPAR 
                          AND cc.FK_GENERIC_DETASER = gd.SERIAL_NUM
    WHERE cc.FK_CATEGORYCATEGOR = 'PROFCAT'
) profcat_cat ON profcat_cat.FK_CUSTOMERCUST_ID = c.CUST_ID

LEFT JOIN (
    SELECT cc.FK_CUSTOMERCUST_ID, gd.DESCRIPTION
    FROM CUSTOMER_CATEGORY cc
    JOIN GENERIC_DETAIL gd ON cc.FK_GENERIC_DETAFK = gd.FK_GENERIC_HEADPAR 
                          AND cc.FK_GENERIC_DETASER = gd.SERIAL_NUM
    WHERE cc.FK_CATEGORYCATEGOR = 'EDULEVEL'
) edulevel_cat ON edulevel_cat.FK_CUSTOMERCUST_ID = c.CUST_ID

LEFT JOIN (
    SELECT cc.FK_CUSTOMERCUST_ID, gd.DESCRIPTION
    FROM CUSTOMER_CATEGORY cc
    JOIN GENERIC_DETAIL gd ON cc.FK_GENERIC_DETAFK = gd.FK_GENERIC_HEADPAR 
                          AND cc.FK_GENERIC_DETASER = gd.SERIAL_NUM
    WHERE cc.FK_CATEGORYCATEGOR = 'BCOUNTRY'
) bcountry_cat ON bcountry_cat.FK_CUSTOMERCUST_ID = c.CUST_ID

LEFT JOIN (
    SELECT cc.FK_CUSTOMERCUST_ID, gd.DESCRIPTION
    FROM CUSTOMER_CATEGORY cc
    JOIN GENERIC_DETAIL gd ON cc.FK_GENERIC_DETAFK = gd.FK_GENERIC_HEADPAR 
                          AND cc.FK_GENERIC_DETASER = gd.SERIAL_NUM
    WHERE cc.FK_CATEGORYCATEGOR = 'REGION'
) region_cat ON region_cat.FK_CUSTOMERCUST_ID = c.CUST_ID

LEFT JOIN (
    SELECT cc.FK_CUSTOMERCUST_ID, gd.DESCRIPTION
    FROM CUSTOMER_CATEGORY cc
    JOIN GENERIC_DETAIL gd ON cc.FK_GENERIC_DETAFK = gd.FK_GENERIC_HEADPAR 
                          AND cc.FK_GENERIC_DETASER = gd.SERIAL_NUM
    WHERE cc.FK_CATEGORYCATEGOR = 'ACTIVITY'
) activity_cat ON activity_cat.FK_CUSTOMERCUST_ID = c.CUST_ID

-- Identification Information
LEFT JOIN OTHER_ID oid ON oid.FK_CUSTOMERCUST_ID = c.CUST_ID 
                      AND COALESCE(oid.MAIN_FLAG, '1') = '1'
LEFT JOIN GENERIC_DETAIL id_type ON id_type.FK_GENERIC_HEADPAR = oid.FKGH_HAS_TYPE
                                AND id_type.SERIAL_NUM = oid.FKGD_HAS_TYPE
LEFT JOIN GENERIC_DETAIL id_country ON id_country.FK_GENERIC_HEADPAR = oid.FKGH_HAS_BEEN_ISSU
                                   AND id_country.SERIAL_NUM = oid.FKGD_HAS_BEEN_ISSU

-- Tax Information
LEFT JOIN OTHER_AFM afm ON afm.FK_CUSTOMERCUST_ID = c.CUST_ID 
                       AND COALESCE(afm.MAIN_FLAG, '1') = '1'

-- Address Information
LEFT JOIN CUST_ADDRESS addr_comm ON addr_comm.FK_CUSTOMERCUST_ID = c.CUST_ID
                                AND addr_comm.COMMUNICATION_ADDR = '1'
                                AND addr_comm.ENTRY_STATUS = '1'
LEFT JOIN GENERIC_DETAIL comm_country ON comm_country.FK_GENERIC_HEADPAR = addr_comm.FKGH_HAS_COUNTRY
                                     AND comm_country.SERIAL_NUM = addr_comm.FKGD_HAS_COUNTRY

LEFT JOIN CUST_ADDRESS addr_work ON addr_work.FK_CUSTOMERCUST_ID = c.CUST_ID
                                AND addr_work.ADDRESS_TYPE = '4'
                                AND addr_work.ENTRY_STATUS = '1'
LEFT JOIN GENERIC_DETAIL work_country ON work_country.FK_GENERIC_HEADPAR = addr_work.FKGH_HAS_COUNTRY
                                     AND work_country.SERIAL_NUM = addr_work.FKGD_HAS_COUNTRY

WHERE c.ENTRY_STATUS = '1'  -- Active customers only
ORDER BY c.CUST_ID;"""

if __name__ == "__main__":
    analyze_source_tables()