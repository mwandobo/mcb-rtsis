#!/usr/bin/env python3
"""
Analyze Individual Data Availability by Learning from Customer View Struct
Thi
w.
"""

from db2_ction
g
import csv
import re

def ces():
    logging.basicConfig(level=logging.INFO)
    db2_conn = DB2Conn)
    
    # Individual data requng
    individual_data_requirements = {
        "PERSONAL_DATA": {
            "reportingDate": {"},
            "customerIdentificationNumber": {"description": "C,
          NAME"},
            "middleNames": {"description"
            "otherNames": {"descr,
            "fullNames": {"desc
            "presentSurname": 
            "birthSurname": {"description": "Birth surname if chan
          X"},
            "maritalStMILY"},
            "numberSpouse": {"descri,
            "nationality": {"de"},
            "citizenship": {"d"},
            "residency": {"description": "Is the Customer"},
          ,
            "sectorSnaClCODE"},
            "fateStatus": {"descripti,
            "socialStatus": {"d,
            "employmentStatus"OFCAT"},
            "monthlyIncome": {"description": "Total monthly_AMN"},
          LDREN"},
            "educationL,
            "averageMonthlyExp,
            "negativeClientStatu_IND"}
        },
        "SPOUSE_INFO": {
          ,
            "spouseIdeWN"},
            "spouseIdentificationNumber": {"description": "I
            "maidenName": {"des
            "monthlyExpenses":"}
        },
        "B
            "birthDate": {"H"},
            "birthCountry": {"des"},
            "birthPostalCode": 
            "birthHouseNumber"
            "birthRegion": {"description": "Location/State of 
          "},
            "birthWard": },
            "birthStreet": {"descriptionACE"}
        },
        "IDENTIFICATION_INFO":
            "identificationType": {"description": "Ty"},
          
            "issuan},
            "expirationDate":TE"},
            "issuancePlace": {"
            "issuingAuthority"NKNOWN"}
        },
        "BO": {
            "businessName"
            "establishmentDate":_DAT"},
            "businessRegistratiID"},
            "businessRegistrat
            "businessLicenseNumber": {"description": "BusinesKNOWN"},
          M_NO"}
        },
        "EMPLOYER_INFO": {
            "employerName": {"de
            "employerRegion": {
            "employerDistrict": {"description"NKNOWN"},
          
            "employerStrRESS"},
            "employerHouseNumber": {"descriptiWN"},
            "employerPostalCode"},
            "businessNature": 
        },
        "C {
            "mobileNumbeTEL"},
            "alternativeMobileNumber": {"desc
            "fixedLineNumber": NE_1"},
            "faxNumber": {"des"},
            "emailAddress": {"description": "Email address", "source": IL"},
          
            "mainAddre}
        },
        "PRIMARY_ADDRESS": {
            "street": {"descri
            "houseNumber": {"description": "Number of,
          DE"},
            "region": {,
            "district": {"descri 
            "ward": {"descripti,
            "country": {"descr"}
        },
        "S: {
            "street": {"description""},
            "houseNumber": {"desc
            "postalCode": {"des},
            "region": {"descrik)"},
            "district": {"description": "Secondary district", "souN"}, 
          },
            "country": )"}
        }
    }
    
    # Key tables identified from the view
    sourcebles = {
        "CUSTOMER": "Main,
        "CUST_ADDRESS": "Customer ",
        "CUSTOMER_CATEGORY": "Cations",
        "OTHER_ID": "Customer 
        "OTHER_AFM": "Customer tax information",
        "G,
        "GENERIC_HEADER": "Lors",
        "TAX_OFFICE": "Tax office",
        "CURRENCY": "Currency i,
        "CUST_ADVANCES_INFO": "
    }
    
    try:
        with db2_conn.get_connectionas conn:
            cursor = conn.cursor()
            
            print("🔍 ANALYZING INDIVIDUAL DATA SOURCES FROM CUSTOMER VRUCTURE")
          
            
            # Analyze each source table
            table_analysis = {}
            
            for table_name, description in source_tables.items():
          )
                print(f"ANA}")
                print(f"Descriptio")
                print(f"{'='*100}")
                
                try:
          
                    structure_query = "
                    SELECT COL
                    FROM SYSCAT.NS
                    WHERE TABName}'
                    AND TABSCHEMA = 'PROFITS'
          Y COLNO
                    """
                    
                    cursor.exec
                    columns = c)
                    
          mns:
        )
                        contnue
                    
                    # Get row count
                    count_query}"
                    cursor.exec
                    row_count = cursor.fetchone()[0]
                 
                    print(f"  ✓ Found rows")
                    
                    # Store tablnfo
                    table_analy
                        'columns': columns,
          
                        'relevant_field
                    }
                    
                    # Find rele data
                    print(f"\n  📋 Relevant Columns for Indita:")
          
                    
                    for col_name, col_tys:
                        col_low()
                        
                        # Check if this column is ta
          
                        for ):
                            fo):
                                ()
                               ource:
                                    relevant_
             
        
                           ppend({
                      l_name,
                                'type':e,
                               l_length,
                              e,
                                'relevant_for': relevant_for
          
                         
                            
                            # G
                            tr
                                sample_query "
           cnt
                            }
                              
                                e}
                               
                                """
          ry)
                             chall()
                                
                                :
                               
                                    total_count = sum([ss])
          )
                        
                                ")
                
                except Excepti
                    print(f"  ✗ Error analyzing {tab
          
            # Generate comng
            print(f"\n{'='*100}")
            print("📊 COMPREHENS
            print(f"{'='*100}")
            
           []
            
            for category, fiel
                print(f"\n📂 {ca")
                
                for api_field, field_info tems():
          
                    desc']
                    
                    # Determine
                    if source =KNOWN":
                        status = "❌ Not Available"
          d"
         = False
                    elif source == ":
                        status ated"
                        table_column = MP"
                        data_avTrue
                    elif sourcD":
                        status = "✅ Calculated Field"
          
                        data_avaiue
                    else:
                        # Checka
                        table_ource
                        if table_name in table_analy 0:
          vailable"
                         e
                            data_availa
                        else:
                            sting"
                            table_column = source
          alse
                    
                    print(f"  {status} {
                    
                    mapping_red({
                        'Category': category,
          field,
                        'D,
                        'Source_Table_Column': tn,
                        'Data_A,
                        'Statu'')
                    })
            
            # Generate statistics
            print(f"\n{'='*100)
            print("📈 AVAILABILIS")
            print(f"{'='*100}"
            
          lts)
        able']])
            missing_fields = ts
            
            print(f"\n📊 Overall Statistics:")
            print(f"   Total AP
            print(f"   Availab
            print(f"   Missing: {missing_fields} ({missing_fields")
            
            # Category breakdon
            print(f"\n📋 By Category:")
            categories = {}
            for result in mapp
                cat = result['Category']
          
                    categories[cat] = {
                categories[cat]['tot += 1
                if result['Data']:
                    categories] += 1
            
          ():
                pct = stats['available 0
                print(f"   {category}: 
            
            # Save results
            output_file = "individual_data_source_mapping.cs
           csvfile:
                fieldnames = ['Cat
                writer = csv.Des)
                writer.writehead
                for result in ts:
                    writer.writerow(result)
            
            print(f"\n✓ Detailed map")
            
            # Generate SQL mappript
            print(f"\n🔧 GENER)
            sql_script = generate_sql_mapping(mapping_analysis)
            
        
                f.write(sql_script)
            
            print(f"✓ SQL mapping 
            
            print(f"\n{'='*100}")
            print("✅ ANALYSIS COMPLETE!")
          0}")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

def generate_sql_mapping(mappalysis):
    """Generate SQL script for"
    
    sql_script = """-- Individuping
-- Generated from source table analysis
-- Maps AP

SELECT 
    -- REPORTING INFORMATION
    VARCHAR_FORMAT(CURRENT_TIMEte,
    CAST(c.CUST_ID AS VARCHAR(50)) AS customerIdeumber,
    
    -- PERSONAL PARTICULARS
    c.FIRST_NAME AS firstName,
    c.MIDDLE_NAME AS middleName
    CAST(NULL AS VARCHAR(100)) 
    TRIM(
        CO 
        COALESCE(c.MIDDLE_NAME,  || 
        COALESCE(c.SURNAME, ''
    ) AS fullNames,
    c.SURNAME AS presentSurname,
    c.MOTHER_SURNAME AS birthSurname,
    CASE 
        WHEN c.SEX = 'M' THEN '
        WHEN c.SEX = 'F' THEN emale'
        ELSE 'NotSpecified'
    END AS gender,
    family_cat.DESCRIPTION AS maritalStatus,
    CAST(Ne
    national_cat.DESCRIPTIOty,
    citizen_cat.DESCRIPTION AS cit
    CASE 
        WHEN c.NON_RESIDENT = 'ent'
        WHEN c.NON_RESIDENT = '1' THEN 'Non-Resident'
        ELnknown'
    END 
    profes_cat.DESion,
    indcode_cat.DESCRIPTIn,
    CASE 
        WHEN c.ENTRY_STATUS = ''
        ELSE 'Active'
    END AS fateStatus,
    profle
    profcat_cat.DESCRIPTION AS emploatus,
    c.SALARY_AMN AS monthlyIncome,
    c.NUM_OF_CHILDREN AS numbers,
    edulevel_cat.DESCRIPTION ASl,
    CAST(NULL AS DECIMAL(15,2)) AS averageMonthlyExpen
    CASE 
        WHEN c.BLACKLISTED_I
        WHEN c.AML_STATUS IS NOT NULLUS
        ELSE 'Clean'
    END AS negativeClientStatus
    
    -- SPO
    c.SPOUSE_NAME AS sName,
    CAST(NULL AS VARCHAR(50)) AS
    CAST(NULL AS VARCHAR(50)) A
    c.MOTHER_SURNAME AS maidenN
    CAST(NULL AS DECIMAL(15,2)) AS montilable
    
    -- BIRTH INFORMATION
    VARCHAR_FORMAT(c.DATE_OF_BIR,
    bcountry_cat.DESCRIPTION AS
    CAST(NULL AS VARCHAR(20)) A
    CAST(NULL AS VARCHAR(20)) AS birthHous
    region,
    CAST(NULL AS VARCHAR
    CAST(NULL AS VARCHAR(100)) AS birthWar
    c.BIRTHPLACE AS birthStreet,
    
    -- IDENTIFICATION INFORMATION
    id_typ,
    oid.ID_NO AS identifber,
    VARCHAR_FORMAT(oid.ISSUE_DATE, 'Date,
    VARCHAR_FORMAT(oid.EXPIRY_De,
    id_country.DESCRIPTION AS ice,
    CAST(NULL AS VARCHAR(200)) AS issuingAuavailable
    
    -- BNFORMATION
    CASE 
        WHEN c.CUST_TYPE =rate
        ELSE NULL
    END AS businessName,
    VARCHAR_FORMAT(c.CUSTOMER_B
    c.CHAMBER_ID AS businessRegistrationNumber,
    VARCHAte,
    CAST(NULL AS VARCHAR(50)) A
    afm.AFM_NO AS taxIdentific
    
    -- EMPLOYER INFORMATION
    c.EMPLOYER AS employerName,
    CAST(Nle
    CAST(NULL AS VARCHAR(100))
    CAST(NULL AS VARCHAR(100)) AS emble
    c.EMPLOYER_ADDRESS AS emplo
    CAST(NULL AS VARCHAR(20)) Ale
    CAST(NULL AS VARCHAR(20)) AS employerPostalCode,  -- Not 
    activire,
    
    -- INDIVIDUAL CONTACTS
    c.MOBILE_TEL AS mobileNumber,
    c.MOBILE_TEL2 AS alternatiNumber,
    c.TELEPHONE_1 AS fixedLineNumber,
    addr_c
    c.E_MAIL AS emailAddress,
    c.INTERNET_ADDRESS AS soci
    addr_comm.ADDRESS_1 AS mainA,
    
    -- PRIMARY ADDRESS (Communication Address)
    addr_ceet,
    CAST(NULL AS VARCHAR
    addr_comm.ZIP_CODE AS postalCode,
    addr_comm.REGION AS region,
    CAST(NULL AS VARCHAR(100))lable
    CAST(NULL AS VARCHAR(100)) AS ward,  -- Not aailable
    comm_c
    
    -- SECONDARY ADDRESS (Work Address)
    addr_work.ADDRESS_1 AS secot,
    CAST(NULL AS VARCHAR(20)) ailable
    addr_work.ZIP_CODE AS secondaryPostalCode,
    addr_w
    CAST
    CAST(NULL AS VARCHAR(10
    work_country.DESCRIPTION

FROM CUSTOMER c

-- Customer Categories (using subqueries to get specific caes)
LEFT JOIN 
    SELECT cc.FK_CUSTOMERCUST_ID,ION
    FROM CUSTOMER_CATEGORY cc
    JOIN GENERIC_DETAIL gd ON ccHEADPAR 
                          AND cUM
    WHERE cc.FK_CATEGORYCATEGOR = 'FAMILY' AND gd.FK_GENERIC_HET'
) family_c

LEFT JOIN (
    SELECT cc.FK_CUSTOMERCUST_I
    FROM CUSTOMER_CATEGORY cc
    JOIN GENERIC_DETAIL gd ON cc.FK_GENERIC_DETAFK = gd.FK_GENERIC_HEADPAR 
          IAL_NUM
    WHERE cc.FK_CATEGORYCATE = 'NATIO'
) national_cat ON national_cat.FK_ST_ID

LEFT JOIN (
    SELECT cc.FK_CUSTOMERCUST_ID, gd.DESCRIPTION
    FROM CY cc
    JOIN GENERIC_DETAIL gd ON EADPAR 
                          AND NUM
    WHERE cc.FK_CATEGORYCATEGOR 
) citizen_cat ON citizen_cat.FKCUST_ID

LEFT JOIN (
    SELECT cc.FK_CUSTOMERC
    FROM CUSTOMER_CATEGORY cc
    JOIN GENERIC_DETAIL gd ON ccDPAR 
                          AND cNUM
    WHERE cc.FK_CATEGORYCATEGOR = 'PROFES' AND gd.FK_GENERIC_PROFF'
) profes_cID

LEFT JOIN (
    SELECT cc.FK_CUSTOMERCUST_I
    FROM CUSTOMER_CATEGORY cc
    JOIN GENERIC_DETAIL gd ON cc.FK_GENERIC_DETAFK = gd.FK_GENERIC_AR 
         L_NUM
    W
) inST_ID

LEFT JOIN (
    SELECT cc.FK_CUSTOMERCUST_ID, PTION
    FROM CUScc
    JOIN GENERIC_DETAIL gd ON cc.FK_GENERIC_DETAFK = gd.FK_GENERIC_HEADPAR 
                          AN
    WHERE cc.FK_CATEGORYCATEGOR = 'PROFLEVL' AND gRFST'
) proflevl_cat ON proflevl_cT_ID

LEFT JOIN (
    SELECT cc.FKN
    FROM CUSTOMER_CATEGORY cc
    JOIN GENERIC_DETAIL gd ON cc.FK_GENERIC_DETAFK = g 
                          AND cc.FK_GENERIC_DETASER = gd.SERIAL_NUM
    WHERE cc.FK_TP'
) profcat_cat ON profcat_cat.FK_CUSTOMERCUST_ID = c.CUST_ID

LEFT JOIN (
    SELECT cc.FK_CUSTOMERCUST_ID, gd.DESCRIPTION
    FROM CUSTOMER_CATEGORY cc
    JOIN GENERIC_DETAIL gd ON cc.FK_GENEEADPAR 
                          AND cc.FK_GENERIAL_NUM
    WHERE cc.FK_CATELV'
) edulevel_cat O

LEFT JOIN (
    SELECT cc.FK_CUSTOMERCUST_ID, gd.DESCRIPTION
    FROM CUSTOMER_CATEGORY cc
    JOIN GENERIC_DETAIL gd ON cc.FK_GENERIC_DETAFK = gd.FK_GENERIC_HEADPAR 
                L_NUM
    WHERE cc.FK_CATEGORYCATEGOR = 
) bcountry_cat ON bcountry_cat.FK_CUSTOMERCUST_ID = c.CUST_ID

LEFT JOIN (
    SELECT cc.FK_CUSTOMERCUST_ID, gd.DES
    FROM CUSTOMER_CATEGORY cc
    JOIN GENERIC_DETAIL gd OAR 
            M
    WHERE cc.FK_CATEGORYCAIO'
) region_cat ON region_cat.FK_CUSTOMERCUST_ID = c.CUST_ID

LEFT JOIN (
    SELECT cc.FK_CUSTOMERCUST_ID, gd.DESCRIPTION
    FROM CUSTOMER_CATEGORY cc
    JOIN GENERIC_DETAIL gd ON cc.FK_GENERIC_DETAFK = gd.FK_GENERIC_HEADPAR 
                          AND cc.FK_GENERIC_DETASER = gd.SERIAL_NUM
    WHERE cc.FK_CATEGORYCATEGOR = 'ACTIVITY' AND gd.FK_GENERIC_E'
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

-- Address In
LEFT JOIN CUUST_ID
                        1'
             = '1'
LEFT JOIN GENERIC_DETAIL comm_country ON comm_country.FRY
                                     AND COUNTRY

LEFT JOIN CUST_A
                                AND
                                AND add= '1'
LEFT JOIN GENERIC_DETAIL work_countryUNTRY
                RY

WHERE c.ENTRY_STATUS = '1'  -- Active customers only
ORDER BY c.CUST_ID;

-- Additional queries for missing data investigation:

-- 1. Check for additional address types
-- SELECT DISTINCT ADDRE) 
-- FROM CUST_ADDRESS 
-- GROUP BY ADDRESS_TYPE;

-- 2. Check for additional customer category types
-- SELECT DISTINCT FK_CATEGORYCATEGOR, COUNT(*) 
-- FROM CUSTOMER_CATEGORY 
-- GROUP BY FK_CATEGORYCR;

-- 3. Check for spouse information in related tables
-- SELECT * FROM CUSTOMER WHERE SPOUSE_NAME

-- 4. Check for employment information
-- SELECT DISTINCT EMPLOYER, COUNT(*) 
-- FROM CUSTOMER 
-- WHERE EMPLOYER IS NOT NULL 
-- GROUP BY EMPLOYER 
-- FETCH FIRST 2ONLY;
"""
    
    return sql_sript

if __name__ == "__main__":
    analyze_individual_data_sources()