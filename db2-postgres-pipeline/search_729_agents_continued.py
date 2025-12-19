#!/usr/bin/env python3
"""
Continue searching for the 729 agents - check specific patterns
"""

import sys
import os
import logging

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db2_connection import DB2Connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def search_729_agents_continued():
    """Continue searching for 729 agents with specific patterns"""
    
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as connection:
            cursor = connection.cursor()
            
            logger.info("=" * 80)
            logger.info("CONTINUED SEARCH FOR 729 AGENTS")
            logger.info("=" * 80)
            
            # 1. Check for exactly 729 customers with specific criteria
            logger.info("\n🔍 Looking for exactly 729 customers with various criteria:")
            
            # Check different combinations that might give 729
            criteria_list = [
                ("CUST_TYPE = '2' AND MOBILE_TEL IS NOT NULL", "Corporate customers with mobile"),
                ("CUST_TYPE = '1' AND BUSINESS_IND = '1'", "Individual customers marked as business"),
                ("CUST_TYPE = '1' AND VIP_IND = '1'", "Individual VIP customers"),
                ("CUST_STATUS = '1' AND MOBILE_TEL IS NOT NULL", "Status 1 customers with mobile"),
                ("FKUNIT_BELONGS = 201 AND MOBILE_TEL IS NOT NULL", "Unit 201 customers with mobile"),
                ("FKUNIT_BELONGS = 200 AND MOBILE_TEL IS NOT NULL", "Unit 200 customers with mobile"),
                ("CUSTOMER_BEGIN_DAT >= '2020-01-01' AND MOBILE_TEL IS NOT NULL", "Customers from 2020+ with mobile"),
                ("CUSTOMER_BEGIN_DAT >= '2018-01-01' AND CUSTOMER_BEGIN_DAT < '2019-01-01' AND MOBILE_TEL IS NOT NULL", "2018 customers with mobile"),
            ]
            
            for criteria, description in criteria_list:
                try:
                    query = f"""
                        SELECT COUNT(*) as count
                        FROM CUSTOMER c
                        WHERE c.ENTRY_STATUS = '1'
                            AND ({criteria})
                    """
                    cursor.execute(query)
                    count = cursor.fetchone()[0]
                    
                    if count == 729:
                        logger.info(f"  🎯 FOUND 729: {description}")
                        logger.info(f"     Criteria: {criteria}")
                        
                        # Get sample records
                        sample_query = f"""
                            SELECT 
                                c.CUST_ID,
                                TRIM(COALESCE(c.FIRST_NAME, '') || ' ' || COALESCE(c.SURNAME, '')) as name,
                                c.CUST_TYPE,
                                c.MOBILE_TEL,
                                c.BUSINESS_IND,
                                c.VIP_IND,
                                c.CUST_STATUS
                            FROM CUSTOMER c
                            WHERE c.ENTRY_STATUS = '1'
                                AND ({criteria})
                            ORDER BY c.CUST_ID
                            FETCH FIRST 10 ROWS ONLY
                        """
                        cursor.execute(sample_query)
                        samples = cursor.fetchall()
                        
                        logger.info("     Sample records:")
                        for cust_id, name, cust_type, mobile, bus_ind, vip_ind, status in samples:
                            mobile_display = mobile if mobile else "No Mobile"
                            logger.info(f"       {cust_id:6}: {name:30} | Type:{cust_type} | {mobile_display:15} | Bus:{bus_ind} | VIP:{vip_ind} | Status:{status}")
                    else:
                        logger.info(f"  {count:4}: {description}")
                        
                except Exception as e:
                    logger.info(f"  Error: {description} - {e}")
            
            # 2. Check for customers with specific employee assignments that might total 729
            logger.info("\n🔍 Checking employee assignments for 729 total:")
            query2 = """
                SELECT 
                    c.FK_BANKEMPLOYEEID,
                    be.FIRST_NAME,
                    be.LAST_NAME,
                    COUNT(*) as customer_count
                FROM CUSTOMER c
                JOIN BANKEMPLOYEE be ON be.ID = c.FK_BANKEMPLOYEEID
                WHERE c.ENTRY_STATUS = '1'
                    AND c.MOBILE_TEL IS NOT NULL
                    AND c.MOBILE_TEL != ''
                    AND be.EMPL_STATUS = '1'
                GROUP BY c.FK_BANKEMPLOYEEID, be.FIRST_NAME, be.LAST_NAME
                ORDER BY customer_count DESC
                FETCH FIRST 30 ROWS ONLY
            """
            cursor.execute(query2)
            emp_assignments = cursor.fetchall()
            
            # Check if any combination of employees manages exactly 729 customers
            total_customers = 0
            for emp_id, first_name, last_name, count in emp_assignments:
                total_customers += count
                name = f"{first_name or ''} {last_name or ''}".strip()
                logger.info(f"  {emp_id}: {name:25} manages {count:4} customers (running total: {total_customers:5})")
                
                if count == 729:
                    logger.info(f"  🎯 FOUND: Employee {emp_id} ({name}) manages exactly 729 customers!")
                    break
                elif total_customers >= 729:
                    break
            
            # 3. Check for customers in specific date ranges
            logger.info("\n🔍 Checking specific date ranges for 729 customers:")
            
            # Check monthly registrations
            query3 = """
                SELECT 
                    YEAR(c.CUSTOMER_BEGIN_DAT) as reg_year,
                    MONTH(c.CUSTOMER_BEGIN_DAT) as reg_month,
                    COUNT(*) as count
                FROM CUSTOMER c
                WHERE c.ENTRY_STATUS = '1'
                    AND c.MOBILE_TEL IS NOT NULL
                    AND c.MOBILE_TEL != ''
                    AND c.CUSTOMER_BEGIN_DAT IS NOT NULL
                GROUP BY YEAR(c.CUSTOMER_BEGIN_DAT), MONTH(c.CUSTOMER_BEGIN_DAT)
                ORDER BY count DESC
                FETCH FIRST 20 ROWS ONLY
            """
            cursor.execute(query3)
            date_ranges = cursor.fetchall()
            
            for year, month, count in date_ranges:
                if count == 729:
                    logger.info(f"  🎯 FOUND: {year}-{month:02d} has exactly 729 customers!")
                    
                    # Get sample from this month
                    sample_query = f"""
                        SELECT 
                            c.CUST_ID,
                            TRIM(COALESCE(c.FIRST_NAME, '') || ' ' || COALESCE(c.SURNAME, '')) as name,
                            c.CUST_TYPE,
                            c.MOBILE_TEL
                        FROM CUSTOMER c
                        WHERE c.ENTRY_STATUS = '1'
                            AND c.MOBILE_TEL IS NOT NULL
                            AND c.MOBILE_TEL != ''
                            AND YEAR(c.CUSTOMER_BEGIN_DAT) = {year}
                            AND MONTH(c.CUSTOMER_BEGIN_DAT) = {month}
                        ORDER BY c.CUST_ID
                        FETCH FIRST 10 ROWS ONLY
                    """
                    cursor.execute(sample_query)
                    samples = cursor.fetchall()
                    
                    logger.info(f"     Sample customers from {year}-{month:02d}:")
                    for cust_id, name, cust_type, mobile in samples:
                        logger.info(f"       {cust_id:6}: {name:30} | Type:{cust_type} | {mobile}")
                    break
                else:
                    logger.info(f"  {year}-{month:02d}: {count:4} customers")
            
            # 4. Check for customers with specific account patterns
            logger.info("\n🔍 Checking for customers with specific account patterns:")
            
            # This might fail if ACCOUNT table doesn't exist, but let's try
            try:
                query4 = """
                    SELECT COUNT(DISTINCT c.CUST_ID) as unique_customers
                    FROM CUSTOMER c
                    WHERE c.ENTRY_STATUS = '1'
                        AND c.MOBILE_TEL IS NOT NULL
                        AND c.MOBILE_TEL != ''
                        AND EXISTS (
                            SELECT 1 FROM GLI_TRX_EXTRACT gte 
                            WHERE gte.CUST_ID = c.CUST_ID 
                            AND gte.TRN_DATE >= DATE('2024-01-01')
                        )
                """
                cursor.execute(query4)
                active_customers = cursor.fetchone()[0]
                logger.info(f"  Customers with transactions in 2024: {active_customers:,}")
                
                if active_customers == 729:
                    logger.info("  🎯 FOUND: 729 customers with transactions in 2024!")
                
            except Exception as e:
                logger.info(f"  Transaction check failed: {e}")
            
            # 5. Check for customers with specific mobile number patterns
            logger.info("\n🔍 Checking mobile number patterns:")
            
            mobile_patterns = [
                ("MOBILE_TEL LIKE '0%'", "Numbers starting with 0"),
                ("MOBILE_TEL LIKE '255%'", "Numbers starting with 255"),
                ("LENGTH(MOBILE_TEL) = 10", "10-digit numbers"),
                ("LENGTH(MOBILE_TEL) = 12", "12-digit numbers"),
                ("MOBILE_TEL LIKE '%255%'", "Numbers containing 255"),
            ]
            
            for pattern, description in mobile_patterns:
                try:
                    query = f"""
                        SELECT COUNT(*) as count
                        FROM CUSTOMER c
                        WHERE c.ENTRY_STATUS = '1'
                            AND c.MOBILE_TEL IS NOT NULL
                            AND c.MOBILE_TEL != ''
                            AND {pattern}
                    """
                    cursor.execute(query)
                    count = cursor.fetchone()[0]
                    
                    if count == 729:
                        logger.info(f"  🎯 FOUND 729: {description}")
                    else:
                        logger.info(f"  {count:5}: {description}")
                        
                except Exception as e:
                    logger.info(f"  Error checking {description}: {e}")
            
            logger.info("\n" + "=" * 80)
            logger.info("💡 SEARCH SUMMARY:")
            logger.info("  Looking for exactly 729 customers with various criteria")
            logger.info("  Check above for any 🎯 FOUND markers")
            logger.info("  If no exact match found, the 729 might be from:")
            logger.info("  - A specific report or view")
            logger.info("  - A combination of multiple criteria")
            logger.info("  - A different table or data source")
            logger.info("=" * 80)
            
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    search_729_agents_continued()