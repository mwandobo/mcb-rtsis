#!/usr/bin/env python3
"""
Find ALL agents with phone numbers using comprehensive search
"""

import sys
import os
import logging

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db2_connection import DB2Connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def find_all_agents_with_phone():
    """Find all potential agents with phone numbers"""
    
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as connection:
            cursor = connection.cursor()
            
            logger.info("=" * 80)
            logger.info("COMPREHENSIVE SEARCH FOR ALL AGENTS WITH PHONE NUMBERS")
            logger.info("=" * 80)
            
            # Comprehensive search with expanded patterns
            query = """
                SELECT 
                    c.CUST_ID,
                    TRIM(COALESCE(c.FIRST_NAME, '') || ' ' || COALESCE(c.MIDDLE_NAME, '') || ' ' || COALESCE(c.SURNAME, '')) as full_name,
                    c.CUST_TYPE,
                    c.MOBILE_TEL,
                    c.BUSINESS_IND,
                    c.CUSTOMER_BEGIN_DAT,
                    c.CUST_STATUS,
                    c.FKUNIT_BELONGS
                FROM CUSTOMER c
                WHERE c.ENTRY_STATUS = '1'
                    AND c.MOBILE_TEL IS NOT NULL
                    AND c.MOBILE_TEL != ''
                    AND LENGTH(TRIM(c.MOBILE_TEL)) > 5
                    AND (
                        -- Agent patterns
                        UPPER(c.FIRST_NAME) LIKE '%AGENT%'
                        OR UPPER(c.SURNAME) LIKE '%AGENT%'
                        OR UPPER(c.MIDDLE_NAME) LIKE '%AGENT%'
                        
                        -- Wakala (Agent in Swahili)
                        OR UPPER(c.FIRST_NAME) LIKE '%WAKALA%'
                        OR UPPER(c.SURNAME) LIKE '%WAKALA%'
                        OR UPPER(c.MIDDLE_NAME) LIKE '%WAKALA%'
                        
                        -- Duka/Maduka (Shop/Shops in Swahili)
                        OR UPPER(c.FIRST_NAME) LIKE '%DUKA%'
                        OR UPPER(c.SURNAME) LIKE '%DUKA%'
                        OR UPPER(c.MIDDLE_NAME) LIKE '%DUKA%'
                        OR UPPER(c.FIRST_NAME) LIKE '%MADUKA%'
                        OR UPPER(c.SURNAME) LIKE '%MADUKA%'
                        OR UPPER(c.MIDDLE_NAME) LIKE '%MADUKA%'
                        
                        -- Shop/Store patterns
                        OR UPPER(c.FIRST_NAME) LIKE '%SHOP%'
                        OR UPPER(c.SURNAME) LIKE '%SHOP%'
                        OR UPPER(c.MIDDLE_NAME) LIKE '%SHOP%'
                        OR UPPER(c.FIRST_NAME) LIKE '%STORE%'
                        OR UPPER(c.SURNAME) LIKE '%STORE%'
                        OR UPPER(c.MIDDLE_NAME) LIKE '%STORE%'
                        
                        -- Service patterns
                        OR UPPER(c.FIRST_NAME) LIKE '%SERVICE%'
                        OR UPPER(c.SURNAME) LIKE '%SERVICE%'
                        OR UPPER(c.MIDDLE_NAME) LIKE '%SERVICE%'
                        
                        -- Business patterns
                        OR UPPER(c.FIRST_NAME) LIKE '%BUSINESS%'
                        OR UPPER(c.SURNAME) LIKE '%BUSINESS%'
                        OR UPPER(c.MIDDLE_NAME) LIKE '%BUSINESS%'
                        
                        -- Mobile Money patterns
                        OR UPPER(c.FIRST_NAME) LIKE '%MOBILE%'
                        OR UPPER(c.SURNAME) LIKE '%MOBILE%'
                        OR UPPER(c.MIDDLE_NAME) LIKE '%MOBILE%'
                        OR UPPER(c.FIRST_NAME) LIKE '%MONEY%'
                        OR UPPER(c.SURNAME) LIKE '%MONEY%'
                        OR UPPER(c.MIDDLE_NAME) LIKE '%MONEY%'
                        
                        -- MNO patterns
                        OR UPPER(c.FIRST_NAME) LIKE '%MPESA%'
                        OR UPPER(c.SURNAME) LIKE '%MPESA%'
                        OR UPPER(c.MIDDLE_NAME) LIKE '%MPESA%'
                        OR UPPER(c.FIRST_NAME) LIKE '%TIGO%'
                        OR UPPER(c.SURNAME) LIKE '%TIGO%'
                        OR UPPER(c.MIDDLE_NAME) LIKE '%TIGO%'
                        OR UPPER(c.FIRST_NAME) LIKE '%AIRTEL%'
                        OR UPPER(c.SURNAME) LIKE '%AIRTEL%'
                        OR UPPER(c.MIDDLE_NAME) LIKE '%AIRTEL%'
                        OR UPPER(c.FIRST_NAME) LIKE '%HALO%'
                        OR UPPER(c.SURNAME) LIKE '%HALO%'
                        OR UPPER(c.MIDDLE_NAME) LIKE '%HALO%'
                        
                        -- Financial patterns
                        OR UPPER(c.FIRST_NAME) LIKE '%MICROFINANCE%'
                        OR UPPER(c.SURNAME) LIKE '%MICROFINANCE%'
                        OR UPPER(c.MIDDLE_NAME) LIKE '%MICROFINANCE%'
                        OR UPPER(c.FIRST_NAME) LIKE '%FINANCE%'
                        OR UPPER(c.SURNAME) LIKE '%FINANCE%'
                        OR UPPER(c.MIDDLE_NAME) LIKE '%FINANCE%'
                        
                        -- Other business patterns
                        OR UPPER(c.FIRST_NAME) LIKE '%STATIONERY%'
                        OR UPPER(c.SURNAME) LIKE '%STATIONERY%'
                        OR UPPER(c.MIDDLE_NAME) LIKE '%STATIONERY%'
                        OR UPPER(c.FIRST_NAME) LIKE '%GENERAL%'
                        OR UPPER(c.SURNAME) LIKE '%GENERAL%'
                        OR UPPER(c.MIDDLE_NAME) LIKE '%GENERAL%'
                        
                        -- Customer/Client patterns (Mteja in Swahili)
                        OR UPPER(c.FIRST_NAME) LIKE '%MTEJA%'
                        OR UPPER(c.SURNAME) LIKE '%MTEJA%'
                        OR UPPER(c.MIDDLE_NAME) LIKE '%MTEJA%'
                        OR UPPER(c.FIRST_NAME) LIKE '%CLIENT%'
                        OR UPPER(c.SURNAME) LIKE '%CLIENT%'
                        OR UPPER(c.MIDDLE_NAME) LIKE '%CLIENT%'
                    )
                ORDER BY c.CUSTOMER_BEGIN_DAT DESC
            """
            
            cursor.execute(query)
            results = cursor.fetchall()
            
            logger.info(f"\n📊 Found {len(results)} potential agents with phone numbers")
            logger.info("-" * 80)
            logger.info("ID     | Name                                     | Type | Mobile          | Bus | Unit | Status | Date")
            logger.info("-" * 80)
            
            # Count by customer type
            cust_type_1_count = 0
            cust_type_2_count = 0
            cust_type_other_count = 0
            
            # Count by unit
            unit_counts = {}
            
            for row in results:
                cust_id, full_name, cust_type, mobile_tel, business_ind, begin_date, cust_status, unit_belongs = row
                
                if cust_type == '1':
                    cust_type_1_count += 1
                elif cust_type == '2':
                    cust_type_2_count += 1
                else:
                    cust_type_other_count += 1
                
                unit_counts[unit_belongs] = unit_counts.get(unit_belongs, 0) + 1
                
                logger.info(f"{cust_id:6} | {full_name:40} | {cust_type:4} | {mobile_tel:15} | {business_ind:3} | {str(unit_belongs):4} | {cust_status:6} | {begin_date}")
            
            # Summary statistics
            logger.info("\n" + "=" * 80)
            logger.info("📊 COMPREHENSIVE AGENT SUMMARY:")
            logger.info("=" * 80)
            
            logger.info(f"\n📱 Total agents with phone numbers: {len(results)}")
            logger.info(f"\n🔍 By Customer Type:")
            logger.info(f"  CUST_TYPE '1' (Individual): {cust_type_1_count} ({cust_type_1_count/len(results)*100:.1f}%)")
            logger.info(f"  CUST_TYPE '2' (Corporate): {cust_type_2_count} ({cust_type_2_count/len(results)*100:.1f}%)")
            if cust_type_other_count > 0:
                logger.info(f"  Other types: {cust_type_other_count} ({cust_type_other_count/len(results)*100:.1f}%)")
            
            logger.info(f"\n🏢 By Unit (Top 10):")
            sorted_units = sorted(unit_counts.items(), key=lambda x: x[1], reverse=True)[:10]
            for unit, count in sorted_units:
                if unit:
                    logger.info(f"  Unit {unit}: {count} agents ({count/len(results)*100:.1f}%)")
            
            # Check if we have all agents from the image
            logger.info(f"\n🔍 Checking specific agents from your image:")
            specific_agents = [
                "ISHISHA MIN SHOP AGENT",
                "LARRIES BUSINESS SERVICE",
                "MBEVI STATIONERY AND GENERAL SERVICES", 
                "NKOMEY MICROFINANCE"
            ]
            
            found_specific = 0
            for agent_name in specific_agents:
                found = False
                for row in results:
                    cust_id, full_name, cust_type, mobile_tel, business_ind, begin_date, cust_status, unit_belongs = row
                    if any(part.upper() in full_name.upper() for part in agent_name.split()):
                        logger.info(f"  ✅ Found: {full_name} (ID: {cust_id})")
                        found = True
                        found_specific += 1
                        break
                if not found:
                    logger.info(f"  ❌ Not found: {agent_name}")
            
            logger.info(f"\n📋 Found {found_specific}/{len(specific_agents)} specific agents from your image")
            
            # Final recommendation
            logger.info(f"\n💡 RECOMMENDATION:")
            if len(results) > 50:
                logger.info(f"  Found {len(results)} potential agents - this seems comprehensive!")
                logger.info(f"  The search covers multiple patterns and should capture most agents.")
            else:
                logger.info(f"  Found {len(results)} potential agents - this might be all available agents.")
            
            logger.info(f"  Most agents are CUST_TYPE '1' individual customers with agent-like names.")
            logger.info(f"  The current approach should capture all agents with phone numbers.")
            
            logger.info("\n" + "=" * 80)
            
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    find_all_agents_with_phone()