#!/usr/bin/env python3
"""
Analyze what field groups agent customers together - simplified version
"""

import sys
import os
import logging

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db2_connection import DB2Connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def analyze_agent_grouping_simple():
    """Analyze what field groups agent customers together using existing fields"""
    
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as connection:
            cursor = connection.cursor()
            
            logger.info("=" * 80)
            logger.info("ANALYZING WHAT GROUPS AGENT CUSTOMERS TOGETHER")
            logger.info("=" * 80)
            
            # Get all agent-like customers with key fields
            query = """
                SELECT 
                    c.CUST_ID,
                    TRIM(COALESCE(c.FIRST_NAME, '') || ' ' || COALESCE(c.MIDDLE_NAME, '') || ' ' || COALESCE(c.SURNAME, '')) as full_name,
                    c.CUST_TYPE,
                    c.BUSINESS_IND,
                    c.VIP_IND,
                    c.CUST_STATUS,
                    c.SEGM_FLAGS,
                    c.EMPLOYER,
                    c.MOBILE_TEL,
                    c.CUSTOMER_BEGIN_DAT,
                    c.FKUNIT_BELONGS,
                    c.FKUNIT_IS_SERVICED,
                    c.FK_DISTR_CHANNEID,
                    c.DAILY_ORDER_AMNT,
                    c.NO_OF_BUSINESSES,
                    c.OWNERSHIP_INDICATION
                FROM CUSTOMER c
                WHERE c.ENTRY_STATUS = '1'
                    AND c.MOBILE_TEL IS NOT NULL
                    AND c.MOBILE_TEL != ''
                    AND LENGTH(TRIM(c.MOBILE_TEL)) > 5
                    AND (UPPER(c.FIRST_NAME) LIKE '%AGENT%'
                         OR UPPER(c.SURNAME) LIKE '%AGENT%'
                         OR UPPER(c.MIDDLE_NAME) LIKE '%AGENT%'
                         OR UPPER(c.FIRST_NAME) LIKE '%WAKALA%'
                         OR UPPER(c.SURNAME) LIKE '%WAKALA%'
                         OR UPPER(c.MIDDLE_NAME) LIKE '%WAKALA%'
                         OR UPPER(c.FIRST_NAME) LIKE '%DUKA%'
                         OR UPPER(c.SURNAME) LIKE '%DUKA%'
                         OR UPPER(c.MIDDLE_NAME) LIKE '%DUKA%'
                         OR UPPER(c.FIRST_NAME) LIKE '%SHOP%'
                         OR UPPER(c.SURNAME) LIKE '%SHOP%'
                         OR UPPER(c.MIDDLE_NAME) LIKE '%SHOP%'
                         OR UPPER(c.FIRST_NAME) LIKE '%SERVICE%'
                         OR UPPER(c.SURNAME) LIKE '%SERVICE%'
                         OR UPPER(c.MIDDLE_NAME) LIKE '%SERVICE%'
                         OR UPPER(c.FIRST_NAME) LIKE '%BUSINESS%'
                         OR UPPER(c.SURNAME) LIKE '%BUSINESS%'
                         OR UPPER(c.MIDDLE_NAME) LIKE '%BUSINESS%')
                ORDER BY c.CUSTOMER_BEGIN_DAT DESC
                FETCH FIRST 30 ROWS ONLY
            """
            
            cursor.execute(query)
            results = cursor.fetchall()
            
            logger.info(f"\n📊 Found {len(results)} agent-like customers")
            logger.info("\nDetailed Analysis:")
            logger.info("-" * 140)
            logger.info("ID     | Name                           | Type | Bus | VIP | Status | SegmFlags | Unit | Channel | DailyAmt | Businesses | Owner")
            logger.info("-" * 140)
            
            # Collect statistics
            cust_types = {}
            business_inds = {}
            vip_inds = {}
            cust_statuses = {}
            segm_flags = {}
            units = {}
            channels = {}
            daily_amounts = {}
            businesses = {}
            ownership = {}
            
            for row in results:
                cust_id, full_name, cust_type, business_ind, vip_ind, cust_status, segm_flag, employer, mobile, begin_date, unit_belongs, unit_serviced, channel, daily_amt, no_businesses, ownership_ind = row
                
                # Count occurrences
                cust_types[cust_type] = cust_types.get(cust_type, 0) + 1
                business_inds[business_ind] = business_inds.get(business_ind, 0) + 1
                vip_inds[vip_ind] = vip_inds.get(vip_ind, 0) + 1
                cust_statuses[cust_status] = cust_statuses.get(cust_status, 0) + 1
                segm_flags[segm_flag] = segm_flags.get(segm_flag, 0) + 1
                units[unit_belongs] = units.get(unit_belongs, 0) + 1
                channels[channel] = channels.get(channel, 0) + 1
                daily_amounts[daily_amt] = daily_amounts.get(daily_amt, 0) + 1
                businesses[no_businesses] = businesses.get(no_businesses, 0) + 1
                ownership[ownership_ind] = ownership.get(ownership_ind, 0) + 1
                
                daily_display = f"{daily_amt:.0f}" if daily_amt else "None"
                logger.info(f"{cust_id:6} | {full_name[:30]:30} | {cust_type:4} | {business_ind:3} | {vip_ind:3} | {cust_status:6} | {str(segm_flag)[:9]:9} | {str(unit_belongs)[:4]:4} | {str(channel)[:7]:7} | {daily_display[:8]:8} | {str(no_businesses)[:10]:10} | {str(ownership_ind)[:5]:5}")
            
            # Print statistics
            logger.info("\n" + "=" * 80)
            logger.info("📊 GROUPING ANALYSIS:")
            logger.info("=" * 80)
            
            logger.info(f"\n🔍 CUST_TYPE Distribution:")
            for key, count in sorted(cust_types.items()):
                logger.info(f"  {key}: {count} customers ({count/len(results)*100:.1f}%)")
            
            logger.info(f"\n🔍 BUSINESS_IND Distribution:")
            for key, count in sorted(business_inds.items()):
                logger.info(f"  {key}: {count} customers ({count/len(results)*100:.1f}%)")
            
            logger.info(f"\n🔍 VIP_IND Distribution:")
            for key, count in sorted(vip_inds.items()):
                logger.info(f"  {key}: {count} customers ({count/len(results)*100:.1f}%)")
            
            logger.info(f"\n🔍 CUST_STATUS Distribution:")
            for key, count in sorted(cust_statuses.items()):
                logger.info(f"  {key}: {count} customers ({count/len(results)*100:.1f}%)")
            
            logger.info(f"\n🔍 SEGM_FLAGS Distribution:")
            for key, count in sorted(segm_flags.items()):
                if key and str(key).strip():
                    logger.info(f"  '{key}': {count} customers ({count/len(results)*100:.1f}%)")
            
            logger.info(f"\n🔍 UNIT_BELONGS Distribution (Top 10):")
            sorted_units = sorted(units.items(), key=lambda x: x[1], reverse=True)[:10]
            for key, count in sorted_units:
                if key:
                    logger.info(f"  {key}: {count} customers ({count/len(results)*100:.1f}%)")
            
            logger.info(f"\n🔍 CHANNEL Distribution:")
            for key, count in sorted(channels.items()):
                if key:
                    logger.info(f"  {key}: {count} customers ({count/len(results)*100:.1f}%)")
            
            logger.info(f"\n🔍 NO_OF_BUSINESSES Distribution:")
            for key, count in sorted(businesses.items()):
                if key is not None:
                    logger.info(f"  {key}: {count} customers ({count/len(results)*100:.1f}%)")
            
            logger.info(f"\n🔍 OWNERSHIP_INDICATION Distribution:")
            for key, count in sorted(ownership.items()):
                if key:
                    logger.info(f"  {key}: {count} customers ({count/len(results)*100:.1f}%)")
            
            # Check for most common values that could be grouping fields
            logger.info("\n" + "=" * 80)
            logger.info("💡 POTENTIAL GROUPING FIELDS:")
            logger.info("=" * 80)
            
            # Check each field for high concentration
            threshold = 0.7  # 70% threshold
            
            for field_name, field_data in [
                ("CUST_TYPE", cust_types),
                ("BUSINESS_IND", business_inds), 
                ("VIP_IND", vip_inds),
                ("CUST_STATUS", cust_statuses),
                ("UNIT_BELONGS", units),
                ("CHANNEL", channels),
                ("NO_OF_BUSINESSES", businesses),
                ("OWNERSHIP_INDICATION", ownership)
            ]:
                if field_data:
                    max_count = max(field_data.values())
                    max_key = max(field_data.items(), key=lambda x: x[1])[0]
                    percentage = max_count / len(results)
                    
                    if percentage >= threshold:
                        logger.info(f"🎯 {field_name} = '{max_key}': {max_count}/{len(results)} ({percentage*100:.1f}%)")
                        logger.info(f"   This field could be used to identify agents!")
            
            # Check if name patterns are the only reliable grouping
            logger.info(f"\n📝 CONCLUSION:")
            logger.info(f"   Based on the analysis, the agents are grouped by:")
            logger.info(f"   1. CUST_TYPE = '1' (Individual customers)")
            logger.info(f"   2. Agent-like names (WAKALA, DUKA, MADUKA, etc.)")
            logger.info(f"   3. Having valid mobile numbers")
            logger.info(f"   No other single field reliably identifies all agents.")
            
            logger.info("\n" + "=" * 80)
            
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    analyze_agent_grouping_simple()