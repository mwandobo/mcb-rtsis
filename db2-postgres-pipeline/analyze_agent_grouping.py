#!/usr/bin/env python3
"""
Analyze what field groups agent customers together
"""

import sys
import os
import logging

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db2_connection import DB2Connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def analyze_agent_grouping():
    """Analyze what field groups agent customers together"""
    
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as connection:
            cursor = connection.cursor()
            
            logger.info("=" * 80)
            logger.info("ANALYZING WHAT GROUPS AGENT CUSTOMERS TOGETHER")
            logger.info("=" * 80)
            
            # Get all agent-like customers with detailed fields
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
                    c.FKGH_HAS_CUST_CATE,
                    c.FKGD_HAS_CUST_CATE,
                    c.FKGH_HAS_CUST_CLAS,
                    c.FKGD_HAS_CUST_CLAS,
                    c.FKGH_HAS_CUST_SEGM,
                    c.FKGD_HAS_CUST_SEGM
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
            logger.info("-" * 120)
            logger.info("ID     | Name                           | Type | Bus | VIP | Status | SegmFlags | Unit | Channel | Category | Class | Segment")
            logger.info("-" * 120)
            
            # Collect statistics
            cust_types = {}
            business_inds = {}
            vip_inds = {}
            cust_statuses = {}
            segm_flags = {}
            units = {}
            channels = {}
            categories = {}
            classes = {}
            segments = {}
            
            for row in results:
                cust_id, full_name, cust_type, business_ind, vip_ind, cust_status, segm_flag, employer, mobile, begin_date, unit_belongs, unit_serviced, channel, cat_head, cat_detail, class_head, class_detail, seg_head, seg_detail = row
                
                # Count occurrences
                cust_types[cust_type] = cust_types.get(cust_type, 0) + 1
                business_inds[business_ind] = business_inds.get(business_ind, 0) + 1
                vip_inds[vip_ind] = vip_inds.get(vip_ind, 0) + 1
                cust_statuses[cust_status] = cust_statuses.get(cust_status, 0) + 1
                segm_flags[segm_flag] = segm_flags.get(segm_flag, 0) + 1
                units[unit_belongs] = units.get(unit_belongs, 0) + 1
                channels[channel] = channels.get(channel, 0) + 1
                categories[cat_detail] = categories.get(cat_detail, 0) + 1
                classes[class_detail] = classes.get(class_detail, 0) + 1
                segments[seg_detail] = segments.get(seg_detail, 0) + 1
                
                logger.info(f"{cust_id:6} | {full_name[:30]:30} | {cust_type:4} | {business_ind:3} | {vip_ind:3} | {cust_status:6} | {str(segm_flag)[:9]:9} | {str(unit_belongs)[:4]:4} | {str(channel)[:7]:7} | {str(cat_detail)[:8]:8} | {str(class_detail)[:5]:5} | {str(seg_detail)[:7]:7}")
            
            # Print statistics
            logger.info("\n" + "=" * 80)
            logger.info("📊 GROUPING ANALYSIS:")
            logger.info("=" * 80)
            
            logger.info(f"\n🔍 CUST_TYPE Distribution:")
            for key, count in sorted(cust_types.items()):
                logger.info(f"  {key}: {count} customers")
            
            logger.info(f"\n🔍 BUSINESS_IND Distribution:")
            for key, count in sorted(business_inds.items()):
                logger.info(f"  {key}: {count} customers")
            
            logger.info(f"\n🔍 VIP_IND Distribution:")
            for key, count in sorted(vip_inds.items()):
                logger.info(f"  {key}: {count} customers")
            
            logger.info(f"\n🔍 CUST_STATUS Distribution:")
            for key, count in sorted(cust_statuses.items()):
                logger.info(f"  {key}: {count} customers")
            
            logger.info(f"\n🔍 SEGM_FLAGS Distribution:")
            for key, count in sorted(segm_flags.items()):
                if key and str(key).strip():
                    logger.info(f"  '{key}': {count} customers")
            
            logger.info(f"\n🔍 UNIT_BELONGS Distribution (Top 10):")
            sorted_units = sorted(units.items(), key=lambda x: x[1], reverse=True)[:10]
            for key, count in sorted_units:
                if key:
                    logger.info(f"  {key}: {count} customers")
            
            logger.info(f"\n🔍 CHANNEL Distribution:")
            for key, count in sorted(channels.items()):
                if key:
                    logger.info(f"  {key}: {count} customers")
            
            logger.info(f"\n🔍 CATEGORY Distribution:")
            for key, count in sorted(categories.items()):
                if key:
                    logger.info(f"  {key}: {count} customers")
            
            logger.info(f"\n🔍 CLASS Distribution:")
            for key, count in sorted(classes.items()):
                if key:
                    logger.info(f"  {key}: {count} customers")
            
            logger.info(f"\n🔍 SEGMENT Distribution:")
            for key, count in sorted(segments.items()):
                if key:
                    logger.info(f"  {key}: {count} customers")
            
            # Check if there's a common pattern
            logger.info("\n" + "=" * 80)
            logger.info("💡 GROUPING INSIGHTS:")
            logger.info("=" * 80)
            
            # Check for most common values
            most_common_unit = max(units.items(), key=lambda x: x[1]) if units else (None, 0)
            most_common_channel = max(channels.items(), key=lambda x: x[1]) if channels else (None, 0)
            most_common_category = max(categories.items(), key=lambda x: x[1]) if categories else (None, 0)
            
            logger.info(f"Most common UNIT_BELONGS: {most_common_unit[0]} ({most_common_unit[1]} customers)")
            logger.info(f"Most common CHANNEL: {most_common_channel[0]} ({most_common_channel[1]} customers)")
            logger.info(f"Most common CATEGORY: {most_common_category[0]} ({most_common_category[1]} customers)")
            
            # Check if there's a specific field that could identify agents
            if most_common_category[1] > len(results) * 0.7:  # If 70%+ have same category
                logger.info(f"\n🎯 POTENTIAL GROUPING FIELD: CUSTOMER CATEGORY = {most_common_category[0]}")
                logger.info("   This could be used to identify agents instead of name patterns!")
            
            if most_common_channel[1] > len(results) * 0.7:  # If 70%+ have same channel
                logger.info(f"\n🎯 POTENTIAL GROUPING FIELD: DISTRIBUTION CHANNEL = {most_common_channel[0]}")
                logger.info("   This could be used to identify agents instead of name patterns!")
            
            if most_common_unit[1] > len(results) * 0.7:  # If 70%+ belong to same unit
                logger.info(f"\n🎯 POTENTIAL GROUPING FIELD: UNIT_BELONGS = {most_common_unit[0]}")
                logger.info("   This could be used to identify agents instead of name patterns!")
            
            logger.info("\n" + "=" * 80)
            
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    analyze_agent_grouping()