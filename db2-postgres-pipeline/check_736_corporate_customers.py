#!/usr/bin/env python3
"""
Check the 736 corporate customers with mobile - might be the 729 agents
"""

import sys
import os
import logging

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db2_connection import DB2Connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_736_corporate_customers():
    """Check the 736 corporate customers - might be the 729 agents"""
    
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as connection:
            cursor = connection.cursor()
            
            logger.info("=" * 80)
            logger.info("CHECKING 736 CORPORATE CUSTOMERS (CLOSEST TO 729)")
            logger.info("=" * 80)
            
            # Get all corporate customers with mobile numbers
            query = """
                SELECT 
                    c.CUST_ID,
                    TRIM(COALESCE(c.FIRST_NAME, '') || ' ' || COALESCE(c.MIDDLE_NAME, '') || ' ' || COALESCE(c.SURNAME, '')) as name,
                    c.CUST_TYPE,
                    c.MOBILE_TEL,
                    c.BUSINESS_IND,
                    c.VIP_IND,
                    c.CUST_STATUS,
                    c.CUSTOMER_BEGIN_DAT,
                    c.FKUNIT_BELONGS,
                    c.FK_BANKEMPLOYEEID
                FROM CUSTOMER c
                WHERE c.ENTRY_STATUS = '1'
                    AND c.CUST_TYPE = '2'
                    AND c.MOBILE_TEL IS NOT NULL
                    AND c.MOBILE_TEL != ''
                ORDER BY c.CUSTOMER_BEGIN_DAT DESC
            """
            cursor.execute(query)
            corporate_customers = cursor.fetchall()
            
            total_count = len(corporate_customers)
            logger.info(f"\n📊 Found {total_count} corporate customers with mobile numbers")
            
            if total_count == 729:
                logger.info("🎯 EXACT MATCH! These are likely the 729 agents!")
            elif abs(total_count - 729) <= 10:
                logger.info(f"🎯 VERY CLOSE! Only {abs(total_count - 729)} difference from 729")
                logger.info("   These are likely the agents you're looking for")
            
            # Show sample records
            logger.info(f"\n📋 Sample corporate customers (showing first 20 of {total_count}):")
            logger.info("-" * 100)
            logger.info("ID     | Name                                     | Mobile          | Status | Unit | Employee | Date")
            logger.info("-" * 100)
            
            agent_like_count = 0
            mobile_money_count = 0
            service_count = 0
            
            for i, row in enumerate(corporate_customers[:20]):
                cust_id, name, cust_type, mobile, bus_ind, vip_ind, status, begin_date, unit, emp_id = row
                
                # Count agent-like names
                name_upper = name.upper()
                if any(keyword in name_upper for keyword in ['AGENT', 'WAKALA', 'DUKA', 'SHOP', 'STORE']):
                    agent_like_count += 1
                if any(keyword in name_upper for keyword in ['MOBILE', 'MONEY', 'MPESA', 'TIGO', 'AIRTEL']):
                    mobile_money_count += 1
                if any(keyword in name_upper for keyword in ['SERVICE', 'BUSINESS', 'COMPANY', 'LIMITED']):
                    service_count += 1
                
                logger.info(f"{cust_id:6} | {name:40} | {mobile:15} | {status:6} | {unit:4} | {emp_id:8} | {begin_date}")
            
            # Count patterns in all records
            logger.info(f"\n📊 Pattern Analysis of all {total_count} corporate customers:")
            
            total_agent_like = 0
            total_mobile_money = 0
            total_service = 0
            total_insurance = 0
            total_microfinance = 0
            
            for row in corporate_customers:
                cust_id, name, cust_type, mobile, bus_ind, vip_ind, status, begin_date, unit, emp_id = row
                name_upper = name.upper()
                
                if any(keyword in name_upper for keyword in ['AGENT', 'WAKALA', 'DUKA', 'SHOP', 'STORE']):
                    total_agent_like += 1
                if any(keyword in name_upper for keyword in ['MOBILE', 'MONEY', 'MPESA', 'TIGO', 'AIRTEL', 'HALO']):
                    total_mobile_money += 1
                if any(keyword in name_upper for keyword in ['SERVICE', 'BUSINESS', 'COMPANY', 'LIMITED']):
                    total_service += 1
                if any(keyword in name_upper for keyword in ['INSURANCE', 'ASSURANCE']):
                    total_insurance += 1
                if any(keyword in name_upper for keyword in ['MICROFINANCE', 'FINANCE', 'SACCO']):
                    total_microfinance += 1
            
            logger.info(f"  Agent-like names: {total_agent_like:3} ({total_agent_like/total_count*100:.1f}%)")
            logger.info(f"  Mobile money related: {total_mobile_money:3} ({total_mobile_money/total_count*100:.1f}%)")
            logger.info(f"  Service companies: {total_service:3} ({total_service/total_count*100:.1f}%)")
            logger.info(f"  Insurance companies: {total_insurance:3} ({total_insurance/total_count*100:.1f}%)")
            logger.info(f"  Financial services: {total_microfinance:3} ({total_microfinance/total_count*100:.1f}%)")
            
            # Check unit distribution
            logger.info(f"\n🏢 Unit Distribution:")
            unit_counts = {}
            for row in corporate_customers:
                unit = row[8]  # FKUNIT_BELONGS
                unit_counts[unit] = unit_counts.get(unit, 0) + 1
            
            for unit, count in sorted(unit_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
                logger.info(f"  Unit {unit}: {count:3} customers ({count/total_count*100:.1f}%)")
            
            # Check employee distribution
            logger.info(f"\n👥 Employee Distribution (Top 10):")
            emp_counts = {}
            for row in corporate_customers:
                emp_id = row[9]  # FK_BANKEMPLOYEEID
                if emp_id:
                    emp_counts[emp_id] = emp_counts.get(emp_id, 0) + 1
            
            for emp_id, count in sorted(emp_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
                logger.info(f"  Employee {emp_id}: {count:3} customers ({count/total_count*100:.1f}%)")
            
            # Show some specific agent-like corporate customers
            if total_agent_like > 0:
                logger.info(f"\n🎯 Agent-like Corporate Customers (showing first 10):")
                logger.info("-" * 80)
                
                agent_count = 0
                for row in corporate_customers:
                    if agent_count >= 10:
                        break
                    
                    cust_id, name, cust_type, mobile, bus_ind, vip_ind, status, begin_date, unit, emp_id = row
                    name_upper = name.upper()
                    
                    if any(keyword in name_upper for keyword in ['AGENT', 'WAKALA', 'DUKA', 'SHOP', 'STORE']):
                        logger.info(f"  {cust_id:6}: {name:50} | {mobile:15}")
                        agent_count += 1
            
            logger.info("\n" + "=" * 80)
            logger.info("💡 CONCLUSION:")
            if total_count == 729:
                logger.info("✅ FOUND THE 729 AGENTS!")
                logger.info("   These 729 corporate customers are the agents")
            elif abs(total_count - 729) <= 10:
                logger.info(f"✅ LIKELY FOUND THE AGENTS! ({total_count} vs 729)")
                logger.info("   These corporate customers are probably the agents")
                logger.info("   The small difference might be due to recent additions/removals")
            else:
                logger.info(f"❓ PARTIAL MATCH ({total_count} vs 729)")
                logger.info("   These might be related but not the exact 729 agents")
            
            logger.info(f"   Use CUST_TYPE = '2' with mobile numbers for agent identification")
            logger.info("=" * 80)
            
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_736_corporate_customers()