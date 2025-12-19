#!/usr/bin/env python3
"""
Deep investigation into all database tables to find agent-related data
"""

import sys
import os
import logging

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db2_connection import DB2Connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def deep_agent_investigation():
    """Deep investigation into all tables for agent-related data"""
    
    db2_conn = DB2Connection()
    
    try:
        with db2_conn.get_connection() as connection:
            cursor = connection.cursor()
            
            logger.info("=" * 80)
            logger.info("DEEP AGENT INVESTIGATION - ALL TABLES")
            logger.info("=" * 80)
            
            # 1. Get all tables in PROFITS schema
            logger.info("\n🔍 Getting all tables in PROFITS schema...")
            query_tables = """
                SELECT TABNAME, CARD, REMARKS
                FROM SYSCAT.TABLES
                WHERE TABSCHEMA = 'PROFITS'
                    AND TYPE = 'T'
                ORDER BY TABNAME
            """
            cursor.execute(query_tables)
            all_tables = cursor.fetchall()
            
            logger.info(f"Found {len(all_tables)} tables in PROFITS schema")
            
            # 2. Search for tables with agent-related names
            logger.info("\n🎯 Tables with agent-related names:")
            agent_related_tables = []
            for table_name, card, remarks in all_tables:
                if any(keyword in table_name.upper() for keyword in ['AGENT', 'MOBILE', 'CHANNEL', 'TERMINAL', 'WAKALA', 'DUKA']):
                    agent_related_tables.append((table_name, card, remarks))
                    logger.info(f"  {table_name:<30} | Records: {card:<10} | {remarks or 'No description'}")
            
            # 3. Search for tables with customer-related names that might contain agents
            logger.info("\n👥 Customer-related tables:")
            customer_tables = []
            for table_name, card, remarks in all_tables:
                if any(keyword in table_name.upper() for keyword in ['CUSTOMER', 'CLIENT', 'CUST', 'USER', 'USR']):
                    customer_tables.append((table_name, card, remarks))
                    logger.info(f"  {table_name:<30} | Records: {card:<10} | {remarks or 'No description'}")
            
            # 4. Search for tables with transaction-related names
            logger.info("\n💳 Transaction-related tables:")
            transaction_tables = []
            for table_name, card, remarks in all_tables:
                if any(keyword in table_name.upper() for keyword in ['TRX', 'TRANSACTION', 'TRANS', 'TXN', 'PAYMENT', 'TRANSFER']):
                    transaction_tables.append((table_name, card, remarks))
                    logger.info(f"  {table_name:<30} | Records: {card:<10} | {remarks or 'No description'}")
            
            # 5. Examine each agent-related table in detail
            logger.info("\n" + "=" * 80)
            logger.info("DETAILED EXAMINATION OF AGENT-RELATED TABLES")
            logger.info("=" * 80)
            
            for table_name, card, remarks in agent_related_tables:
                logger.info(f"\n📋 Examining table: {table_name}")
                logger.info(f"   Records: {card}, Description: {remarks or 'No description'}")
                
                try:
                    # Get table structure
                    query_structure = f"""
                        SELECT COLNAME, TYPENAME, LENGTH, NULLS
                        FROM SYSCAT.COLUMNS
                        WHERE TABSCHEMA = 'PROFITS'
                            AND TABNAME = '{table_name}'
                        ORDER BY COLNO
                    """
                    cursor.execute(query_structure)
                    columns = cursor.fetchall()
                    
                    logger.info("   Columns:")
                    for col_name, type_name, length, nulls in columns:
                        logger.info(f"     {col_name:<25} | {type_name:<12} | {length:<6} | {nulls}")
                    
                    # Sample data if table has records
                    if card and card > 0:
                        query_sample = f"""
                            SELECT *
                            FROM {table_name}
                            FETCH FIRST 5 ROWS ONLY
                        """
                        cursor.execute(query_sample)
                        sample_data = cursor.fetchall()
                        
                        if sample_data:
                            logger.info("   Sample data (first 5 rows):")
                            for i, row in enumerate(sample_data, 1):
                                logger.info(f"     Row {i}: {str(row)[:200]}...")
                        else:
                            logger.info("   No sample data available")
                    else:
                        logger.info("   Table is empty")
                        
                except Exception as e:
                    logger.error(f"   Error examining {table_name}: {e}")
            
            # 6. Look for tables that might reference our known agents
            logger.info("\n" + "=" * 80)
            logger.info("SEARCHING FOR TABLES REFERENCING KNOWN AGENTS")
            logger.info("=" * 80)
            
            # Use some of our known agent customer IDs
            known_agent_ids = [38988, 38971, 45117, 39572, 37538, 42488, 56431, 32799, 38208]
            agent_ids_str = ','.join(map(str, known_agent_ids))
            
            # Check which tables have CUST_ID columns that reference our agents
            for table_name, card, remarks in all_tables:
                if card and card > 0:  # Only check tables with data
                    try:
                        # Check if table has CUST_ID column
                        query_check_custid = f"""
                            SELECT COLNAME
                            FROM SYSCAT.COLUMNS
                            WHERE TABSCHEMA = 'PROFITS'
                                AND TABNAME = '{table_name}'
                                AND COLNAME LIKE '%CUST%'
                        """
                        cursor.execute(query_check_custid)
                        cust_columns = cursor.fetchall()
                        
                        if cust_columns:
                            # Check if any of our known agents are in this table
                            for col_name, in cust_columns:
                                try:
                                    query_agent_check = f"""
                                        SELECT COUNT(*) as agent_count
                                        FROM {table_name}
                                        WHERE {col_name} IN ({agent_ids_str})
                                    """
                                    cursor.execute(query_agent_check)
                                    agent_count = cursor.fetchone()[0]
                                    
                                    if agent_count > 0:
                                        logger.info(f"🎯 {table_name} contains {agent_count} of our known agents (column: {col_name})")
                                        
                                        # Get sample data for these agents
                                        query_agent_sample = f"""
                                            SELECT *
                                            FROM {table_name}
                                            WHERE {col_name} IN ({agent_ids_str})
                                            FETCH FIRST 3 ROWS ONLY
                                        """
                                        cursor.execute(query_agent_sample)
                                        agent_sample = cursor.fetchall()
                                        
                                        for i, row in enumerate(agent_sample, 1):
                                            logger.info(f"     Agent Row {i}: {str(row)[:150]}...")
                                            
                                except Exception as e:
                                    logger.debug(f"     Error checking {col_name} in {table_name}: {e}")
                                    
                    except Exception as e:
                        logger.debug(f"   Error checking table {table_name}: {e}")
            
            # 7. Look for tables with mobile/phone numbers that might match our agents
            logger.info("\n" + "=" * 80)
            logger.info("SEARCHING FOR TABLES WITH MOBILE/PHONE NUMBERS")
            logger.info("=" * 80)
            
            # Get mobile numbers from our known agents
            query_agent_mobiles = f"""
                SELECT CUST_ID, MOBILE_TEL
                FROM CUSTOMER
                WHERE CUST_ID IN ({agent_ids_str})
                    AND MOBILE_TEL IS NOT NULL
                    AND TRIM(MOBILE_TEL) != ''
            """
            cursor.execute(query_agent_mobiles)
            agent_mobiles = cursor.fetchall()
            
            if agent_mobiles:
                mobile_numbers = [mobile for _, mobile in agent_mobiles if mobile]
                logger.info(f"Found {len(mobile_numbers)} mobile numbers from known agents")
                
                # Search for these mobile numbers in other tables
                for table_name, card, remarks in all_tables:
                    if card and card > 0:  # Only check tables with data
                        try:
                            # Check if table has mobile/phone columns
                            query_check_mobile = f"""
                                SELECT COLNAME
                                FROM SYSCAT.COLUMNS
                                WHERE TABSCHEMA = 'PROFITS'
                                    AND TABNAME = '{table_name}'
                                    AND (COLNAME LIKE '%MOBILE%' 
                                         OR COLNAME LIKE '%PHONE%' 
                                         OR COLNAME LIKE '%TEL%'
                                         OR COLNAME LIKE '%CONTACT%')
                            """
                            cursor.execute(query_check_mobile)
                            mobile_columns = cursor.fetchall()
                            
                            if mobile_columns:
                                for col_name, in mobile_columns:
                                    try:
                                        # Check first few mobile numbers
                                        for cust_id, mobile in agent_mobiles[:3]:
                                            if mobile and len(mobile.strip()) > 5:
                                                query_mobile_check = f"""
                                                    SELECT COUNT(*) as mobile_count
                                                    FROM {table_name}
                                                    WHERE {col_name} LIKE '%{mobile.strip()[-6:]}%'
                                                """
                                                cursor.execute(query_mobile_check)
                                                mobile_count = cursor.fetchone()[0]
                                                
                                                if mobile_count > 0:
                                                    logger.info(f"📱 {table_name}.{col_name} contains mobile number ending in {mobile.strip()[-6:]} ({mobile_count} matches)")
                                                    break
                                                    
                                    except Exception as e:
                                        logger.debug(f"     Error checking mobile in {table_name}.{col_name}: {e}")
                                        
                        except Exception as e:
                            logger.debug(f"   Error checking mobile columns in {table_name}: {e}")
            
            # 8. Summary
            logger.info("\n" + "=" * 80)
            logger.info("INVESTIGATION SUMMARY")
            logger.info("=" * 80)
            logger.info(f"Total tables examined: {len(all_tables)}")
            logger.info(f"Agent-related tables found: {len(agent_related_tables)}")
            logger.info(f"Customer-related tables found: {len(customer_tables)}")
            logger.info(f"Transaction-related tables found: {len(transaction_tables)}")
            logger.info("\nRecommendations:")
            logger.info("1. Focus on tables that contain our known agent customer IDs")
            logger.info("2. Examine transaction tables for agent activity patterns")
            logger.info("3. Look for terminal/channel tables that might link to agents")
            logger.info("4. Consider combining data from multiple tables for complete agent profiles")
            logger.info("=" * 80)
            
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    deep_agent_investigation()