#!/usr/bin/env python3
"""
Test Optimized Investment Debt Securities Query
"""

import pyodbc
import logging
from config import Config

def test_optimized_query():
    """Test the optimized investment debt securities query"""
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    config = Config()
    
    logger.info("🔍 Testing Optimized Investment Debt Securities Query")
    logger.info("=" * 50)
    
    try:
        conn_str = (
            f"DRIVER={{IBM DB2 ODBC DRIVER}};"
            f"DATABASE={config.database.db2_database};"
            f"HOSTNAME={config.database.db2_host};"
            f"PORT={config.database.db2_port};"
            f"UID={config.database.db2_user};"
            f"PWD={config.database.db2_password};"
        )
        
        conn = pyodbc.connect(conn_str, timeout=30)
        cursor = conn.cursor()
        
        # Read the optimized SQL query from file
        with open('sqls/investment_debt_securities_optimized.sql', 'r') as f:
            query = f.read()
        
        logger.info("📋 Testing optimized query with FETCH FIRST 10 ROWS...")
        
        # Add FETCH FIRST to limit results for testing
        limited_query = query.rstrip(';').replace('ORDER BY \n    securityType, securityNumber;', 'ORDER BY securityType, securityNumber FETCH FIRST 10 ROWS ONLY')
        
        # Execute the query
        cursor.execute(limited_query)
        
        # Fetch results
        results = cursor.fetchall()
        
        logger.info(f"✅ Optimized query executed successfully!")
        logger.info(f"📊 Sample records returned: {len(results)}")
        
        if results:
            # Show column names
            columns = [desc[0] for desc in cursor.description]
            logger.info(f"\n📋 Columns ({len(columns)} total):")
            for i, col in enumerate(columns[:10]):  # Show first 10 columns
                logger.info(f"  {i+1:2d}. {col}")
            if len(columns) > 10:
                logger.info(f"  ... and {len(columns) - 10} more columns")
            
            # Show sample records
            logger.info(f"\n📋 Sample records:")
            for i, row in enumerate(results[:5]):
                sec_num = row[1]  # securityNumber
                sec_type = row[2]  # securityType
                issuer = row[3]  # securityIssuerName
                amount = row[9]  # orgCostValueAmount
                logger.info(f"  {i+1}. {sec_num} | {sec_type} | {issuer} | {amount:,.2f}")
            
            # Show summary by security type
            logger.info(f"\n📊 Summary by Security Type (from sample):")
            security_types = {}
            for row in results:
                sec_type = row[2]  # securityType
                if sec_type in security_types:
                    security_types[sec_type] += 1
                else:
                    security_types[sec_type] = 1
            
            for sec_type, count in security_types.items():
                logger.info(f"  {sec_type}: {count} records")
        else:
            logger.info("⚠️ No records returned from optimized query")
        
        # Now test the full query count (without FETCH FIRST)
        logger.info(f"\n📋 Testing full query count...")
        
        count_query = f"""
        SELECT COUNT(*) FROM (
            {query.rstrip(';')}
        ) AS full_result
        """
        
        try:
            cursor.execute(count_query)
            total_count = cursor.fetchone()[0]
            logger.info(f"📊 Total records in full query: {total_count:,}")
        except Exception as e:
            logger.info(f"⚠️ Could not get full count: {e}")
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"❌ Optimized query execution failed: {e}")
        return False

if __name__ == "__main__":
    test_optimized_query()