#!/usr/bin/env python3
"""
Test Investment Debt Securities Query
"""

import pyodbc
import logging
from config import Config

def test_investment_securities_query():
    """Test the investment debt securities query"""
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    config = Config()
    
    logger.info("🔍 Testing Investment Debt Securities Query")
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
        
        # Read the SQL query from file
        with open('sqls/investment_debt_securities.sql', 'r') as f:
            query = f.read()
        
        logger.info("📋 Executing investment debt securities query...")
        
        # Execute the query
        cursor.execute(query)
        
        # Fetch results
        results = cursor.fetchall()
        
        logger.info(f"✅ Query executed successfully!")
        logger.info(f"📊 Total records returned: {len(results)}")
        
        if results:
            # Show first few records
            logger.info("\n📋 Sample records (first 3):")
            columns = [desc[0] for desc in cursor.description]
            logger.info(f"Columns: {', '.join(columns[:5])}...")  # Show first 5 columns
            
            for i, row in enumerate(results[:3]):
                logger.info(f"Record {i+1}: {row[:5]}...")  # Show first 5 values
            
            # Show summary by security type
            logger.info("\n📊 Summary by Security Type:")
            security_types = {}
            for row in results:
                sec_type = row[2]  # securityType is 3rd column
                if sec_type in security_types:
                    security_types[sec_type] += 1
                else:
                    security_types[sec_type] = 1
            
            for sec_type, count in security_types.items():
                logger.info(f"  {sec_type}: {count:>10,} records")
        else:
            logger.info("⚠️ No records returned from query")
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"❌ Query execution failed: {e}")
        return False

if __name__ == "__main__":
    test_investment_securities_query()