#!/usr/bin/env python3
"""
Test Investment Debt Securities Query Syntax
"""

import pyodbc
import logging
from config import Config

def test_query_syntax():
    """Test the investment debt securities query syntax"""
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    config = Config()
    
    logger.info("🔍 Testing Investment Debt Securities Query Syntax")
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
        
        logger.info("📋 Testing query syntax with EXPLAIN...")
        
        # Test with EXPLAIN to check syntax without executing
        explain_query = f"EXPLAIN PLAN FOR {query}"
        
        try:
            cursor.execute(explain_query)
            logger.info("✅ Query syntax is valid!")
        except Exception as e:
            logger.error(f"❌ Syntax error: {e}")
            
            # Try to identify the specific issue
            if "CONCAT" in str(e):
                logger.info("💡 CONCAT function issue - DB2 uses || operator")
            elif "column" in str(e).lower():
                logger.info("💡 Column name issue detected")
            elif "table" in str(e).lower():
                logger.info("💡 Table name issue detected")
            
            return False
        
        # If syntax is OK, try a limited execution
        logger.info("📋 Testing limited execution (first 5 rows)...")
        
        # Add FETCH FIRST 5 ROWS ONLY to limit results
        limited_query = query.rstrip(';') + " FETCH FIRST 5 ROWS ONLY"
        
        try:
            cursor.execute(limited_query)
            results = cursor.fetchall()
            
            logger.info(f"✅ Limited query executed successfully!")
            logger.info(f"📊 Records returned: {len(results)}")
            
            if results:
                columns = [desc[0] for desc in cursor.description]
                logger.info(f"Columns: {', '.join(columns[:5])}...")
                
                for i, row in enumerate(results):
                    logger.info(f"Record {i+1}: {row[:3]}...")  # Show first 3 values
            else:
                logger.info("⚠️ No records returned from limited query")
                
        except Exception as e:
            logger.error(f"❌ Limited execution failed: {e}")
            return False
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"❌ Connection failed: {e}")
        return False

if __name__ == "__main__":
    test_query_syntax()