#!/usr/bin/env python3
"""
Test Branch SQL Query - Count Records from DB2
"""

from db2_connection import DB2Connection
import logging

def test_branch_sql_count():
    """Test how many records the branch SQL returns from DB2"""
    db2_conn = DB2Connection()
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    # Read the branch SQL query
    with open('../sqls/branch1.sql', 'r') as f:
        branch_query = f.read()
    
    try:
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            
            logger.info("🔍 Executing branch SQL query from branch1.sql...")
            logger.info(f"📝 Query preview: {branch_query[:200]}...")
            
            cursor.execute(branch_query)
            
            rows = cursor.fetchall()
            logger.info(f"📊 Total records returned: {len(rows)}")
            
            if len(rows) > 0:
                logger.info("📋 Sample records:")
                for i, row in enumerate(rows[:5]):  # Show first 5 records
                    logger.info(f"  Record {i+1}:")
                    logger.info(f"    Branch Code: {row[4]}")
                    logger.info(f"    Branch Name: {row[1]}")
                    logger.info(f"    Region: {row[6]}")
                    logger.info(f"    District: {row[7]}")
                    logger.info(f"    Status: {row[16]}")
                    logger.info("")
            else:
                logger.warning("⚠️ No records found!")
                
            # Also test without the WHERE clause to see total branches
            logger.info("🔍 Testing query without WHERE clause to see all branches...")
            
            # Remove the WHERE clause for total count
            query_no_where = branch_query.split('WHERE')[0] + " ORDER BY u.CODE"
            cursor.execute(query_no_where)
            
            all_rows = cursor.fetchall()
            logger.info(f"📊 Total branches in UNIT table: {len(all_rows)}")
            
            if len(all_rows) > 0:
                logger.info("📋 All branch codes and names:")
                for i, row in enumerate(all_rows):
                    logger.info(f"  {i+1}. Code: {row[4]}, Name: {row[1]}, Status: {row[16]}")
                    
    except Exception as e:
        logger.error(f"❌ Error executing query: {e}")

if __name__ == "__main__":
    test_branch_sql_count()