#!/usr/bin/env python3
"""
Test script for agents pipeline
"""

import sys
import os
import logging
from datetime import datetime

# Add the parent directory to the path so we can import our modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import Config
from db2_connection import DB2Connection
from processors.agent_processor import AgentProcessor
import psycopg2

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_agents_pipeline():
    """Test the complete agents pipeline"""
    config = Config()
    
    # Test DB2 connection and data extraction
    logger.info("Testing DB2 connection and agents data extraction...")
    
    try:
        # Connect to DB2
        db2_conn = DB2Connection()
        
        with db2_conn.get_connection() as connection:
            cursor = connection.cursor()
            
            # Get agents configuration
            agents_config = config.tables['agents']
            
            # Execute the query
            logger.info("Executing agents query...")
            cursor.execute(agents_config.query)
            
            # Fetch results
            results = cursor.fetchall()
            logger.info(f"Found {len(results)} agent records")
            
            if results:
                # Show first few records
                logger.info("Sample agent records:")
                for i, record in enumerate(results[:3]):
                    logger.info(f"Record {i+1}: Agent ID={record[2]}, Name={record[1]}, Status={record[12]}")
            
            # Test data processing
            logger.info("Testing agent data processing...")
            processor = AgentProcessor()
            
            processed_records = []
            for raw_record in results[:5]:  # Process first 5 records
                try:
                    agent_record = processor.process_record(raw_record, 'agents')
                    if processor.validate_record(agent_record):
                        processed_records.append(agent_record)
                        logger.info(f"Processed: {agent_record.agent_name} (ID: {agent_record.agent_id})")
                    else:
                        logger.warning(f"Invalid record: {raw_record[1]} (ID: {raw_record[2]})")
                except Exception as e:
                    logger.error(f"Error processing record {raw_record[2]}: {e}")
            
            logger.info(f"Successfully processed {len(processed_records)} agent records")
            
            # Test PostgreSQL insertion
            logger.info("Testing PostgreSQL insertion...")
            
            try:
                # Connect to PostgreSQL
                pg_conn = psycopg2.connect(
                    host=config.database.pg_host,
                    port=config.database.pg_port,
                    database=config.database.pg_database,
                    user=config.database.pg_user,
                    password=config.database.pg_password
                )
                pg_cursor = pg_conn.cursor()
                
                # Insert processed records
                for record in processed_records:
                    try:
                        processor.insert_to_postgres(record, pg_cursor)
                        logger.info(f"Inserted agent: {record.agent_name}")
                    except Exception as e:
                        logger.error(f"Error inserting agent {record.agent_name}: {e}")
                
                # Commit the transaction
                pg_conn.commit()
                
                # Verify insertion
                pg_cursor.execute('SELECT COUNT(*) FROM "agents"')
                count = pg_cursor.fetchone()[0]
                logger.info(f"Total agents in PostgreSQL: {count}")
                
                # Show some inserted records
                pg_cursor.execute('SELECT "agentName", "agentId", "agentStatus", "agentType" FROM "agents" ORDER BY "lastModified" DESC LIMIT 5')
                recent_agents = pg_cursor.fetchall()
                
                logger.info("Recent agents in PostgreSQL:")
                for agent in recent_agents:
                    logger.info(f"  - {agent[0]} (ID: {agent[1]}, Status: {agent[2]}, Type: {agent[3]})")
                
                pg_cursor.close()
                pg_conn.close()
                
            except Exception as e:
                logger.error(f"PostgreSQL error: {e}")
                return False
        
        logger.info("✅ Agents pipeline test completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"DB2 connection error: {e}")
        return False

def test_agents_sql_count():
    """Test how many agent records are available in DB2"""
    config = Config()
    
    try:
        # Connect to DB2
        db2_conn = DB2Connection()
        
        with db2_conn.get_connection() as connection:
            cursor = connection.cursor()
            
            # Count total agents
            count_query = """
            SELECT COUNT(*) as total_agents
            FROM AGENT a
            INNER JOIN CUSTOMER c ON a.FK_CUSTOMERCUST_ID = c.CUST_ID
            WHERE a.ENTRY_STATUS = '1' 
                AND c.ENTRY_STATUS = '1'
                AND COALESCE(a.UPDATE_TMSTAMP, a.INSERTION_TMSTAMP, c.LAST_UPDATE) >= TIMESTAMP('2016-01-01 00:00:00')
            """
            
            cursor.execute(count_query)
            result = cursor.fetchone()
            total_count = result[0] if result else 0
            
            logger.info(f"Total agent records available: {total_count}")
            
            # Count by status
            status_query = """
            SELECT 
                CASE 
                    WHEN a.ENTRY_STATUS = '1' AND c.ENTRY_STATUS = '1' THEN 'Active'
                    WHEN a.ENTRY_STATUS = '0' OR c.ENTRY_STATUS = '0' THEN 'Inactive'
                    ELSE 'Suspended'
                END AS status,
                COUNT(*) as count
            FROM AGENT a
            INNER JOIN CUSTOMER c ON a.FK_CUSTOMERCUST_ID = c.CUST_ID
            WHERE COALESCE(a.UPDATE_TMSTAMP, a.INSERTION_TMSTAMP, c.LAST_UPDATE) >= TIMESTAMP('2016-01-01 00:00:00')
            GROUP BY 
                CASE 
                    WHEN a.ENTRY_STATUS = '1' AND c.ENTRY_STATUS = '1' THEN 'Active'
                    WHEN a.ENTRY_STATUS = '0' OR c.ENTRY_STATUS = '0' THEN 'Inactive'
                    ELSE 'Suspended'
                END
            ORDER BY count DESC
            """
            
            cursor.execute(status_query)
            status_results = cursor.fetchall()
            
            logger.info("Agent records by status:")
            for status, count in status_results:
                logger.info(f"  - {status}: {count}")
        
        return total_count
        
    except Exception as e:
        logger.error(f"Error counting agents: {e}")
        return 0

if __name__ == "__main__":
    print("=" * 60)
    print("AGENTS PIPELINE TEST")
    print("=" * 60)
    
    # First, check how many records are available
    print("\n1. Checking agent data availability...")
    count = test_agents_sql_count()
    
    if count > 0:
        print(f"\n2. Testing pipeline with {count} available records...")
        success = test_agents_pipeline()
        
        if success:
            print("\n✅ All tests passed! Agents pipeline is ready.")
        else:
            print("\n❌ Some tests failed. Check the logs above.")
    else:
        print("\n❌ No agent data found. Check DB2 connection and data availability.")