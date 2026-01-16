#!/usr/bin/env python3
"""
Reload Agents Pipeline - Delete and reload agents with new query
"""

import psycopg2
import logging
from config import Config
from db2_connection import DB2Connection
from processors.agent_processor import AgentProcessor

def reload_agents():
    """Delete all agents and reload with new query"""
    config = Config()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 60)
    logger.info("👥 AGENTS RELOAD PIPELINE")
    logger.info("=" * 60)
    
    try:
        # Initialize connections
        db2_conn = DB2Connection()
        processor = AgentProcessor()
        
        # Get table configuration
        table_config = config.tables['agents']
        
        # Connect to PostgreSQL
        pg_conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        pg_cursor = pg_conn.cursor()
        logger.info("✅ Connected to PostgreSQL")
        
        # Step 1: Delete all existing agents
        logger.info("🗑️  Deleting all existing agents...")
        pg_cursor.execute('SELECT COUNT(*) FROM "agents"')
        old_count = pg_cursor.fetchone()[0]
        logger.info(f"   Found {old_count} existing agent records")
        
        pg_cursor.execute('DELETE FROM "agents"')
        pg_conn.commit()
        logger.info(f"✅ Deleted {old_count} agent records")
        
        # Step 2: Load agents with new query
        logger.info("🔍 Fetching data from DB2 with new query...")
        
        processed_count = 0
        skipped_count = 0
        error_count = 0
        
        with db2_conn.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(table_config.query)
            
            logger.info("📊 Processing records...")
            
            while True:
                row = cursor.fetchone()
                if not row:
                    break
                
                try:
                    # Process the record
                    record = processor.process_record(row, table_config.name)
                    
                    # Validate record
                    if processor.validate_record(record):
                        # Insert directly to PostgreSQL
                        processor.insert_to_postgres(record, pg_cursor)
                        processed_count += 1
                        
                        if processed_count % 50 == 0:
                            logger.info(f"✅ Processed {processed_count} agent records")
                            pg_conn.commit()  # Commit every 50 records
                    else:
                        skipped_count += 1
                        logger.warning(f"⚠️ Invalid record skipped: {record.agentId if hasattr(record, 'agentId') else 'Unknown'}")
                        
                except Exception as e:
                    error_count += 1
                    logger.error(f"❌ Error processing record: {e}")
                    # Continue processing other records instead of stopping
                    continue
        
        # Final commit
        pg_conn.commit()
        
        # Check final count
        pg_cursor.execute('SELECT COUNT(*) FROM "agents"')
        total_count = pg_cursor.fetchone()[0]
        
        logger.info("=" * 60)
        logger.info(f"📊 PIPELINE SUMMARY:")
        logger.info(f"   - Old records deleted: {old_count}")
        logger.info(f"   - Successfully processed: {processed_count}")
        logger.info(f"   - Skipped (validation): {skipped_count}")
        logger.info(f"   - Errors: {error_count}")
        logger.info(f"   - Total in PostgreSQL: {total_count}")
        
        # Show sample records
        pg_cursor.execute("""
            SELECT "agentName", "agentId", "businessForm", "region", "district"
            FROM "agents" 
            ORDER BY "registrationDate" DESC 
            LIMIT 5
        """)
        
        sample_records = pg_cursor.fetchall()
        logger.info("📋 Sample records:")
        for record in sample_records:
            agent_name, agent_id, business_form, region, district = record
            logger.info(f"  - {agent_name[:30]:<30} ({agent_id}) {business_form} - {region}, {district}")
        
        # Show statistics
        pg_cursor.execute("""
            SELECT 
                "businessForm", 
                COUNT(*) as count
            FROM "agents" 
            GROUP BY "businessForm" 
            ORDER BY COUNT(*) DESC
        """)
        
        business_forms = pg_cursor.fetchall()
        logger.info("📊 Business Forms Distribution:")
        for form, count in business_forms:
            logger.info(f"  - {form}: {count} agents")
        
        # Show region distribution
        pg_cursor.execute("""
            SELECT 
                "region", 
                COUNT(*) as count
            FROM "agents" 
            GROUP BY "region" 
            ORDER BY COUNT(*) DESC
            LIMIT 10
        """)
        
        regions = pg_cursor.fetchall()
        logger.info("📊 Top 10 Regions:")
        for region, count in regions:
            logger.info(f"  - {region}: {count} agents")
        
        # Close connections
        pg_cursor.close()
        pg_conn.close()
        
        logger.info("=" * 60)
        logger.info("✅ Agents reload pipeline completed successfully!")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"❌ Error in agents reload pipeline: {e}")
        raise

if __name__ == "__main__":
    reload_agents()
