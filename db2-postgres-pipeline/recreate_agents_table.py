#!/usr/bin/env python3
"""
Drop and recreate agents table based on query fields
"""

import psycopg2
import logging
from config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def recreate_agents_table():
    """Drop and recreate agents table"""
    config = Config()
    
    try:
        conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        
        cursor = conn.cursor()
        
        # Check current count
        try:
            cursor.execute('SELECT COUNT(*) FROM "agents"')
            current_count = cursor.fetchone()[0]
            logger.info(f"Current records in agents table: {current_count:,}")
        except:
            logger.info("Agents table does not exist or is inaccessible")
        
        logger.info("Dropping agents table...")
        cursor.execute('DROP TABLE IF EXISTS "agents" CASCADE')
        logger.info("✓ Table dropped")
        
        logger.info("Creating agents table with all fields from query...")
        
        # Create table based on query fields
        # Note: registrationDate and closedDate are VARCHAR because DB2 data is not in standard date format
        # agentTaxIdentificationNumber and businessLicense are NOT NULL as per query requirements
        create_table_sql = """
        CREATE TABLE "agents" (
            id SERIAL PRIMARY KEY,
            "reportingDate" VARCHAR(50),
            "agentName" VARCHAR(255),
            "terminalID" VARCHAR(50),
            "agentId" VARCHAR(50) UNIQUE NOT NULL,
            "tillNumber" VARCHAR(50),
            "businessForm" VARCHAR(100),
            "agentPrincipal" VARCHAR(50),
            "agentPrincipalName" VARCHAR(255),
            gender VARCHAR(20),
            "registrationDate" VARCHAR(50),
            "closedDate" VARCHAR(50),
            "certIncorporation" VARCHAR(100),
            nationality VARCHAR(100),
            "agentStatus" VARCHAR(50),
            "agentType" VARCHAR(50),
            "accountNumber" VARCHAR(50),
            region VARCHAR(100),
            district VARCHAR(100),
            ward VARCHAR(100),
            street VARCHAR(255),
            "houseNumber" VARCHAR(50),
            "postalCode" VARCHAR(20),
            country VARCHAR(100),
            "gpsCoordinates" VARCHAR(100),
            "agentTaxIdentificationNumber" VARCHAR(50) NOT NULL,
            "businessLicense" VARCHAR(100) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_sql)
        logger.info("✓ Table created")
        
        # Create indexes
        logger.info("Creating indexes...")
        
        cursor.execute('CREATE INDEX idx_agents_agent_id ON "agents"("agentId")')
        cursor.execute('CREATE INDEX idx_agents_terminal_id ON "agents"("terminalID")')
        cursor.execute('CREATE INDEX idx_agents_agent_name ON "agents"("agentName")')
        cursor.execute('CREATE INDEX idx_agents_region ON "agents"(region)')
        cursor.execute('CREATE INDEX idx_agents_district ON "agents"(district)')
        
        logger.info("✓ Indexes created")
        
        # Create trigger for updated_at
        logger.info("Creating trigger for updated_at...")
        
        cursor.execute("""
            CREATE TRIGGER update_agents_updated_at
            BEFORE UPDATE ON "agents"
            FOR EACH ROW
            EXECUTE FUNCTION update_updated_at_column();
        """)
        
        logger.info("✓ Trigger created")
        
        conn.commit()
        
        # Show table structure
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'agents'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        logger.info("\n" + "=" * 80)
        logger.info("AGENTS TABLE STRUCTURE")
        logger.info("=" * 80)
        for col_name, data_type, max_length, nullable in columns:
            length_info = f"({max_length})" if max_length else ""
            null_info = "NULL" if nullable == "YES" else "NOT NULL"
            logger.info(f"  {col_name:40} {data_type}{length_info:20} {null_info}")
        logger.info("=" * 80)
        
        # Show indexes
        cursor.execute("""
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE tablename = 'agents'
        """)
        
        indexes = cursor.fetchall()
        logger.info("\nINDEXES:")
        for idx_name, idx_def in indexes:
            logger.info(f"  - {idx_name}")
        
        # Show triggers
        cursor.execute("""
            SELECT trigger_name, event_manipulation, action_timing
            FROM information_schema.triggers
            WHERE event_object_table = 'agents'
        """)
        
        triggers = cursor.fetchall()
        logger.info("\nTRIGGERS:")
        for trigger_name, event, timing in triggers:
            logger.info(f"  - {trigger_name}: {timing} {event}")
        
        cursor.close()
        conn.close()
        
        logger.info("\n✓ Agents table recreated successfully")
        logger.info("  All fields from query are included")
        logger.info("  Ready for pipeline execution")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        raise

if __name__ == "__main__":
    logger.info("=" * 80)
    logger.info("RECREATING AGENTS TABLE")
    logger.info("=" * 80)
    recreate_agents_table()
    logger.info("=" * 80)
    logger.info("DONE")
    logger.info("=" * 80)
