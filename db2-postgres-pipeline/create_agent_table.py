#!/usr/bin/env python3
"""
Create Agent Information Table - BOT Project
"""

import psycopg2
import logging
from config import Config

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_agent_table():
    """Create agent information table in PostgreSQL"""
    
    config = Config()
    
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        
        cursor = conn.cursor()
        
        # Drop existing table if it exists
        logger.info("🗑️ Dropping existing agents table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS "agents" CASCADE;')
        
        # Create agents table
        logger.info("🏗️ Creating agents table...")
        create_table_query = """
        CREATE TABLE "agents" (
            id SERIAL PRIMARY KEY,
            "reportingDate" VARCHAR(20),
            "agentName" VARCHAR(200) NOT NULL,
            "agentId" VARCHAR(50),
            "tillNumber" VARCHAR(50),
            "businessForm" VARCHAR(100),
            "agentPrincipal" VARCHAR(100),
            "agentPrincipalName" VARCHAR(200),
            "gender" VARCHAR(20),
            "registrationDate" VARCHAR(20),
            "closedDate" VARCHAR(20),
            "certIncorporation" VARCHAR(100),
            "nationality" VARCHAR(100),
            "agentStatus" VARCHAR(50),
            "agentType" VARCHAR(100),
            "accountNumber" VARCHAR(50),
            "region" VARCHAR(100),
            "district" VARCHAR(100),
            "ward" VARCHAR(100),
            "street" VARCHAR(200),
            "houseNumber" VARCHAR(50),
            "postalCode" VARCHAR(20),
            "country" VARCHAR(100),
            "gpsCoordinates" VARCHAR(100),
            "agentTaxIdentificationNumber" VARCHAR(50),
            "businessLicense" VARCHAR(200),
            "lastModified" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        cursor.execute(create_table_query)
        
        # Create indexes
        logger.info("📊 Creating indexes...")
        
        indexes = [
            'CREATE INDEX idx_agents_agent_id ON "agents"("agentId");',
            'CREATE INDEX idx_agents_agent_name ON "agents"("agentName");',
            'CREATE INDEX idx_agents_agent_status ON "agents"("agentStatus");',
            'CREATE INDEX idx_agents_region ON "agents"("region");',
            'CREATE INDEX idx_agents_district ON "agents"("district");',
            'CREATE INDEX idx_agents_last_modified ON "agents"("lastModified");'
        ]
        
        for index_query in indexes:
            cursor.execute(index_query)
        
        # Create trigger for updated_at
        trigger_query = """
        CREATE OR REPLACE FUNCTION update_updated_at_column()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ language 'plpgsql';
        
        CREATE TRIGGER update_agents_updated_at 
            BEFORE UPDATE ON "agents" 
            FOR EACH ROW 
            EXECUTE FUNCTION update_updated_at_column();
        """
        
        cursor.execute(trigger_query)
        
        # Commit changes
        conn.commit()
        
        logger.info("✅ Agent table and indexes created successfully!")
        
        # Show table structure
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'agents' 
            ORDER BY ordinal_position;
        """)
        
        columns = cursor.fetchall()
        logger.info("📋 Table structure:")
        for column_name, data_type in columns:
            logger.info(f"  {column_name}: {data_type}")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error creating agent table: {e}")
        return False

def main():
    """Main function"""
    print("🏗️ Creating Agent Information Table")
    print("=" * 50)
    
    success = create_agent_table()
    
    if success:
        print("✅ Agent table created successfully!")
    else:
        print("❌ Failed to create agent table!")

if __name__ == "__main__":
    main()