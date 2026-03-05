#!/usr/bin/env python3
"""
Create agents table in PostgreSQL
Based on agents-from-agents-list-NEW-V5.table.sql structure
"""

import psycopg2
import logging
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config

def create_agents_table():
    """Create the agents table in PostgreSQL"""
    
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
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
        
        logger.info("Dropping existing agents table if it exists...")
        cursor.execute('DROP TABLE IF EXISTS agents CASCADE')
        
        logger.info("Creating agents table...")
        create_table_sql = """
        CREATE TABLE agents (
            id SERIAL PRIMARY KEY,
            "reportingDate" VARCHAR(12),
            "agentName" VARCHAR(255),
            "agentId" VARCHAR(50),
            "tillNumber" VARCHAR(50),
            "businessForm" VARCHAR(100),
            "agentPrincipal" VARCHAR(100),
            "agentPrincipalName" VARCHAR(255),
            gender VARCHAR(20),
            "registrationDate" VARCHAR(50),
            "closedDate" VARCHAR(50),
            "certIncorporation" VARCHAR(100),
            nationality VARCHAR(100),
            "agentStatus" VARCHAR(50),
            "agentType" VARCHAR(100),
            "accountNumber" VARCHAR(50),
            region VARCHAR(100),
            district VARCHAR(100),
            ward VARCHAR(100),
            street VARCHAR(255),
            "houseNumber" VARCHAR(50),
            "postalCode" VARCHAR(20),
            country VARCHAR(100),
            "gpsCoordinates" VARCHAR(100),
            "agentTaxIdentificationNumber" VARCHAR(50),
            "businessLicense" VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_sql)
        
        logger.info("Creating indexes...")
        
        indexes = [
            'CREATE INDEX idx_agents_reporting_date ON agents("reportingDate")',
            'CREATE UNIQUE INDEX idx_agents_id ON agents("agentId")',
            'CREATE INDEX idx_agents_name ON agents("agentName")',
            'CREATE INDEX idx_agents_status ON agents("agentStatus")',
            'CREATE INDEX idx_agents_type ON agents("agentType")',
            'CREATE INDEX idx_agents_region ON agents(region)',
            'CREATE INDEX idx_agents_district ON agents(district)',
            'CREATE INDEX idx_agents_created_at ON agents(created_at)'
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
            index_name = index_sql.split('INDEX')[1].split('ON')[0].strip()
            logger.info(f"Created index: {index_name}")
        
        conn.commit()
        
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'agents'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        
        logger.info("Agents table created successfully!")
        logger.info("Table structure:")
        logger.info("-" * 80)
        logger.info(f"{'Column Name':<35} {'Data Type':<20} {'Max Length':<12} {'Nullable':<10}")
        logger.info("-" * 80)
        
        for col in columns:
            col_name, data_type, max_length, nullable = col
            max_len_str = str(max_length) if max_length else 'N/A'
            logger.info(f"{col_name:<35} {data_type:<20} {max_len_str:<12} {nullable:<10}")
        
        logger.info("-" * 80)
        logger.info(f"Total columns: {len(columns)}")
        
        cursor.close()
        conn.close()
        
        logger.info("Agents table setup completed successfully!")
        
    except Exception as e:
        logger.error(f"Error creating agents table: {e}")
        raise

if __name__ == "__main__":
    create_agents_table()
