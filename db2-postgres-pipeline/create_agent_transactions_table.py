#!/usr/bin/env python3
"""
Create agentTransactions table with camelCase naming
"""

import psycopg2
from config import Config

def create_agent_transactions_table():
    """Create the agentTransactions table with camelCase fields"""
    
    config = Config()
    
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host=config.database.pg_host,
        port=config.database.pg_port,
        database=config.database.pg_database,
        user=config.database.pg_user,
        password=config.database.pg_password
    )
    
    cursor = conn.cursor()
    
    # Drop table if exists
    drop_query = 'DROP TABLE IF EXISTS "agentTransactions"'
    cursor.execute(drop_query)
    print("✅ Dropped existing agentTransactions table (if it existed)")
    
    # Create table with camelCase naming
    create_query = """
    CREATE TABLE "agentTransactions" (
        "reportingDate" TIMESTAMP NOT NULL,
        "agentId" VARCHAR(50) NOT NULL,
        "agentStatus" VARCHAR(20) NOT NULL,
        "transactionDate" DATE NOT NULL,
        "transactionId" VARCHAR(200) NOT NULL PRIMARY KEY,
        "transactionType" VARCHAR(50) NOT NULL,
        "serviceChannel" VARCHAR(50) NOT NULL,
        "tillNumber" VARCHAR(50),
        "currency" VARCHAR(10) NOT NULL,
        "tzsAmount" DECIMAL(18,2) NOT NULL,
        "createdAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        "updatedAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    
    cursor.execute(create_query)
    print("✅ Created agentTransactions table with camelCase fields")
    
    # Create indexes for better performance
    indexes = [
        'CREATE INDEX "idx_agentTransactions_agentId" ON "agentTransactions" ("agentId")',
        'CREATE INDEX "idx_agentTransactions_transactionDate" ON "agentTransactions" ("transactionDate")',
        'CREATE INDEX "idx_agentTransactions_transactionType" ON "agentTransactions" ("transactionType")',
        'CREATE INDEX "idx_agentTransactions_reportingDate" ON "agentTransactions" ("reportingDate")'
    ]
    
    for index_query in indexes:
        cursor.execute(index_query)
        print(f"✅ Created index: {index_query.split('CREATE INDEX ')[1].split(' ON')[0]}")
    
    # Create trigger for updatedAt
    trigger_query = """
    CREATE OR REPLACE FUNCTION update_updated_at_column()
    RETURNS TRIGGER AS $$
    BEGIN
        NEW."updatedAt" = CURRENT_TIMESTAMP;
        RETURN NEW;
    END;
    $$ language 'plpgsql';
    
    CREATE TRIGGER update_agentTransactions_updated_at 
        BEFORE UPDATE ON "agentTransactions" 
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    """
    
    cursor.execute(trigger_query)
    print("✅ Created updatedAt trigger")
    
    # Commit changes
    conn.commit()
    cursor.close()
    conn.close()
    
    print("\n🎉 agentTransactions table created successfully with camelCase naming!")
    print("📋 Table structure:")
    print("   - reportingDate (TIMESTAMP)")
    print("   - agentId (VARCHAR(50))")
    print("   - agentStatus (VARCHAR(20))")
    print("   - transactionDate (DATE)")
    print("   - transactionId (VARCHAR(200)) - PRIMARY KEY")
    print("   - transactionType (VARCHAR(50))")
    print("   - serviceChannel (VARCHAR(50))")
    print("   - tillNumber (VARCHAR(50)) - NULLABLE")
    print("   - currency (VARCHAR(10))")
    print("   - tzsAmount (DECIMAL(18,2))")
    print("   - createdAt (TIMESTAMP)")
    print("   - updatedAt (TIMESTAMP)")

if __name__ == "__main__":
    create_agent_transactions_table()