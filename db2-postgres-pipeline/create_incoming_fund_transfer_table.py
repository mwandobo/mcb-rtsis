#!/usr/bin/env python3
"""
Create PostgreSQL table for incoming fund transfer records
"""

import psycopg2
from config import Config

def create_incoming_fund_transfer_table():
    """Create the incomingFundTransfer table in PostgreSQL with camelCase fields"""
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
        
        # Drop table if exists
        cursor.execute('DROP TABLE IF EXISTS "incomingFundTransfer" CASCADE')
        
        # Create incoming fund transfer table with all 21 fields using camelCase (quoted identifiers)
        create_table_query = """
        CREATE TABLE "incomingFundTransfer" (
            "id" SERIAL PRIMARY KEY,
            "reportingDate" TIMESTAMP NOT NULL,
            "transactionId" VARCHAR(255) NOT NULL,
            "transactionDate" DATE,
            "transferChannel" VARCHAR(50) DEFAULT 'EFT',
            "subCategoryTransferChannel" VARCHAR(100),
            "recipientName" VARCHAR(255),
            "senderAccountNumber" VARCHAR(50),
            "recipientIdentificationType" VARCHAR(50),
            "recipientIdentificationNumber" VARCHAR(50),
            "recipientCountry" VARCHAR(100) DEFAULT 'TANZANIA, UNITED REPUBLIC OF',
            "senderName" VARCHAR(255),
            "senderBankOrFspCode" VARCHAR(50),
            "senderAccountOrWalletNumber" VARCHAR(50),
            "serviceCategory" VARCHAR(100),
            "serviceSubCategory" VARCHAR(100),
            "currency" VARCHAR(10),
            "orgAmount" DECIMAL(18,2),
            "usdAmount" DECIMAL(18,2),
            "tzsAmount" DECIMAL(18,0),
            "purposes" TEXT,
            "senderInstruction" VARCHAR(255),
            "createdAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_query)
        
        # Create indexes for better performance using camelCase (quoted identifiers)
        cursor.execute('CREATE INDEX "idxIncomingFundTransferReportingDate" ON "incomingFundTransfer"("reportingDate")')
        cursor.execute('CREATE INDEX "idxIncomingFundTransferTransactionId" ON "incomingFundTransfer"("transactionId")')
        cursor.execute('CREATE INDEX "idxIncomingFundTransferTransactionDate" ON "incomingFundTransfer"("transactionDate")')
        cursor.execute('CREATE INDEX "idxIncomingFundTransferSenderAccount" ON "incomingFundTransfer"("senderAccountNumber")')
        cursor.execute('CREATE INDEX "idxIncomingFundTransferTransferChannel" ON "incomingFundTransfer"("transferChannel")')
        cursor.execute('CREATE INDEX "idxIncomingFundTransferCurrency" ON "incomingFundTransfer"("currency")')
        cursor.execute('CREATE INDEX "idxIncomingFundTransferRecipientName" ON "incomingFundTransfer"("recipientName")')
        cursor.execute('CREATE INDEX "idxIncomingFundTransferCreatedAt" ON "incomingFundTransfer"("createdAt")')
        
        conn.commit()
        print("Incoming Fund Transfer table created successfully with indexes")
        
        # Show table info
        cursor.execute("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns 
            WHERE table_name = 'incomingFundTransfer' 
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        print(f"\nTable structure ({len(columns)} columns):")
        for col in columns:
            nullable = 'NULL' if col[2] == 'YES' else 'NOT NULL'
            default = f" DEFAULT {col[3]}" if col[3] else ""
            print(f"  {col[0]}: {col[1]} ({nullable}){default}")
        
        conn.close()
        
    except Exception as e:
        print(f"Error creating incoming fund transfer table: {e}")
        raise

if __name__ == "__main__":
    create_incoming_fund_transfer_table()