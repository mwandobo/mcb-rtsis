#!/usr/bin/env python3
"""
Create PostgreSQL table for outgoing fund transfer records
"""

import psycopg2
from config import Config

def create_outgoing_fund_transfer_table():
    """Create the outgoingFundTransfer table in PostgreSQL with camelCase fields"""
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
        cursor.execute('DROP TABLE IF EXISTS "outgoingFundTransfer" CASCADE')
        
        # Create outgoing fund transfer table with all 23 fields using camelCase (quoted identifiers)
        create_table_query = """
        CREATE TABLE "outgoingFundTransfer" (
            "id" SERIAL PRIMARY KEY,
            "reportingDate" TIMESTAMP NOT NULL,
            "transactionId" VARCHAR(255) NOT NULL,
            "transactionDate" DATE,
            "transferChannel" VARCHAR(50) DEFAULT 'EFT',
            "subCategoryTransferChannel" VARCHAR(100),
            "senderAccountNumber" VARCHAR(50),
            "senderIdentificationType" VARCHAR(50),
            "senderIdentificationNumber" VARCHAR(50),
            "recipientName" VARCHAR(255),
            "recipientMobileNumber" VARCHAR(20),
            "recipientCountry" VARCHAR(100) DEFAULT 'TANZANIA, UNITED REPUBLIC OF',
            "recipientBankOrFspCode" VARCHAR(50) DEFAULT 'N/A',
            "recipientAccountOrWalletNumber" VARCHAR(50) DEFAULT 'N/A',
            "serviceChannel" VARCHAR(100) DEFAULT 'Automated Teller Machines',
            "serviceCategory" VARCHAR(100) DEFAULT 'Internet banking',
            "serviceSubCategory" VARCHAR(100) DEFAULT 'Transfer',
            "currency" VARCHAR(10),
            "orgAmount" DECIMAL(18,2),
            "usdAmount" DECIMAL(18,2),
            "tzsAmount" DECIMAL(18,0),
            "purposes" TEXT,
            "senderInstruction" VARCHAR(255) DEFAULT 'N/A',
            "transactionPlace" VARCHAR(100) DEFAULT 'TANZANIA, UNITED REPUBLIC OF',
            "createdAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        cursor.execute(create_table_query)
        
        # Create indexes for better performance using camelCase (quoted identifiers)
        cursor.execute('CREATE INDEX "idxOutgoingFundTransferReportingDate" ON "outgoingFundTransfer"("reportingDate")')
        cursor.execute('CREATE INDEX "idxOutgoingFundTransferTransactionId" ON "outgoingFundTransfer"("transactionId")')
        cursor.execute('CREATE INDEX "idxOutgoingFundTransferTransactionDate" ON "outgoingFundTransfer"("transactionDate")')
        cursor.execute('CREATE INDEX "idxOutgoingFundTransferSenderAccount" ON "outgoingFundTransfer"("senderAccountNumber")')
        cursor.execute('CREATE INDEX "idxOutgoingFundTransferTransferChannel" ON "outgoingFundTransfer"("transferChannel")')
        cursor.execute('CREATE INDEX "idxOutgoingFundTransferCurrency" ON "outgoingFundTransfer"("currency")')
        cursor.execute('CREATE INDEX "idxOutgoingFundTransferServiceChannel" ON "outgoingFundTransfer"("serviceChannel")')
        cursor.execute('CREATE INDEX "idxOutgoingFundTransferCreatedAt" ON "outgoingFundTransfer"("createdAt")')
        
        conn.commit()
        print("Outgoing Fund Transfer table created successfully with indexes")
        
        # Show table info
        cursor.execute("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns 
            WHERE table_name = 'outgoingFundTransfer' 
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
        print(f"Error creating outgoing fund transfer table: {e}")
        raise

if __name__ == "__main__":
    create_outgoing_fund_transfer_table()