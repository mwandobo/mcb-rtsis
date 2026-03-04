-- Create IBCM Transactions table in PostgreSQL
-- This table stores Inter-Bank Call Money transaction data

CREATE TABLE IF NOT EXISTS "ibcmTransactions" (
    id SERIAL PRIMARY KEY,
    "reportingDate" VARCHAR(12) NOT NULL,
    "transactionDate" VARCHAR(12) NOT NULL,
    "lenderName" VARCHAR(255) NOT NULL,
    "borrowerName" VARCHAR(255) NOT NULL,
    "transactionType" VARCHAR(50),
    "tzsAmount" DECIMAL(18,2),
    tenure INTEGER,
    "interestRate" DECIMAL(8,4),
    "createdAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Composite unique constraint to prevent duplicates
    CONSTRAINT uk_ibcm_transactions UNIQUE ("transactionDate", "lenderName", "borrowerName")
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_ibcm_transactions_reporting_date ON "ibcmTransactions"("reportingDate");
CREATE INDEX IF NOT EXISTS idx_ibcm_transactions_transaction_date ON "ibcmTransactions"("transactionDate");
CREATE INDEX IF NOT EXISTS idx_ibcm_transactions_lender ON "ibcmTransactions"("lenderName");
CREATE INDEX IF NOT EXISTS idx_ibcm_transactions_borrower ON "ibcmTransactions"("borrowerName");
CREATE INDEX IF NOT EXISTS idx_ibcm_transactions_type ON "ibcmTransactions"("transactionType");

-- Create trigger to update updatedAt timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW."updatedAt" = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

DROP TRIGGER IF EXISTS update_ibcm_transactions_updated_at ON "ibcmTransactions";
CREATE TRIGGER update_ibcm_transactions_updated_at 
    BEFORE UPDATE ON "ibcmTransactions" 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

-- Add comments for documentation
COMMENT ON TABLE "ibcmTransactions" IS 'Inter-Bank Call Money transactions data from DB2';
COMMENT ON COLUMN "ibcmTransactions"."reportingDate" IS 'Date and time when the report was generated (DDMMYYYYHHMM format)';
COMMENT ON COLUMN "ibcmTransactions"."transactionDate" IS 'Date and time of the transaction (DDMMYYYYHHMM format)';
COMMENT ON COLUMN "ibcmTransactions"."lenderName" IS 'Name of the lending institution';
COMMENT ON COLUMN "ibcmTransactions"."borrowerName" IS 'Name of the borrowing institution';
COMMENT ON COLUMN "ibcmTransactions"."transactionType" IS 'Type of transaction (Market/Off Market)';
COMMENT ON COLUMN "ibcmTransactions"."tzsAmount" IS 'Transaction amount in Tanzanian Shillings';
COMMENT ON COLUMN "ibcmTransactions".tenure IS 'Duration of the transaction in days';
COMMENT ON COLUMN "ibcmTransactions"."interestRate" IS 'Interest rate applied to the transaction';