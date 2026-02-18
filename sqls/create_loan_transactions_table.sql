-- Create loan_transactions table for streaming pipeline
-- Target: PostgreSQL database

DROP TABLE IF EXISTS loan_transactions CASCADE;

CREATE TABLE loan_transactions (
    id SERIAL PRIMARY KEY,
    reporting_date TIMESTAMP,
    loan_number VARCHAR(50),
    transaction_date DATE,
    loan_transaction_type VARCHAR(100),
    loan_transaction_sub_type VARCHAR(100),
    currency VARCHAR(10),
    org_transaction_amount DECIMAL(18, 2),
    usd_transaction_amount DECIMAL(18, 2),
    tzs_transaction_amount DECIMAL(18, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX idx_loan_transactions_loan_number 
    ON loan_transactions(loan_number);

CREATE INDEX idx_loan_transactions_transaction_date 
    ON loan_transactions(transaction_date);

CREATE INDEX idx_loan_transactions_type 
    ON loan_transactions(loan_transaction_type);

CREATE INDEX idx_loan_transactions_currency 
    ON loan_transactions(currency);

CREATE INDEX idx_loan_transactions_reporting_date 
    ON loan_transactions(reporting_date);

-- Create trigger to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_loan_transactions_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_loan_transactions_updated_at
    BEFORE UPDATE ON loan_transactions
    FOR EACH ROW
    EXECUTE FUNCTION update_loan_transactions_updated_at();

-- Add comments for documentation
COMMENT ON TABLE loan_transactions IS 'Stores loan transaction data streamed from DB2 source';
COMMENT ON COLUMN loan_transactions.reporting_date IS 'Timestamp when the record was reported';
COMMENT ON COLUMN loan_transactions.loan_number IS 'Unique loan account number';
COMMENT ON COLUMN loan_transactions.transaction_date IS 'Date when the transaction occurred';
COMMENT ON COLUMN loan_transactions.loan_transaction_type IS 'Type of loan transaction (e.g., Installment payment, Loan disbursement)';
COMMENT ON COLUMN loan_transactions.loan_transaction_sub_type IS 'Sub-type providing additional transaction details';
COMMENT ON COLUMN loan_transactions.currency IS 'Currency code (USD, TZS, etc.)';
COMMENT ON COLUMN loan_transactions.org_transaction_amount IS 'Original transaction amount in source currency';
COMMENT ON COLUMN loan_transactions.usd_transaction_amount IS 'Transaction amount in USD';
COMMENT ON COLUMN loan_transactions.tzs_transaction_amount IS 'Transaction amount in TZS';
