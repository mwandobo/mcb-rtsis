-- Create interbank loan payable table for RTSIS reporting
DROP TABLE IF EXISTS "interbankLoanPayable" CASCADE;

-- Create table with all fields from SQL query (21 fields) - No primary key to allow duplicates
CREATE TABLE "interbankLoanPayable" (
    "reportingDate" TIMESTAMP NOT NULL,
    "lenderName" VARCHAR(200),
    "accountNumber" VARCHAR(100) NOT NULL,
    "lenderCountry" VARCHAR(100),
    "borrowingType" VARCHAR(100),
    "transactionDate" DATE,
    "disbursementDate" DATE,
    "maturityDate" DATE,
    "currency" VARCHAR(10),
    "orgAmountOpening" DECIMAL(15,2),
    "usdAmountOpening" DECIMAL(15,2),
    "tzsAmountOpening" DECIMAL(15,2),
    "orgAmountRepayment" DECIMAL(15,2),
    "usdAmountRepayment" DECIMAL(15,2),
    "tzsAmountRepayment" DECIMAL(15,2),
    "orgAmountClosing" DECIMAL(15,2),
    "usdAmountClosing" DECIMAL(15,2),
    "tzsAmountClosing" DECIMAL(15,2),
    "tenureDays" INTEGER,
    "annualInterestRate" DECIMAL(9,6),
    "interestRateType" VARCHAR(50),
    "originalTimestamp" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_interbank_loan_payable_reporting_date ON "interbankLoanPayable" ("reportingDate");
CREATE INDEX IF NOT EXISTS idx_interbank_loan_payable_account_number ON "interbankLoanPayable" ("accountNumber");
CREATE INDEX IF NOT EXISTS idx_interbank_loan_payable_lender ON "interbankLoanPayable" ("lenderName");
CREATE INDEX IF NOT EXISTS idx_interbank_loan_payable_currency ON "interbankLoanPayable" ("currency");
CREATE INDEX IF NOT EXISTS idx_interbank_loan_payable_maturity ON "interbankLoanPayable" ("maturityDate");