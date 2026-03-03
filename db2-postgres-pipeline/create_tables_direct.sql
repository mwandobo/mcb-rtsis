-- Direct SQL script to create all tables
-- Run this directly in PostgreSQL client (psql, pgAdmin, etc.)
-- Note: This uses the same camelCase schema with quoted identifiers

\echo 'Creating PostgreSQL tables for RTSIS...'

-- Drop existing tables if they exist (optional - uncomment if needed)
-- DROP TABLE IF EXISTS "investmentDebtSecurities" CASCADE;
-- DROP TABLE IF EXISTS "incomeStatement" CASCADE;
-- DROP TABLE IF EXISTS "insuranceCommission" CASCADE;
-- DROP TABLE IF EXISTS "sharedCapital" CASCADE;
-- DROP TABLE IF EXISTS "icbmTransaction" CASCADE;
-- DROP TABLE IF EXISTS "cheques" CASCADE;
-- DROP TABLE IF EXISTS "assetOwned" CASCADE;
-- DROP TABLE IF EXISTS "balanceMno" CASCADE;
-- DROP TABLE IF EXISTS "balanceBot" CASCADE;
-- DROP TABLE IF EXISTS "otherAsset" CASCADE;
-- DROP TABLE IF EXISTS "balanceOtherBanks" CASCADE;
-- DROP TABLE IF EXISTS "loanTransaction" CASCADE;
-- DROP TABLE IF EXISTS "loan" CASCADE;
-- DROP TABLE IF EXISTS "cashInformation" CASCADE;

\echo 'Creating tables...'

-- Cash Information
CREATE TABLE IF NOT EXISTS "cashInformation" (
    "reportingDate" TIMESTAMP,
    "branchCode" INTEGER,
    "cashCategory" VARCHAR(50),
    "cashSubCategory" VARCHAR(50),
    "cashSubmissionTime" VARCHAR(50),
    "currency" VARCHAR(10),
    "cashDenomination" VARCHAR(50),
    "quantityOfCoinsNotes" INTEGER,
    "orgAmount" DECIMAL(15,2),
    "usdAmount" DECIMAL(15,2),
    "tzsAmount" DECIMAL(15,2),
    "transactionDate" DATE,
    "maturityDate" DATE,
    "allowanceProbableLoss" DECIMAL(15,2),
    "botProvision" DECIMAL(15,2)
);

\echo 'Created cashInformation table'

-- Loan Information
CREATE TABLE IF NOT EXISTS "loan" (
    "customerIdentificationNumber" VARCHAR(50),
    "accountNumber" VARCHAR(50),
    "clientName" VARCHAR(200),
    "loanStatus" VARCHAR(50),
    "currency" VARCHAR(10),
    "loanInstallment" INTEGER,
    "branchCode" INTEGER,
    "loanInstallmentPaid" INTEGER,
    "transactionDate" TIMESTAMP
);

\echo 'Created loan table'

-- Loan Transaction
CREATE TABLE IF NOT EXISTS "loanTransaction" (
    "reportingDate" TIMESTAMP,
    "loanNumber" VARCHAR(50),
    "transactionDate" DATE,
    "loanTransactionType" VARCHAR(50),
    "loanTransactionSubType" VARCHAR(50),
    "currency" VARCHAR(10),
    "orgTransactionAmount" DECIMAL(15,2),
    "usdTransactionAmount" DECIMAL(15,2),
    "tzsTransactionAmount" DECIMAL(15,2)
);

\echo 'Created loanTransaction table'

-- Balance with Other Banks
CREATE TABLE IF NOT EXISTS "balanceOtherBanks" (
    "reportingDate" TIMESTAMP,
    "accountNumber" VARCHAR(50),
    "accountName" VARCHAR(200),
    "bankCode" VARCHAR(20),
    "country" VARCHAR(50),
    "relationshipType" VARCHAR(50),
    "accountType" VARCHAR(50),
    "subAccountType" VARCHAR(50),
    "currency" VARCHAR(10),
    "orgAmount" DECIMAL(15,2),
    "usdAmount" DECIMAL(15,2),
    "tzsAmount" DECIMAL(15,2),
    "transactionDate" DATE,
    "pastDueDays" INTEGER,
    "allowanceProbableLoss" DECIMAL(15,2),
    "botProvision" DECIMAL(15,2),
    "assetsClassificationCategory" VARCHAR(50),
    "contractDate" DATE,
    "maturityDate" DATE,
    "externalRatingCorrespondentBank" VARCHAR(100),
    "gradesUnratedBanks" VARCHAR(50)
);

\echo 'Created balanceOtherBanks table'

-- Other Assets
CREATE TABLE IF NOT EXISTS "otherAsset" (
    "reportingDate" TIMESTAMP,
    "assetType" VARCHAR(50),
    "transactionDate" DATE,
    "maturityDate" DATE,
    "debtorName" VARCHAR(200),
    "debtorCountry" VARCHAR(50),
    "currency" VARCHAR(10),
    "orgAmount" DECIMAL(15,2),
    "usdAmount" DECIMAL(15,2),
    "tzsAmount" DECIMAL(15,2),
    "sectorSnaClassification" VARCHAR(100),
    "pastDueDays" INTEGER,
    "assetClassificationCategory" INTEGER,
    "allowanceProbableLoss" DECIMAL(15,2),
    "botProvision" DECIMAL(15,2)
);

\echo 'Created otherAsset table'

-- Balances BOT
CREATE TABLE IF NOT EXISTS "balanceBot" (
    "reportingDate" TIMESTAMP,
    "accountNumber" VARCHAR(50),
    "accountName" VARCHAR(100),
    "accountType" VARCHAR(50),
    "subAccountType" VARCHAR(50),
    "currency" VARCHAR(10),
    "orgAmount" DECIMAL(15,2),
    "usdAmount" DECIMAL(15,2),
    "tzsAmount" DECIMAL(15,2),
    "transactionDate" DATE,
    "maturityDate" TIMESTAMP,
    "allowanceProbableLoss" DECIMAL(15,2),
    "botProvision" DECIMAL(15,2)
);

\echo 'Created balanceBot table'

-- Balances with MNOs
CREATE TABLE IF NOT EXISTS "balanceMno" (
    "reportingDate" TIMESTAMP,
    "floatBalanceDate" TIMESTAMP,
    "mnoCode" VARCHAR(100),
    "tillNumber" VARCHAR(50),
    "currency" VARCHAR(10),
    "allowanceProbableLoss" DECIMAL(15,2),
    "botProvision" DECIMAL(15,2),
    "orgFloatAmount" DECIMAL(15,2),
    "usdFloatAmount" DECIMAL(15,2),
    "tzsFloatAmount" DECIMAL(15,2)
);

\echo 'Created balanceMno table'

-- Asset Owned
CREATE TABLE IF NOT EXISTS "assetOwned" (
    "reportingDate" TIMESTAMP,
    "acquisitionDate" DATE,
    "currency" VARCHAR(10),
    "assetCategory" VARCHAR(50),
    "assetType" VARCHAR(100),
    "orgCostValue" DECIMAL(15,2),
    "usdCostValue" DECIMAL(15,2),
    "tzsCostValue" DECIMAL(15,2),
    "allowanceProbableLoss" DECIMAL(15,2),
    "botProvision" DECIMAL(15,2)
);

\echo 'Created assetOwned table'

-- Cheque Clearing
CREATE TABLE IF NOT EXISTS "cheques" (
    "reportingDate" TIMESTAMP,
    "chequeNumber" VARCHAR(50),
    "issuerName" VARCHAR(200),
    "issuerBankerCode" VARCHAR(20),
    "payeeName" VARCHAR(200),
    "payeeAccountNumber" VARCHAR(50),
    "chequeDate" DATE,
    "transactionDate" DATE,
    "settlementDate" DATE,
    "allowanceProbableLoss" DECIMAL(15,2),
    "botProvision" DECIMAL(15,2),
    "currency" VARCHAR(10),
    "orgAmountOpening" DECIMAL(15,2),
    "usdAmountOpening" DECIMAL(15,2),
    "tzsAmountOpening" DECIMAL(15,2),
    "orgAmountPayment" DECIMAL(15,2),
    "usdAmountPayment" DECIMAL(15,2),
    "tzsAmountPayment" DECIMAL(15,2),
    "orgAmountBalance" DECIMAL(15,2),
    "usdAmountBalance" DECIMAL(15,2),
    "tzsAmountBalance" DECIMAL(15,2)
);

\echo 'Created cheques table'

-- ICBM Transactions
CREATE TABLE IF NOT EXISTS "icbmTransaction" (
    "reportingDate" TIMESTAMP,
    "transactionDate" DATE,
    "lenderName" VARCHAR(200),
    "borrowerName" VARCHAR(200),
    "transactionType" VARCHAR(20),
    "tzsAmount" DECIMAL(15,2),
    "tenure" VARCHAR(50),
    "interestRate" VARCHAR(20)
);

\echo 'Created icbmTransaction table'

-- Shared Capital
CREATE TABLE IF NOT EXISTS "sharedCapital" (
    "reportingDate" TIMESTAMP,
    "capitalCategory" VARCHAR(100),
    "capitalSubCategory" VARCHAR(100),
    "transactionDate" DATE,
    "transactionType" VARCHAR(20),
    "shareholderNames" VARCHAR(200),
    "clientType" VARCHAR(50),
    "shareholderCountry" VARCHAR(50),
    "numberOfShares" VARCHAR(50),
    "sharePriceBookValue" VARCHAR(50),
    "currency" VARCHAR(10),
    "orgAmount" DECIMAL(15,2),
    "usdAmount" DECIMAL(15,2),
    "tzsAmount" DECIMAL(15,2),
    "sectorSnaClassification" VARCHAR(100)
);

\echo 'Created sharedCapital table'

-- Insurance Commission
CREATE TABLE IF NOT EXISTS "insuranceCommission" (
    "reportingDate" TIMESTAMP,
    "policyNumber" VARCHAR(50),
    "currency" VARCHAR(10),
    "orgCommissionReceivedAmount" DECIMAL(15,2),
    "tzsCommissionReceivedAmount" DECIMAL(15,2),
    "commissionReceivedDate" DATE
);

\echo 'Created insuranceCommission table'

-- Income Statement (Daily Snapshot)
CREATE TABLE IF NOT EXISTS "incomeStatement" (
    "reportingDate" TIMESTAMP,
    "interestIncome" DECIMAL(31,2),
    "interestExpense" DECIMAL(31,2),
    "badDebtsWrittenOffNotProvided" DECIMAL(31,2),
    "provisionBadDoubtfulDebts" DECIMAL(31,2),
    "impairmentsInvestments" DECIMAL(31,2),
    "nonInterestIncome" DECIMAL(31,2),
    "nonInterestExpenses" DECIMAL(31,2),
    "incomeTaxProvision" DECIMAL(31,2),
    "extraordinaryCreditsCharge" DECIMAL(31,2),
    "nonCoreCreditsCharges" DECIMAL(31,2)
);

\echo 'Created incomeStatement table'

-- Investment Debt Securities
CREATE TABLE IF NOT EXISTS "investmentDebtSecurities" (
    "reportingDate" TIMESTAMP,
    "securityNumber" VARCHAR(50),
    "securityType" VARCHAR(50),
    "securityIssuerName" VARCHAR(200),
    "externalIssuerRatting" VARCHAR(20),
    "gradesUnratedBanks" VARCHAR(20),
    "securityIssuerCountry" VARCHAR(50),
    "snaIssuerSector" VARCHAR(100),
    "currency" VARCHAR(10),
    "orgCostValueAmount" DECIMAL(15,2),
    "tzsCostValueAmount" DECIMAL(15,2),
    "usdCostValueAmount" DECIMAL(15,2),
    "orgFaceValueAmount" DECIMAL(15,2),
    "tzsgFaceValueAmount" DECIMAL(15,2),
    "usdgFaceValueAmount" DECIMAL(15,2),
    "orgFairValueAmount" DECIMAL(15,2),
    "tzsgFairValueAmount" DECIMAL(15,2),
    "usdgFairValueAmount" DECIMAL(15,2),
    "interestRate" DECIMAL(9,6),
    "purchaseDate" DATE,
    "valueDate" DATE,
    "maturityDate" DATE,
    "tradingIntent" VARCHAR(50),
    "securityEncumbaranceStatus" VARCHAR(20),
    "pastDueDays" INTEGER,
    "allowanceProbableLoss" DECIMAL(15,2),
    "assetClassificationCategory" INTEGER
);

\echo 'Created investmentDebtSecurities table'

-- Create Indexes
\echo 'Creating indexes...'

CREATE INDEX IF NOT EXISTS idx_cash_info_date ON "cashInformation"("transactionDate");
CREATE INDEX IF NOT EXISTS idx_loan_info_status ON "loan"("loanStatus");
CREATE INDEX IF NOT EXISTS idx_loan_trx_date ON "loanTransaction"("transactionDate");
CREATE INDEX IF NOT EXISTS idx_other_assets_type ON "otherAsset"("assetType");
CREATE INDEX IF NOT EXISTS idx_cheque_settlement ON "cheques"("settlementDate");
CREATE INDEX IF NOT EXISTS idx_inv_debt_sec_type ON "investmentDebtSecurities"("securityType");
CREATE INDEX IF NOT EXISTS idx_inv_debt_sec_issuer ON "investmentDebtSecurities"("securityIssuerName");
CREATE INDEX IF NOT EXISTS idx_inv_debt_sec_maturity ON "investmentDebtSecurities"("maturityDate");

\echo 'All tables and indexes created successfully!'
\echo 'You can now verify the tables with: \\dt'