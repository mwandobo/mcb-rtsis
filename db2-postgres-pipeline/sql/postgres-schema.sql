-- PostgreSQL Target Schema for Bank Data Sync
-- Run this before starting the sink connector if auto.create=false
-- Note: Using quoted identifiers to preserve camelCase naming

-- Drop existing tables to recreate with serial IDs
DROP TABLE IF EXISTS "cashInformation" CASCADE;
DROP TABLE IF EXISTS "loan" CASCADE;
DROP TABLE IF EXISTS "loanTransaction" CASCADE;
DROP TABLE IF EXISTS "balanceOtherBanks" CASCADE;
DROP TABLE IF EXISTS "overdraft" CASCADE;
DROP TABLE IF EXISTS "otherAsset" CASCADE;
DROP TABLE IF EXISTS "balanceBot" CASCADE;
DROP TABLE IF EXISTS "balanceMno" CASCADE;
DROP TABLE IF EXISTS "assetOwned" CASCADE;
DROP TABLE IF EXISTS "cheques" CASCADE;
DROP TABLE IF EXISTS "icbmTransaction" CASCADE;
DROP TABLE IF EXISTS "sharedCapital" CASCADE;
DROP TABLE IF EXISTS "insuranceCommission" CASCADE;
DROP TABLE IF EXISTS "incomeStatement" CASCADE;
DROP TABLE IF EXISTS "investmentDebtSecurities" CASCADE;
DROP TABLE IF EXISTS "claimTreasury" CASCADE;
DROP TABLE IF EXISTS "branch" CASCADE;
DROP TABLE IF EXISTS "agents" CASCADE;

-- Cash Information
CREATE TABLE "cashInformation" (
    "id" SERIAL PRIMARY KEY,
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

-- Loan Information
CREATE TABLE "loan" (
    "id" SERIAL PRIMARY KEY,
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

-- Loan Transaction
CREATE TABLE "loanTransaction" (
    "id" SERIAL PRIMARY KEY,
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

-- Balance with Other Banks
CREATE TABLE "balanceOtherBanks" (
    "id" SERIAL PRIMARY KEY,
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

-- Overdraft
CREATE TABLE "overdraft" (
    "id" SERIAL PRIMARY KEY,
    "reportingDate" VARCHAR(20),
    "accountNumber" VARCHAR(50),
    "customerIdentificationNumber" VARCHAR(50),
    "clientName" VARCHAR(200),
    "clientType" VARCHAR(50),
    "borrowerCountry" VARCHAR(10),
    "ratingStatus" VARCHAR(50),
    "crRatingBorrower" VARCHAR(50),
    "gradesUnratedBanks" VARCHAR(50),
    "groupCode" VARCHAR(50),
    "relatedEntityName" VARCHAR(200),
    "relatedParty" VARCHAR(50),
    "relationshipCategory" VARCHAR(50),
    "loanProductType" VARCHAR(100),
    "overdraftEconomicActivity" VARCHAR(100),
    "loanPhase" VARCHAR(50),
    "transferStatus" VARCHAR(50),
    "purposeOtherLoans" VARCHAR(100),
    "contractDate" DATE,
    "branchCode" VARCHAR(20),
    "loanOfficer" VARCHAR(200),
    "loanSupervisor" VARCHAR(200),
    "currency" VARCHAR(10),
    "orgSanctionedAmount" DECIMAL(15,2),
    "usdSanctionedAmount" DECIMAL(15,2),
    "tzsSanctionedAmount" DECIMAL(15,2),
    "orgUtilisedAmount" DECIMAL(15,2),
    "usdUtilisedAmount" DECIMAL(15,2),
    "tzsUtilisedAmount" DECIMAL(15,2),
    "orgCrUsageLast30DaysAmount" DECIMAL(15,2),
    "usdCrUsageLast30DaysAmount" DECIMAL(15,2),
    "tzsCrUsageLast30DaysAmount" DECIMAL(15,2),
    "disbursementDate" DATE,
    "expiryDate" DATE,
    "realEndDate" DATE,
    "orgOutstandingAmount" DECIMAL(15,2),
    "usdOutstandingAmount" DECIMAL(15,2),
    "tzsOutstandingAmount" DECIMAL(15,2),
    "latestCustomerCreditDate" DATE,
    "latestCreditAmount" DECIMAL(15,2),
    "primeLendingRate" DECIMAL(8,4),
    "annualInterestRate" DECIMAL(8,4),
    "collateralPledged" DECIMAL(15,2),
    "orgCollateralValue" DECIMAL(15,2),
    "usdCollateralValue" DECIMAL(15,2),
    "tzsCollateralValue" DECIMAL(15,2),
    "restructuredLoans" INTEGER,
    "pastDueDays" INTEGER,
    "pastDueAmount" DECIMAL(15,2),
    "orgAccruedInterestAmount" DECIMAL(15,2),
    "usdAccruedInterestAmount" DECIMAL(15,2),
    "tzsAccruedInterestAmount" DECIMAL(15,2),
    "orgPenaltyChargedAmount" DECIMAL(15,2),
    "usdPenaltyChargedAmount" DECIMAL(15,2),
    "tzsPenaltyChargedAmount" DECIMAL(15,2),
    "orgLoanFeesChargedAmount" DECIMAL(15,2),
    "usdLoanFeesChargedAmount" DECIMAL(15,2),
    "tzsLoanFeesChargedAmount" DECIMAL(15,2),
    "orgLoanFeesPaidAmount" DECIMAL(15,2),
    "usdLoanFeesPaidAmount" DECIMAL(15,2),
    "tzsLoanFeesPaidAmount" DECIMAL(15,2),
    "orgTotMonthlyPaymentAmount" DECIMAL(15,2),
    "usdTotMonthlyPaymentAmount" DECIMAL(15,2),
    "tzsTotMonthlyPaymentAmount" DECIMAL(15,2),
    "orgInterestPaidTotal" DECIMAL(15,2),
    "usdInterestPaidTotal" DECIMAL(15,2),
    "tzsInterestPaidTotal" DECIMAL(15,2),
    "assetClassificationCategory" VARCHAR(50),
    "sectorSnaClassification" VARCHAR(100),
    "negStatusContract" VARCHAR(50),
    "customerRole" VARCHAR(50),
    "allowanceProbableLoss" DECIMAL(15,2),
    "botProvision" DECIMAL(15,2)
);

-- Other Assets
CREATE TABLE "otherAsset" (
    "id" SERIAL PRIMARY KEY,
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

-- Balances BOT
CREATE TABLE "balanceBot" (
    "id" SERIAL PRIMARY KEY,
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

-- Balances with MNOs
CREATE TABLE "balanceMno" (
    "id" SERIAL PRIMARY KEY,
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

-- Asset Owned
CREATE TABLE "assetOwned" (
    "id" SERIAL PRIMARY KEY,
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

-- Cheque Clearing
CREATE TABLE "cheques" (
    "id" SERIAL PRIMARY KEY,
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

-- ICBM Transactions
CREATE TABLE "icbmTransaction" (
    "id" SERIAL PRIMARY KEY,
    "reportingDate" TIMESTAMP,
    "transactionDate" DATE,
    "lenderName" VARCHAR(200),
    "borrowerName" VARCHAR(200),
    "transactionType" VARCHAR(20),
    "tzsAmount" DECIMAL(15,2),
    "tenure" VARCHAR(50),
    "interestRate" VARCHAR(20)
);

-- Shared Capital
CREATE TABLE "sharedCapital" (
    "id" SERIAL PRIMARY KEY,
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

-- Insurance Commission
CREATE TABLE "insuranceCommission" (
    "id" SERIAL PRIMARY KEY,
    "reportingDate" TIMESTAMP,
    "policyNumber" VARCHAR(50),
    "currency" VARCHAR(10),
    "orgCommissionReceivedAmount" DECIMAL(15,2),
    "tzsCommissionReceivedAmount" DECIMAL(15,2),
    "commissionReceivedDate" DATE
);

-- Income Statement (Daily Snapshot)
CREATE TABLE "incomeStatement" (
    "id" SERIAL PRIMARY KEY,
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

-- Investment Debt Securities
CREATE TABLE "investmentDebtSecurities" (
    "id" SERIAL PRIMARY KEY,
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

-- Claim Treasury
CREATE TABLE "claimTreasury" (
    "id" SERIAL PRIMARY KEY,
    "reportingDate" VARCHAR(20),
    "transactionDate" VARCHAR(20),
    "govInstitutionName" VARCHAR(200),
    "currency" VARCHAR(10),
    "orgAmountClaimed" DECIMAL(15,2),
    "usdAmountClaimed" DECIMAL(15,2),
    "tzsAmountClaimed" DECIMAL(15,2),
    "valueDate" VARCHAR(20),
    "maturityDate" VARCHAR(20),
    "pastDueDays" INTEGER,
    "allowanceProbableLoss" DECIMAL(15,2),
    "botProvision" DECIMAL(15,2),
    "assetClassificationCategory" VARCHAR(50),
    "sectorSnaClassification" VARCHAR(100)
);

-- Branch Information
CREATE TABLE "branch" (
    "id" SERIAL PRIMARY KEY,
    "reportingDate" TIMESTAMP,
    "branchName" VARCHAR(200),
    "taxIdentificationNumber" VARCHAR(50),
    "businessLicense" VARCHAR(100),
    "branchCode" VARCHAR(20),
    "qrFsrCode" VARCHAR(50),
    "region" VARCHAR(100),
    "district" VARCHAR(100),
    "ward" VARCHAR(100),
    "street" VARCHAR(200),
    "houseNumber" VARCHAR(50),
    "postalCode" VARCHAR(20),
    "gpsCoordinates" VARCHAR(100),
    "bankingServices" TEXT,
    "mobileMoneyServices" TEXT,
    "registrationDate" DATE,
    "branchStatus" VARCHAR(50),
    "closureDate" DATE,
    "contactPerson" VARCHAR(200),
    "telephoneNumber" VARCHAR(50),
    "altTelephoneNumber" VARCHAR(50),
    "branchCategory" VARCHAR(100),
    "lastModified" VARCHAR(50)
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_cash_info_date ON "cashInformation"("transactionDate");
CREATE INDEX IF NOT EXISTS idx_loan_info_status ON "loan"("loanStatus");
CREATE INDEX IF NOT EXISTS idx_loan_trx_date ON "loanTransaction"("transactionDate");
CREATE INDEX IF NOT EXISTS idx_other_assets_type ON "otherAsset"("assetType");
CREATE INDEX IF NOT EXISTS idx_cheque_settlement ON "cheques"("settlementDate");
CREATE INDEX IF NOT EXISTS idx_overdraft_contract ON "overdraft"("contractDate");
CREATE INDEX IF NOT EXISTS idx_overdraft_account ON "overdraft"("accountNumber");
CREATE INDEX IF NOT EXISTS idx_inv_debt_sec_type ON "investmentDebtSecurities"("securityType");
CREATE INDEX IF NOT EXISTS idx_inv_debt_sec_issuer ON "investmentDebtSecurities"("securityIssuerName");
CREATE INDEX IF NOT EXISTS idx_inv_debt_sec_maturity ON "investmentDebtSecurities"("maturityDate");
CREATE INDEX IF NOT EXISTS idx_claim_treasury_reporting ON "claimTreasury"("reportingDate");
CREATE INDEX IF NOT EXISTS idx_claim_treasury_institution ON "claimTreasury"("govInstitutionName");
CREATE INDEX IF NOT EXISTS idx_claim_treasury_classification ON "claimTreasury"("assetClassificationCategory");
CREATE UNIQUE INDEX IF NOT EXISTS idx_branch_code_unique ON "branch"("branchCode");
CREATE INDEX IF NOT EXISTS idx_branch_name ON "branch"("branchName");
CREATE INDEX IF NOT EXISTS idx_branch_status ON "branch"("branchStatus");
CREATE INDEX IF NOT EXISTS idx_branch_region ON "branch"("region");
CREATE INDEX IF NOT EXISTS idx_branch_district ON "branch"("district");
CREATE INDEX IF NOT EXISTS idx_branch_last_modified ON "branch"("lastModified");

-- Agents Information
CREATE TABLE "agents" (
    "id" SERIAL PRIMARY KEY,
    "reportingDate" VARCHAR(20),
    "agentName" VARCHAR(200),
    "agentId" VARCHAR(50),
    "tillNumber" VARCHAR(50),
    "businessForm" VARCHAR(50),
    "agentPrincipal" VARCHAR(50),
    "agentPrincipalName" VARCHAR(200),
    "gender" VARCHAR(20),
    "registrationDate" VARCHAR(20),
    "closedDate" VARCHAR(20),
    "certIncorporation" VARCHAR(50),
    "nationality" VARCHAR(50),
    "agentStatus" VARCHAR(50),
    "agentType" VARCHAR(50),
    "accountNumber" VARCHAR(50),
    "region" VARCHAR(100),
    "district" VARCHAR(100),
    "ward" VARCHAR(100),
    "street" VARCHAR(200),
    "houseNumber" VARCHAR(50),
    "postalCode" VARCHAR(20),
    "country" VARCHAR(50),
    "gpsCoordinates" VARCHAR(100),
    "agentTaxIdentificationNumber" VARCHAR(50),
    "businessLicense" VARCHAR(100),
    "lastModified" TIMESTAMP
);

-- Indexes for agents
CREATE UNIQUE INDEX IF NOT EXISTS idx_agents_id_unique ON "agents"("agentId");
CREATE INDEX IF NOT EXISTS idx_agents_name ON "agents"("agentName");
CREATE INDEX IF NOT EXISTS idx_agents_status ON "agents"("agentStatus");
CREATE INDEX IF NOT EXISTS idx_agents_type ON "agents"("agentType");
CREATE INDEX IF NOT EXISTS idx_agents_region ON "agents"("region");
CREATE INDEX IF NOT EXISTS idx_agents_district ON "agents"("district");
CREATE INDEX IF NOT EXISTS idx_agents_last_modified ON "agents"("lastModified");
