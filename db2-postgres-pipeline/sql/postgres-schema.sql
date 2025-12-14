-- PostgreSQL Target Schema for Bank Data Sync
-- Run this before starting the sink connector if auto.create=false

-- Cash Information
CREATE TABLE IF NOT EXISTS cash_information (
    reporting_date TIMESTAMP,
    branch_code INTEGER,
    cash_category VARCHAR(50),
    cash_sub_category VARCHAR(50),
    cash_submission_time VARCHAR(50),
    currency VARCHAR(10),
    cash_denomination VARCHAR(50),
    quantity_of_coins_notes INTEGER,
    org_amount DECIMAL(15,2),
    usd_amount DECIMAL(15,2),
    tzs_amount DECIMAL(15,2),
    transaction_date DATE,
    maturity_date DATE,
    allowance_probable_loss DECIMAL(15,2),
    bot_provision DECIMAL(15,2)
);

-- Loan Information
CREATE TABLE IF NOT EXISTS loan_information (
    customer_identification_number VARCHAR(50),
    account_number VARCHAR(50),
    client_name VARCHAR(200),
    loan_status VARCHAR(50),
    currency VARCHAR(10),
    loan_installment INTEGER,
    branch_code INTEGER,
    loan_installment_paid INTEGER,
    transaction_date TIMESTAMP,
    PRIMARY KEY (account_number)
);

-- Loan Transaction
CREATE TABLE IF NOT EXISTS loan_transaction (
    reporting_date TIMESTAMP,
    loan_number VARCHAR(50),
    transaction_date DATE,
    loan_transaction_type VARCHAR(50),
    loan_transaction_sub_type VARCHAR(50),
    currency VARCHAR(10),
    org_transaction_amount DECIMAL(15,2),
    usd_transaction_amount DECIMAL(15,2),
    tzs_transaction_amount DECIMAL(15,2),
    PRIMARY KEY (loan_number, transaction_date)
);

-- Balance with Other Banks
CREATE TABLE IF NOT EXISTS balance_with_other_bank (
    "reportingDate" TIMESTAMP,
    "accountNumber" VARCHAR(50),
    "accountName" VARCHAR(200),
    "bankCode" VARCHAR(20),
    country VARCHAR(50),
    "relationshipType" VARCHAR(50),
    "accountType" VARCHAR(50),
    "subAccountType" VARCHAR(50),
    currency VARCHAR(10),
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
CREATE TABLE IF NOT EXISTS overdraft (
    reporting_date VARCHAR(20),
    account_number VARCHAR(50),
    customer_identification_number VARCHAR(50),
    client_name VARCHAR(200),
    client_type VARCHAR(50),
    borrower_country VARCHAR(10),
    rating_status VARCHAR(50),
    cr_rating_borrower VARCHAR(50),
    grades_unrated_banks VARCHAR(50),
    group_code VARCHAR(50),
    allowance_probable_loss DECIMAL(15,2),
    bot_provision DECIMAL(15,2),
    PRIMARY KEY (account_number)
);

-- Other Assets
CREATE TABLE IF NOT EXISTS other_assets (
    "reportingDate" TIMESTAMP,
    "assetType" VARCHAR(50),
    "transactionDate" DATE,
    "maturityDate" DATE,
    "debtorName" VARCHAR(200),
    "debtorCountry" VARCHAR(50),
    currency VARCHAR(10),
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
CREATE TABLE IF NOT EXISTS balances_bot (
    "reportingDate" TIMESTAMP,
    "accountNumber" VARCHAR(50),
    "accountName" VARCHAR(100),
    "accountType" VARCHAR(50),
    "subAccountType" VARCHAR(50),
    currency VARCHAR(10),
    "orgAmount" DECIMAL(15,2),
    "usdAmount" DECIMAL(15,2),
    "tzsAmount" DECIMAL(15,2),
    "transactionDate" DATE,
    "maturityDate" TIMESTAMP,
    "allowanceProbableLoss" DECIMAL(15,2),
    "botProvision" DECIMAL(15,2)
);

-- Balances with MNOs
CREATE TABLE IF NOT EXISTS balances_with_mnos (
    "reportingDate" TIMESTAMP,
    "floatBalanceDate" TIMESTAMP,
    "mnoCode" VARCHAR(100),
    "tillNumber" VARCHAR(50),
    currency VARCHAR(10),
    "allowanceProbableLoss" DECIMAL(15,2),
    "botProvision" DECIMAL(15,2),
    "orgFloatAmount" DECIMAL(15,2),
    "usdFloatAmount" DECIMAL(15,2),
    "tzsFloatAmount" DECIMAL(15,2)
);

-- Asset Owned
CREATE TABLE IF NOT EXISTS asset_owned (
    "reportingDate" TIMESTAMP,
    "acquisitionDate" DATE,
    currency VARCHAR(10),
    "assetCategory" VARCHAR(50),
    "assetType" VARCHAR(100),
    "orgCostValue" DECIMAL(15,2),
    "usdCostValue" DECIMAL(15,2),
    "tzsCostValue" DECIMAL(15,2),
    "allowanceProbableLoss" DECIMAL(15,2),
    "botProvision" DECIMAL(15,2)
);

-- Cheque Clearing
CREATE TABLE IF NOT EXISTS cheque_clearing (
    reporting_date TIMESTAMP,
    cheque_number VARCHAR(50),
    issuer_name VARCHAR(200),
    issuer_banker_code VARCHAR(20),
    payee_name VARCHAR(200),
    payee_account_number VARCHAR(50),
    cheque_date DATE,
    transaction_date DATE,
    settlement_date DATE,
    allowance_probable_loss DECIMAL(15,2),
    bot_provision DECIMAL(15,2),
    currency VARCHAR(10),
    org_amount_opening DECIMAL(15,2),
    usd_amount_opening DECIMAL(15,2),
    tzs_amount_opening DECIMAL(15,2),
    org_amount_payment DECIMAL(15,2),
    usd_amount_payment DECIMAL(15,2),
    tzs_amount_payment DECIMAL(15,2),
    org_amount_balance DECIMAL(15,2),
    usd_amount_balance DECIMAL(15,2),
    tzs_amount_balance DECIMAL(15,2),
    PRIMARY KEY (cheque_number, transaction_date)
);

-- ICBM Transactions
CREATE TABLE IF NOT EXISTS icbm_transactions (
    reporting_date TIMESTAMP,
    transaction_date DATE,
    lender_name VARCHAR(200),
    borrower_name VARCHAR(200),
    transaction_type VARCHAR(20),
    tzs_amount DECIMAL(15,2),
    tenure VARCHAR(50),
    interest_rate VARCHAR(20),
    PRIMARY KEY (transaction_date, transaction_type)
);

-- Shared Capital
CREATE TABLE IF NOT EXISTS shared_capital (
    reporting_date TIMESTAMP,
    capital_category VARCHAR(100),
    capital_sub_category VARCHAR(100),
    transaction_date DATE,
    transaction_type VARCHAR(20),
    shareholder_names VARCHAR(200),
    client_type VARCHAR(50),
    shareholder_country VARCHAR(50),
    number_of_shares VARCHAR(50),
    share_price_book_value VARCHAR(50),
    currency VARCHAR(10),
    org_amount DECIMAL(15,2),
    usd_amount DECIMAL(15,2),
    tzs_amount DECIMAL(15,2),
    sector_sna_classification VARCHAR(100),
    PRIMARY KEY (transaction_date, transaction_type)
);

-- Insurance Commission
CREATE TABLE IF NOT EXISTS insurance_commission (
    reporting_date TIMESTAMP,
    policy_number VARCHAR(50),
    currency VARCHAR(10),
    org_commission_received_amount DECIMAL(15,2),
    tzs_commission_received_amount DECIMAL(15,2),
    commission_received_date DATE,
    PRIMARY KEY (commission_received_date, currency)
);

-- Income Statement (Daily Snapshot)
CREATE TABLE IF NOT EXISTS income_statement (
    reporting_date TIMESTAMP PRIMARY KEY,
    interest_income DECIMAL(31,2),
    interest_expense DECIMAL(31,2),
    bad_debts_written_off_not_provided DECIMAL(31,2),
    provision_bad_doubtful_debts DECIMAL(31,2),
    impairments_investments DECIMAL(31,2),
    non_interest_income DECIMAL(31,2),
    non_interest_expenses DECIMAL(31,2),
    income_tax_provision DECIMAL(31,2),
    extraordinary_credits_charge DECIMAL(31,2),
    non_core_credits_charges DECIMAL(31,2)
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_cash_info_date ON cash_information(transaction_date);
CREATE INDEX IF NOT EXISTS idx_loan_info_status ON loan_information(loan_status);
CREATE INDEX IF NOT EXISTS idx_loan_trx_date ON loan_transaction(transaction_date);
CREATE INDEX IF NOT EXISTS idx_other_assets_type ON other_assets(asset_type);
CREATE INDEX IF NOT EXISTS idx_cheque_settlement ON cheque_clearing(settlement_date);
