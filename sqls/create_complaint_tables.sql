-- ============================================================================
-- COMPLAINT STATISTICS TABLES FOR BOT REPORTING
-- ============================================================================
-- Purpose: Create tables to track customer complaints for BOT regulatory reporting
-- Author: Data Team
-- Date: 2026-01-16
-- ============================================================================

-- ============================================================================
-- 1. MAIN COMPLAINT STATISTICS TABLE
-- ============================================================================

CREATE TABLE PROFITS.COMPLAINT_STATISTICS (
    -- Primary Key
    COMPLAINT_ID INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY,
    
    -- Complainant Information (MANDATORY for BOT)
    COMPLAINANT_NAME VARCHAR(200) NOT NULL,
    COMPLAINANT_MOBILE VARCHAR(20),
    FK_CUSTOMER_ID INTEGER,  -- Link to CUSTOMER table
    
    -- Complaint Details (MANDATORY for BOT)
    COMPLAINT_TYPE INTEGER NOT NULL,  -- Reference to D51_COMPLAINT_TYPES
    OCCURRENCE_DATE TIMESTAMP NOT NULL,  -- When the incident occurred
    REPORTING_DATE TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,  -- When complaint was logged
    CLOSURE_DATE TIMESTAMP,  -- When complaint was resolved
    
    -- Transaction/Channel Details
    TRANSACTION_REFERENCE VARCHAR(50),  -- Link to transaction
    CHANNEL_TYPE VARCHAR(20),  -- ATM, POS, Branch, Mobile, Internet, Agent
    AGENT_NAME VARCHAR(200),
    TILL_NUMBER VARCHAR(50),
    FK_ATM_ID INTEGER,  -- Link to ATM if applicable
    FK_BRANCH_CODE VARCHAR(10),  -- Link to branch
    FK_POS_ID INTEGER,  -- Link to POS if applicable
    
    -- Financial Details (MANDATORY for BOT)
    CURRENCY VARCHAR(3) NOT NULL DEFAULT 'TZS',
    ORG_AMOUNT DECIMAL(18,2) NOT NULL DEFAULT 0,
    TZS_AMOUNT DECIMAL(18,2) NOT NULL DEFAULT 0,
    USD_AMOUNT DECIMAL(18,2) NOT NULL DEFAULT 0,
    
    -- Resolution Details (MANDATORY for BOT)
    EMPLOYEE_ID VARCHAR(50) NOT NULL,  -- Assigned employee
    COMPLAINT_STATUS INTEGER NOT NULL DEFAULT 1,  -- Reference to D107_COMPLAINT_STATUS
    REFERRED_COMPLAINTS INTEGER DEFAULT 1,  -- Reference to D57_REFERRED_AUTHORITIES (1=Not Referred)
    RESOLUTION_NOTES VARCHAR(2000),
    
    -- Linkage to Source Systems (for traceability)
    SOURCE_TABLE VARCHAR(50),  -- ATM_ERROR_LOG, IPS_MESSAGE_ERRORS, etc.
    SOURCE_RECORD_ID VARCHAR(100),  -- ID from source table
    
    -- Audit Fields
    CREATED_BY VARCHAR(50) NOT NULL,
    CREATED_DATE TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UPDATED_BY VARCHAR(50),
    UPDATED_DATE TIMESTAMP,
    
    -- Constraints
    CONSTRAINT CHK_COMPLAINT_STATUS CHECK (COMPLAINT_STATUS BETWEEN 1 AND 10),
    CONSTRAINT CHK_CURRENCY CHECK (CURRENCY IN ('TZS','USD','EUR','GBP','KES','UGX','ZAR'))
);

-- Indexes for performance
CREATE INDEX IDX_COMPLAINT_CUSTOMER ON PROFITS.COMPLAINT_STATISTICS(FK_CUSTOMER_ID);
CREATE INDEX IDX_COMPLAINT_DATE ON PROFITS.COMPLAINT_STATISTICS(REPORTING_DATE);
CREATE INDEX IDX_COMPLAINT_STATUS ON PROFITS.COMPLAINT_STATISTICS(COMPLAINT_STATUS);
CREATE INDEX IDX_COMPLAINT_TYPE ON PROFITS.COMPLAINT_STATISTICS(COMPLAINT_TYPE);
CREATE INDEX IDX_COMPLAINT_EMPLOYEE ON PROFITS.COMPLAINT_STATISTICS(EMPLOYEE_ID);
CREATE INDEX IDX_COMPLAINT_TRX_REF ON PROFITS.COMPLAINT_STATISTICS(TRANSACTION_REFERENCE);
CREATE INDEX IDX_COMPLAINT_OCCURRENCE ON PROFITS.COMPLAINT_STATISTICS(OCCURRENCE_DATE);

-- Comments
COMMENT ON TABLE PROFITS.COMPLAINT_STATISTICS IS 'Customer complaints tracking for BOT regulatory reporting';
COMMENT ON COLUMN PROFITS.COMPLAINT_STATISTICS.COMPLAINT_ID IS 'Unique complaint identifier';
COMMENT ON COLUMN PROFITS.COMPLAINT_STATISTICS.COMPLAINANT_NAME IS 'Name of person submitting complaint';
COMMENT ON COLUMN PROFITS.COMPLAINT_STATISTICS.OCCURRENCE_DATE IS 'Date when incident occurred';
COMMENT ON COLUMN PROFITS.COMPLAINT_STATISTICS.REPORTING_DATE IS 'Date when complaint was logged';
COMMENT ON COLUMN PROFITS.COMPLAINT_STATISTICS.CLOSURE_DATE IS 'Date when complaint was resolved';

-- ============================================================================
-- 2. D51 - COMPLAINT TYPES LOOKUP TABLE
-- ============================================================================

CREATE TABLE PROFITS.D51_COMPLAINT_TYPES (
    COMPLAINT_TYPE_ID INTEGER NOT NULL PRIMARY KEY,
    COMPLAINT_TYPE_CODE VARCHAR(20) NOT NULL UNIQUE,
    COMPLAINT_TYPE_NAME VARCHAR(200) NOT NULL,
    COMPLAINT_CATEGORY VARCHAR(50),  -- Product, Service, Transaction, Fraud, Channel
    DESCRIPTION VARCHAR(500),
    IS_ACTIVE CHARACTER(1) DEFAULT 'Y',
    CREATED_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT CHK_COMPLAINT_TYPE_ACTIVE CHECK (IS_ACTIVE IN ('Y','N'))
);

-- Sample complaint types
INSERT INTO PROFITS.D51_COMPLAINT_TYPES (COMPLAINT_TYPE_ID, COMPLAINT_TYPE_CODE, COMPLAINT_TYPE_NAME, COMPLAINT_CATEGORY, DESCRIPTION) VALUES
(1, 'ATM_CARD_STUCK', 'ATM Card Stuck/Retained', 'ATM', 'Customer card was retained by ATM'),
(2, 'ATM_WRONG_AMOUNT', 'ATM Dispensed Wrong Amount', 'ATM', 'ATM dispensed incorrect cash amount'),
(3, 'ATM_NO_CASH', 'ATM Debited But No Cash', 'ATM', 'Account debited but no cash dispensed'),
(4, 'ATM_OUT_OF_SERVICE', 'ATM Out of Service', 'ATM', 'ATM not working or out of cash'),
(5, 'TRANSFER_FAILED', 'Failed Money Transfer', 'Transaction', 'Money transfer failed but account debited'),
(6, 'TRANSFER_DELAYED', 'Delayed Money Transfer', 'Transaction', 'Money transfer taking too long'),
(7, 'UNAUTHORIZED_TRX', 'Unauthorized Transaction', 'Fraud', 'Transaction not authorized by customer'),
(8, 'CARD_FRAUD', 'Card Fraud/Cloning', 'Fraud', 'Suspected card fraud or cloning'),
(9, 'LOAN_DISBURSEMENT', 'Loan Disbursement Issue', 'Product', 'Problem with loan disbursement'),
(10, 'LOAN_REPAYMENT', 'Loan Repayment Issue', 'Product', 'Problem with loan repayment'),
(11, 'ACCOUNT_BALANCE', 'Incorrect Account Balance', 'Account', 'Account balance showing incorrectly'),
(12, 'ACCOUNT_CHARGES', 'Incorrect Account Charges', 'Account', 'Unexpected or incorrect charges'),
(13, 'POOR_SERVICE', 'Poor Customer Service', 'Service', 'Unsatisfactory customer service'),
(14, 'LONG_WAIT_TIME', 'Long Wait Time', 'Service', 'Excessive waiting time at branch'),
(15, 'MOBILE_BANKING', 'Mobile Banking Issue', 'Channel', 'Problem with mobile banking app'),
(16, 'INTERNET_BANKING', 'Internet Banking Issue', 'Channel', 'Problem with internet banking'),
(17, 'AGENT_BANKING', 'Agent Banking Issue', 'Channel', 'Problem with agent banking service'),
(18, 'POS_TRANSACTION', 'POS Transaction Issue', 'Transaction', 'Problem with POS transaction'),
(19, 'CHEQUE_ISSUE', 'Cheque Processing Issue', 'Transaction', 'Problem with cheque processing'),
(20, 'ACCOUNT_OPENING', 'Account Opening Issue', 'Product', 'Problem opening new account'),
(21, 'ACCOUNT_CLOSURE', 'Account Closure Issue', 'Product', 'Problem closing account'),
(22, 'CARD_ISSUE', 'Card Issuance Issue', 'Product', 'Problem with card issuance'),
(23, 'STATEMENT_ISSUE', 'Statement Issue', 'Service', 'Problem with account statement'),
(24, 'FOREX_RATE', 'Foreign Exchange Rate Dispute', 'Transaction', 'Dispute over forex rate applied'),
(25, 'OTHER', 'Other Complaint', 'Other', 'Other types of complaints');

COMMENT ON TABLE PROFITS.D51_COMPLAINT_TYPES IS 'Lookup table for complaint types (BOT Table D51)';

-- ============================================================================
-- 3. D57 - REFERRED AUTHORITIES LOOKUP TABLE
-- ============================================================================

CREATE TABLE PROFITS.D57_REFERRED_AUTHORITIES (
    AUTHORITY_ID INTEGER NOT NULL PRIMARY KEY,
    AUTHORITY_CODE VARCHAR(20) NOT NULL UNIQUE,
    AUTHORITY_NAME VARCHAR(200) NOT NULL,
    DESCRIPTION VARCHAR(500),
    IS_ACTIVE CHARACTER(1) DEFAULT 'Y',
    CREATED_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT CHK_AUTHORITY_ACTIVE CHECK (IS_ACTIVE IN ('Y','N'))
);

-- Sample referred authorities
INSERT INTO PROFITS.D57_REFERRED_AUTHORITIES (AUTHORITY_ID, AUTHORITY_CODE, AUTHORITY_NAME, DESCRIPTION) VALUES
(1, 'NONE', 'Not Referred', 'Complaint not referred to external authority'),
(2, 'BOT', 'Bank of Tanzania', 'Central Bank of Tanzania'),
(3, 'FCC', 'Fair Competition Commission', 'Fair Competition Commission of Tanzania'),
(4, 'TCRA', 'TCRA', 'Tanzania Communications Regulatory Authority'),
(5, 'POLICE', 'Tanzania Police Force', 'Tanzania Police Force - Criminal Investigation'),
(6, 'COURT', 'Court of Law', 'Judicial system'),
(7, 'OMBUDSMAN', 'Banking Ombudsman', 'Banking Ombudsman Office'),
(8, 'TBA', 'Tanzania Bankers Association', 'Tanzania Bankers Association'),
(9, 'INTERNAL_AUDIT', 'Internal Audit', 'Bank Internal Audit Department'),
(10, 'LEGAL', 'Legal Department', 'Bank Legal Department');

COMMENT ON TABLE PROFITS.D57_REFERRED_AUTHORITIES IS 'Lookup table for authorities complaints are referred to (BOT Table D57)';

-- ============================================================================
-- 4. D107 - COMPLAINT STATUS LOOKUP TABLE
-- ============================================================================

CREATE TABLE PROFITS.D107_COMPLAINT_STATUS (
    STATUS_ID INTEGER NOT NULL PRIMARY KEY,
    STATUS_CODE VARCHAR(20) NOT NULL UNIQUE,
    STATUS_NAME VARCHAR(100) NOT NULL,
    DESCRIPTION VARCHAR(500),
    IS_ACTIVE CHARACTER(1) DEFAULT 'Y',
    CREATED_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT CHK_STATUS_ACTIVE CHECK (IS_ACTIVE IN ('Y','N'))
);

-- Sample complaint statuses
INSERT INTO PROFITS.D107_COMPLAINT_STATUS (STATUS_ID, STATUS_CODE, STATUS_NAME, DESCRIPTION) VALUES
(1, 'OPEN', 'Open/Pending', 'Complaint received and pending investigation'),
(2, 'INVESTIGATING', 'Under Investigation', 'Complaint is being investigated'),
(3, 'RESOLVED', 'Resolved', 'Complaint has been resolved to customer satisfaction'),
(4, 'CLOSED', 'Closed', 'Complaint closed (resolved or unresolved)'),
(5, 'ESCALATED', 'Escalated', 'Complaint escalated to higher authority'),
(6, 'REFERRED', 'Referred External', 'Complaint referred to external authority'),
(7, 'WITHDRAWN', 'Withdrawn', 'Complaint withdrawn by customer'),
(8, 'DUPLICATE', 'Duplicate', 'Duplicate complaint'),
(9, 'INVALID', 'Invalid', 'Invalid or frivolous complaint'),
(10, 'PENDING_INFO', 'Pending Information', 'Awaiting additional information from customer');

COMMENT ON TABLE PROFITS.D107_COMPLAINT_STATUS IS 'Lookup table for complaint status (BOT Table D107)';

-- ============================================================================
-- 5. ADD FOREIGN KEY CONSTRAINTS
-- ============================================================================

ALTER TABLE PROFITS.COMPLAINT_STATISTICS 
    ADD CONSTRAINT FK_COMPLAINT_TYPE 
    FOREIGN KEY (COMPLAINT_TYPE) 
    REFERENCES PROFITS.D51_COMPLAINT_TYPES(COMPLAINT_TYPE_ID);

ALTER TABLE PROFITS.COMPLAINT_STATISTICS 
    ADD CONSTRAINT FK_REFERRED_AUTHORITY 
    FOREIGN KEY (REFERRED_COMPLAINTS) 
    REFERENCES PROFITS.D57_REFERRED_AUTHORITIES(AUTHORITY_ID);

ALTER TABLE PROFITS.COMPLAINT_STATISTICS 
    ADD CONSTRAINT FK_COMPLAINT_STATUS_REF 
    FOREIGN KEY (COMPLAINT_STATUS) 
    REFERENCES PROFITS.D107_COMPLAINT_STATUS(STATUS_ID);

ALTER TABLE PROFITS.COMPLAINT_STATISTICS 
    ADD CONSTRAINT FK_COMPLAINT_CUSTOMER 
    FOREIGN KEY (FK_CUSTOMER_ID) 
    REFERENCES PROFITS.CUSTOMER(CUST_ID);

-- ============================================================================
-- 6. GRANT PERMISSIONS
-- ============================================================================

-- Grant permissions to appropriate users/roles
-- GRANT SELECT, INSERT, UPDATE ON PROFITS.COMPLAINT_STATISTICS TO CUSTOMER_SERVICE_ROLE;
-- GRANT SELECT ON PROFITS.D51_COMPLAINT_TYPES TO CUSTOMER_SERVICE_ROLE;
-- GRANT SELECT ON PROFITS.D57_REFERRED_AUTHORITIES TO CUSTOMER_SERVICE_ROLE;
-- GRANT SELECT ON PROFITS.D107_COMPLAINT_STATUS TO CUSTOMER_SERVICE_ROLE;

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

-- Check tables created
SELECT TABNAME, CARD, CREATE_TIME 
FROM SYSCAT.TABLES 
WHERE TABSCHEMA='PROFITS' 
AND TABNAME IN ('COMPLAINT_STATISTICS', 'D51_COMPLAINT_TYPES', 'D57_REFERRED_AUTHORITIES', 'D107_COMPLAINT_STATUS')
ORDER BY TABNAME;

-- Check lookup data
SELECT COUNT(*) as COMPLAINT_TYPES FROM PROFITS.D51_COMPLAINT_TYPES;
SELECT COUNT(*) as REFERRED_AUTHORITIES FROM PROFITS.D57_REFERRED_AUTHORITIES;
SELECT COUNT(*) as COMPLAINT_STATUSES FROM PROFITS.D107_COMPLAINT_STATUS;

-- ============================================================================
-- END OF SCRIPT
-- ============================================================================
