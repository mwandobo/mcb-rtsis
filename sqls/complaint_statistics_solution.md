# Complaint Statistics - PROFITS Data Source Solution

## Executive Summary

**Current Situation**: BOT requires complaint statistics from your core banking system (PROFITS), but there is NO dedicated complaint tracking table in PROFITS.

**Solution**: Build a complaint tracking system using existing PROFITS data + manual complaint logging.

## What EXISTS in PROFITS (Partial Data Sources)

### 1. **ATM_ERROR_LOG** (5.7 million rows) ✅
**Purpose**: ATM transaction errors and failures
**Useful For**: ATM-related complaints (card stuck, wrong amount dispensed, etc.)

**Structure**:
- IN_TMSTAMP, OUT_TMSTAMP (timestamps)
- CURR_TRX_DATE (transaction date)
- REFERENCE_NUMBER (transaction reference)
- TERMINAL_NUMBER (ATM ID)
- REPL_CODE (response/error code)
- EXIT_STATE (error description)
- IN_MESSAGE, OUT_MESSAGE (transaction details)

**Can Provide**:
- ✅ Occurrence Date (CURR_TRX_DATE)
- ✅ Transaction Reference
- ✅ ATM/Terminal Number (as Till Number)
- ⚠️ Partial: Error type (needs mapping to complaint types)

**Missing**:
- ❌ Complainant Name/Mobile
- ❌ Employee Assignment
- ❌ Resolution Status
- ❌ Closure Date

### 2. **LNS_ACC_ERROR** (1.8 million rows) ✅
**Purpose**: Loan account errors
**Useful For**: Loan-related complaints

**Can Provide**:
- ✅ Loan account issues
- ✅ Error timestamps

### 3. **IPS_MESSAGE_ERRORS** (776 rows) ✅
**Purpose**: Interbank Payment System errors
**Useful For**: Failed transfers, payment disputes

**Related Tables**:
- IPS_REJECTION_CODES (55 rows) - rejection reasons
- IPS_ERROR_CODE (23 rows) - error classifications

### 4. **LOAN_REQUEST** (802k rows) ✅
**Purpose**: Loan applications and requests
**Useful For**: Loan application complaints

### 5. **DEAL_TICKET** (2,447 rows) ✅
**Purpose**: Deal/transaction tickets
**Useful For**: Transaction-related issues

### 6. **AML_NO_MON** (18,743 rows) ✅
**Purpose**: AML monitoring/suspicious activities
**Useful For**: Fraud complaints

## What is MISSING (Critical Gaps)

### ❌ No Complaint Registration System
- No table to log customer complaints
- No complainant information (name, mobile)
- No complaint categorization
- No complaint status tracking

### ❌ No Resolution Tracking
- No employee assignment
- No closure dates
- No resolution status
- No escalation tracking

### ❌ No Complaint-Transaction Linkage
- Errors exist, but not linked to customer complaints
- No way to track which errors resulted in complaints

## RECOMMENDED SOLUTION

### Phase 1: Create Complaint Tracking Table (IMMEDIATE)

Create a new table in PROFITS to track complaints:

```sql
CREATE TABLE PROFITS.COMPLAINT_STATISTICS (
    -- Primary Key
    COMPLAINT_ID INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    
    -- Complainant Information (MANDATORY)
    COMPLAINANT_NAME VARCHAR(200) NOT NULL,
    COMPLAINANT_MOBILE VARCHAR(20),
    FK_CUSTOMER_ID INTEGER,  -- Link to CUSTOMER table
    
    -- Complaint Details (MANDATORY)
    COMPLAINT_TYPE INTEGER NOT NULL,  -- Reference to D51 lookup
    OCCURRENCE_DATE TIMESTAMP NOT NULL,
    REPORTING_DATE TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CLOSURE_DATE TIMESTAMP,
    
    -- Transaction/Channel Details
    TRANSACTION_REFERENCE VARCHAR(50),  -- Link to transaction
    CHANNEL_TYPE VARCHAR(20),  -- ATM, POS, Branch, Mobile, etc.
    AGENT_NAME VARCHAR(200),
    TILL_NUMBER VARCHAR(50),
    FK_ATM_ID INTEGER,  -- Link to ATM if applicable
    FK_BRANCH_CODE VARCHAR(10),  -- Link to branch
    
    -- Financial Details (MANDATORY)
    CURRENCY VARCHAR(3) NOT NULL,
    ORG_AMOUNT DECIMAL(18,2) NOT NULL DEFAULT 0,
    TZS_AMOUNT DECIMAL(18,2) NOT NULL DEFAULT 0,
    USD_AMOUNT DECIMAL(18,2) NOT NULL DEFAULT 0,
    
    -- Resolution Details (MANDATORY)
    EMPLOYEE_ID VARCHAR(50) NOT NULL,  -- Assigned employee
    COMPLAINT_STATUS INTEGER NOT NULL DEFAULT 1,  -- Reference to D107 lookup
    REFERRED_COMPLAINTS INTEGER,  -- Reference to D57 lookup
    RESOLUTION_NOTES VARCHAR(2000),
    
    -- Linkage to Source Systems
    SOURCE_TABLE VARCHAR(50),  -- ATM_ERROR_LOG, IPS_MESSAGE_ERRORS, etc.
    SOURCE_RECORD_ID VARCHAR(100),  -- ID from source table
    
    -- Audit Fields
    CREATED_BY VARCHAR(50) NOT NULL,
    CREATED_DATE TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UPDATED_BY VARCHAR(50),
    UPDATED_DATE TIMESTAMP,
    
    -- Constraints
    CONSTRAINT CHK_COMPLAINT_STATUS CHECK (COMPLAINT_STATUS IN (1,2,3,4,5)),
    CONSTRAINT CHK_CURRENCY CHECK (CURRENCY IN ('TZS','USD','EUR','GBP','KES','UGX'))
);

-- Indexes for performance
CREATE INDEX IDX_COMPLAINT_CUSTOMER ON PROFITS.COMPLAINT_STATISTICS(FK_CUSTOMER_ID);
CREATE INDEX IDX_COMPLAINT_DATE ON PROFITS.COMPLAINT_STATISTICS(REPORTING_DATE);
CREATE INDEX IDX_COMPLAINT_STATUS ON PROFITS.COMPLAINT_STATISTICS(COMPLAINT_STATUS);
CREATE INDEX IDX_COMPLAINT_TYPE ON PROFITS.COMPLAINT_STATISTICS(COMPLAINT_TYPE);
CREATE INDEX IDX_COMPLAINT_EMPLOYEE ON PROFITS.COMPLAINT_STATISTICS(EMPLOYEE_ID);
CREATE INDEX IDX_COMPLAINT_TRX_REF ON PROFITS.COMPLAINT_STATISTICS(TRANSACTION_REFERENCE);
```

### Phase 2: Create Lookup Tables (IMMEDIATE)

#### D51 - Complaint Types
```sql
CREATE TABLE PROFITS.D51_COMPLAINT_TYPES (
    COMPLAINT_TYPE_ID INTEGER NOT NULL PRIMARY KEY,
    COMPLAINT_TYPE_CODE VARCHAR(20) NOT NULL UNIQUE,
    COMPLAINT_TYPE_NAME VARCHAR(200) NOT NULL,
    COMPLAINT_CATEGORY VARCHAR(50),  -- Product, Service, Transaction, Fraud, etc.
    IS_ACTIVE CHARACTER(1) DEFAULT 'Y',
    CREATED_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Sample data
INSERT INTO PROFITS.D51_COMPLAINT_TYPES VALUES
(1, 'ATM_CARD_STUCK', 'ATM Card Stuck/Retained', 'ATM', 'Y', CURRENT_TIMESTAMP),
(2, 'ATM_WRONG_AMOUNT', 'ATM Dispensed Wrong Amount', 'ATM', 'Y', CURRENT_TIMESTAMP),
(3, 'ATM_NO_CASH', 'ATM Debited But No Cash Dispensed', 'ATM', 'Y', CURRENT_TIMESTAMP),
(4, 'TRANSFER_FAILED', 'Failed Money Transfer', 'Transaction', 'Y', CURRENT_TIMESTAMP),
(5, 'UNAUTHORIZED_TRX', 'Unauthorized Transaction', 'Fraud', 'Y', CURRENT_TIMESTAMP),
(6, 'LOAN_DISBURSEMENT', 'Loan Disbursement Issue', 'Product', 'Y', CURRENT_TIMESTAMP),
(7, 'ACCOUNT_BALANCE', 'Incorrect Account Balance', 'Account', 'Y', CURRENT_TIMESTAMP),
(8, 'POOR_SERVICE', 'Poor Customer Service', 'Service', 'Y', CURRENT_TIMESTAMP),
(9, 'MOBILE_BANKING', 'Mobile Banking Issue', 'Channel', 'Y', CURRENT_TIMESTAMP),
(10, 'CARD_FRAUD', 'Card Fraud/Cloning', 'Fraud', 'Y', CURRENT_TIMESTAMP);
```

#### D57 - Referred Authorities
```sql
CREATE TABLE PROFITS.D57_REFERRED_AUTHORITIES (
    AUTHORITY_ID INTEGER NOT NULL PRIMARY KEY,
    AUTHORITY_CODE VARCHAR(20) NOT NULL UNIQUE,
    AUTHORITY_NAME VARCHAR(200) NOT NULL,
    IS_ACTIVE CHARACTER(1) DEFAULT 'Y',
    CREATED_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Sample data
INSERT INTO PROFITS.D57_REFERRED_AUTHORITIES VALUES
(1, 'NONE', 'Not Referred', 'Y', CURRENT_TIMESTAMP),
(2, 'BOT', 'Bank of Tanzania', 'Y', CURRENT_TIMESTAMP),
(3, 'FCC', 'Fair Competition Commission', 'Y', CURRENT_TIMESTAMP),
(4, 'TCRA', 'Tanzania Communications Regulatory Authority', 'Y', CURRENT_TIMESTAMP),
(5, 'POLICE', 'Tanzania Police Force', 'Y', CURRENT_TIMESTAMP),
(6, 'COURT', 'Court of Law', 'Y', CURRENT_TIMESTAMP),
(7, 'OMBUDSMAN', 'Banking Ombudsman', 'Y', CURRENT_TIMESTAMP);
```

#### D107 - Complaint Status
```sql
CREATE TABLE PROFITS.D107_COMPLAINT_STATUS (
    STATUS_ID INTEGER NOT NULL PRIMARY KEY,
    STATUS_CODE VARCHAR(20) NOT NULL UNIQUE,
    STATUS_NAME VARCHAR(100) NOT NULL,
    IS_ACTIVE CHARACTER(1) DEFAULT 'Y',
    CREATED_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Sample data
INSERT INTO PROFITS.D107_COMPLAINT_STATUS VALUES
(1, 'OPEN', 'Open/Pending', 'Y', CURRENT_TIMESTAMP),
(2, 'INVESTIGATING', 'Under Investigation', 'Y', CURRENT_TIMESTAMP),
(3, 'RESOLVED', 'Resolved', 'Y', CURRENT_TIMESTAMP),
(4, 'CLOSED', 'Closed', 'Y', CURRENT_TIMESTAMP),
(5, 'ESCALATED', 'Escalated', 'Y', CURRENT_TIMESTAMP),
(6, 'REFERRED', 'Referred to External Authority', 'Y', CURRENT_TIMESTAMP);
```

### Phase 3: Build Integration (SHORT-TERM)

#### Option A: Manual Entry System
Create a simple web form or desktop application where customer service staff can:
1. Log complaints when customers call/visit
2. Link to existing transactions (if applicable)
3. Assign to employees
4. Track resolution

#### Option B: Auto-Create from Error Logs
Create stored procedures to automatically create complaint records from error logs:

```sql
CREATE PROCEDURE PROFITS.SP_CREATE_COMPLAINT_FROM_ATM_ERROR(
    IN p_error_log_id DECIMAL(10),
    IN p_customer_id INTEGER,
    IN p_complainant_name VARCHAR(200),
    IN p_complainant_mobile VARCHAR(20),
    IN p_employee_id VARCHAR(50)
)
LANGUAGE SQL
BEGIN
    DECLARE v_terminal_number VARCHAR(16);
    DECLARE v_reference_number VARCHAR(12);
    DECLARE v_trx_date DATE;
    DECLARE v_complaint_id INTEGER;
    
    -- Get ATM error details
    SELECT TERMINAL_NUMBER, REFERENCE_NUMBER, CURR_TRX_DATE
    INTO v_terminal_number, v_reference_number, v_trx_date
    FROM ATM_ERROR_LOG
    WHERE ID = p_error_log_id;
    
    -- Create complaint record
    INSERT INTO COMPLAINT_STATISTICS (
        COMPLAINANT_NAME,
        COMPLAINANT_MOBILE,
        FK_CUSTOMER_ID,
        COMPLAINT_TYPE,
        OCCURRENCE_DATE,
        REPORTING_DATE,
        TRANSACTION_REFERENCE,
        CHANNEL_TYPE,
        TILL_NUMBER,
        CURRENCY,
        ORG_AMOUNT,
        TZS_AMOUNT,
        USD_AMOUNT,
        EMPLOYEE_ID,
        COMPLAINT_STATUS,
        SOURCE_TABLE,
        SOURCE_RECORD_ID,
        CREATED_BY
    ) VALUES (
        p_complainant_name,
        p_complainant_mobile,
        p_customer_id,
        3,  -- ATM_NO_CASH
        v_trx_date,
        CURRENT_TIMESTAMP,
        v_reference_number,
        'ATM',
        v_terminal_number,
        'TZS',
        0,  -- To be filled by staff
        0,
        0,
        p_employee_id,
        1,  -- OPEN
        'ATM_ERROR_LOG',
        CAST(p_error_log_id AS VARCHAR(100)),
        p_employee_id
    );
    
    -- Return the new complaint ID
    SET v_complaint_id = IDENTITY_VAL_LOCAL();
    
    -- Return result
    VALUES (v_complaint_id, 'Complaint created successfully');
END;
```

### Phase 4: BOT Reporting Query

Once the table is populated, use this query for BOT reporting:

```sql
SELECT 
    TO_CHAR(CURRENT_TIMESTAMP, 'DDMMYYYYHH24MI') as reportingDate,
    cs.COMPLAINANT_NAME as complainantName,
    cs.COMPLAINANT_MOBILE as complainantMobile,
    ct.COMPLAINT_TYPE_CODE as complaintType,
    TO_CHAR(cs.OCCURRENCE_DATE, 'DDMMYYYYHH24MI') as occurrenceDate,
    TO_CHAR(cs.REPORTING_DATE, 'DDMMYYYYHH24MI') as complaintReportingDate,
    TO_CHAR(cs.CLOSURE_DATE, 'DDMMYYYYHH24MI') as closureDate,
    cs.AGENT_NAME as agentName,
    cs.TILL_NUMBER as tillNumber,
    cs.CURRENCY as currency,
    cs.TZS_AMOUNT as tzsAmount,
    cs.ORG_AMOUNT as orgAmount,
    cs.USD_AMOUNT as usdAmount,
    cs.EMPLOYEE_ID as employeeId,
    ra.AUTHORITY_CODE as referredComplaints,
    cst.STATUS_CODE as complaintStatus
FROM PROFITS.COMPLAINT_STATISTICS cs
LEFT JOIN PROFITS.D51_COMPLAINT_TYPES ct ON cs.COMPLAINT_TYPE = ct.COMPLAINT_TYPE_ID
LEFT JOIN PROFITS.D57_REFERRED_AUTHORITIES ra ON cs.REFERRED_COMPLAINTS = ra.AUTHORITY_ID
LEFT JOIN PROFITS.D107_COMPLAINT_STATUS cst ON cs.COMPLAINT_STATUS = cst.STATUS_ID
WHERE cs.REPORTING_DATE >= CURRENT_DATE - 30 DAYS
ORDER BY cs.REPORTING_DATE DESC
```

## Implementation Timeline

### Week 1: Database Setup
- ✅ Create COMPLAINT_STATISTICS table
- ✅ Create lookup tables (D51, D57, D107)
- ✅ Create indexes
- ✅ Test table structure

### Week 2: Integration
- ✅ Build complaint entry form/system
- ✅ Create stored procedures for auto-creation
- ✅ Train customer service staff
- ✅ Start logging complaints

### Week 3: Testing & Validation
- ✅ Test BOT reporting query
- ✅ Validate data quality
- ✅ Create pipeline to PostgreSQL
- ✅ Test BOT API integration

### Week 4: Go Live
- ✅ Deploy to production
- ✅ Monitor complaint logging
- ✅ Generate first BOT report

## Next Steps

1. **Get Management Approval** for creating the complaint tracking system
2. **Assign IT Team** to build the complaint entry interface
3. **Train Staff** on complaint logging procedures
4. **Create SQL Scripts** to set up tables
5. **Test with Sample Data** before going live

## Contact Points

- **Database Team**: Create tables and stored procedures
- **Application Team**: Build complaint entry system
- **Customer Service**: Define complaint types and workflows
- **Compliance Team**: Validate BOT requirements
- **IT Operations**: Deploy and monitor

---

**IMPORTANT**: This is a NEW system that needs to be built. PROFITS doesn't currently track complaints in a structured way. The solution requires both technical implementation (database tables) and operational changes (complaint logging process).
