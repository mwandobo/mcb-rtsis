# Complaint Statistics - Source Table Analysis

## BOT API Requirements

The BOT requires the following complaint statistics fields:

| Field | Mandatory | Description | Data Type | API Attribute |
|-------|-----------|-------------|-----------|---------------|
| Reporting Date & time | Y | Date of pushing the Report to BOT | DDMMYYYYHHMM | reportingDate |
| Complainant Name | Y | The name of the person submitting the complaints | Text | complainantName |
| Complainant Mobile | N | The mobile/Telephone number of the complainant | Numeric | complainantMobile |
| Complaint Type | Y | Nature of the complaint | See table D51 | complaintType |
| Occurrence Date | Y | The date when the incidence occurred | DDMMYYYYHHMM | occurrenceDate |
| Reporting Date | Y | The date when the incidence happened | DDMMYYYYHHMM | complaintReportingDate |
| Closure Date | N | The date when the incidence was attended to finality | DDMMYYYYHHMM | closureDate |
| Agent Name | N | The name of the agent involved | Text | agentName |
| Till Number | N | Till number of the agent | Numeric/Alphanumeric | tillNumber |
| Currency | Y | The currency of the transaction | See Currency table | currency |
| TZS Amount | Y | Value in Tanzanian Shillings | Numeric | tzsAmount |
| Original Amount | Y | Value in Original Currency | Numeric | orgAmount |
| USD Equivalent Amount | Y | Value in USD equivalent | Numeric | usdAmount |
| Employee Id | Y | Employee assigned to resolve the complaint | Numeric/Alphanumeric | employeeId |
| Referred Complaints | Y | Authority to which unresolved complaints referred | See table D57 | referredComplaints |
| Complaint Status | Y | The status of resolution at reporting date | See table D107 | complaintStatus |

## Current Database Status

### Existing Tables Found:
1. **BOT_91_DISPUTE** - Empty table with only 4 columns:
   - DISPUTE_ID (INTEGER)
   - DATEOFDISPUTERESOLVING (DATE)
   - REASONOFTHEDISPUTE (INTEGER)
   - UNRESOLVEDDISPUTE (INTEGER)
   
   **Status**: ❌ Insufficient - Missing most required fields

2. **Related BOT Tables**: 
   - BOT_17_INSTALMENT (FK_BOT_91_DISPUTE)
   - BOT_19_INVOICEBILL (FK_BOT_91_DISPUTE)
   - BOT_21_NONINSTALMENT (FK_BOT_91_DISPUTE)
   - BOT_8_CREDITCARD (FK_BOT_91_DISPUTE)
   
   **Status**: ❌ All empty, no data

### Potential Source Tables:
Based on the search, these tables might contain partial complaint-related data:

1. **CUSTOMER Tables** - For complainant information:
   - CUSTOMER (customer names, IDs)
   - W_DIM_CUSTOMER (customer details with ID_ISSUE_AUTHORITY)
   - CUSTOMER3 (ID_ISSUE_AUTH, ID_ISSUE_DATE)

2. **Transaction Tables** - For transaction amounts:
   - GLI_TRX_EXTRACT (transaction data with amounts, dates, currencies)
   - PROFITS_ACCOUNT (account information)

3. **Employee Tables** - For employee assignment:
   - EMPLOYEE (employee information)
   - LOAN_OFFICER (for loan-related complaints)

4. **Agent Tables** - For agent information:
   - AGENTS_LIST (agent details with BUSINESS_LICENCE_ISSUER_AND_DATE)
   - WAKALA tables (agent/wakala operations)

## Recommendations

### Option 1: Create New Complaint Tracking Table (RECOMMENDED)
Create a dedicated `COMPLAINT_STATISTICS` table in PROFITS database with all required fields:

```sql
CREATE TABLE PROFITS.COMPLAINT_STATISTICS (
    COMPLAINT_ID INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    COMPLAINANT_NAME VARCHAR(200) NOT NULL,
    COMPLAINANT_MOBILE VARCHAR(20),
    COMPLAINT_TYPE INTEGER NOT NULL,  -- Reference to D51 lookup
    OCCURRENCE_DATE TIMESTAMP NOT NULL,
    REPORTING_DATE TIMESTAMP NOT NULL,
    CLOSURE_DATE TIMESTAMP,
    AGENT_NAME VARCHAR(200),
    TILL_NUMBER VARCHAR(50),
    CURRENCY VARCHAR(3) NOT NULL,
    TZS_AMOUNT DECIMAL(18,2) NOT NULL,
    ORG_AMOUNT DECIMAL(18,2) NOT NULL,
    USD_AMOUNT DECIMAL(18,2) NOT NULL,
    EMPLOYEE_ID VARCHAR(50) NOT NULL,
    REFERRED_COMPLAINTS INTEGER,  -- Reference to D57 lookup
    COMPLAINT_STATUS INTEGER NOT NULL,  -- Reference to D107 lookup
    FK_CUSTOMER_ID INTEGER,  -- Link to CUSTOMER table
    FK_TRANSACTION_ID VARCHAR(50),  -- Link to transaction
    FK_AGENT_ID INTEGER,  -- Link to agent
    CREATED_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UPDATED_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Advantages**:
- ✅ Complete control over data structure
- ✅ All required fields in one place
- ✅ Easy to maintain and query
- ✅ Can be integrated with existing customer service systems

**Implementation Steps**:
1. Create the table in DB2
2. Integrate with customer service/CRM system to capture complaints
3. Create triggers or procedures to populate from existing systems
4. Build pipeline to extract to PostgreSQL for BOT reporting

### Option 2: Create View from Existing Tables
Create a view that combines data from multiple existing tables:

```sql
CREATE VIEW PROFITS.V_COMPLAINT_STATISTICS AS
SELECT 
    -- Would need to map from existing tables
    -- This is complex and may not have all required data
    ...
FROM CUSTOMER c
LEFT JOIN GLI_TRX_EXTRACT gte ON ...
LEFT JOIN EMPLOYEE e ON ...
WHERE ...
```

**Challenges**:
- ❌ No single source for complaint data
- ❌ Missing key fields (complaint type, status, closure date, etc.)
- ❌ Difficult to maintain
- ❌ May not have historical complaint data

### Option 3: Enhance BOT_91_DISPUTE Table
Alter the existing `BOT_91_DISPUTE` table to add all required fields:

```sql
ALTER TABLE PROFITS.BOT_91_DISPUTE ADD COLUMN COMPLAINANT_NAME VARCHAR(200);
ALTER TABLE PROFITS.BOT_91_DISPUTE ADD COLUMN COMPLAINANT_MOBILE VARCHAR(20);
-- ... add all other required columns
```

**Advantages**:
- ✅ Uses existing BOT-specific table
- ✅ Already has foreign key relationships with other BOT tables

**Challenges**:
- ⚠️ Table is currently empty
- ⚠️ Need to populate historical data
- ⚠️ Need integration with complaint tracking system

## Lookup Tables Required

The following lookup tables need to be created or verified:

1. **D51 - Complaint Types**
   - Product/Service complaints
   - Transaction disputes
   - Fraud complaints
   - Customer service issues
   - etc.

2. **D57 - Referred Authorities**
   - Bank of Tanzania
   - Fair Competition Commission
   - Tanzania Communications Regulatory Authority
   - Police
   - Court
   - etc.

3. **D107 - Complaint Status**
   - Open/Pending
   - Under Investigation
   - Resolved
   - Closed
   - Escalated
   - etc.

## Next Steps

1. **Immediate**: Confirm with business/compliance team:
   - Is there an existing complaint tracking system?
   - Where is complaint data currently stored?
   - What is the complaint handling process?

2. **Short-term**: 
   - Create the complaint statistics table (Option 1 recommended)
   - Create lookup tables (D51, D57, D107)
   - Integrate with existing complaint management system

3. **Long-term**:
   - Build data pipeline to PostgreSQL
   - Create BOT API endpoint
   - Implement automated reporting

## SQL Query Template (Once Table Exists)

```sql
SELECT 
    TO_CHAR(CURRENT_TIMESTAMP, 'DDMMYYYYHH24MI') as reportingDate,
    cs.COMPLAINANT_NAME as complainantName,
    cs.COMPLAINANT_MOBILE as complainantMobile,
    cs.COMPLAINT_TYPE as complaintType,
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
    cs.REFERRED_COMPLAINTS as referredComplaints,
    cs.COMPLAINT_STATUS as complaintStatus
FROM PROFITS.COMPLAINT_STATISTICS cs
WHERE cs.REPORTING_DATE >= CURRENT_DATE - 30 DAYS
ORDER BY cs.REPORTING_DATE DESC
```

## Conclusion

**There is currently NO suitable source table in the PROFITS database for complaint statistics.**

The recommended approach is to **create a new dedicated COMPLAINT_STATISTICS table** and integrate it with the bank's complaint management system. This will ensure all required BOT fields are captured and maintained properly.
