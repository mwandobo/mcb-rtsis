SELECT
    -- 1. Reporting Date & time (DDMMYYYYHHMM)
    COALESCE(TO_CHAR(current_timestamp, 'DDMMYYYYHH24MI'), '') as reportingDate,

    -- 2. Complainant Name
    COALESCE(CUSTOMER_NAME, '')                                as complainantName,

    -- 3. Complainant Mobile
    COALESCE(CONTACT_NUMBER, '')                               as complainantMobile,

    -- 4. Complaint Type (Mapped based on Nature of Complaint)
    -- 4. Complaint Type (Mapped based on Granular BOT Accepted Values)
    CASE
        -- Loan Related
        WHEN UPPER(NATURE_OF_COMPLAINT) LIKE '%INTEREST RATE%' THEN 'Interest rates'
        WHEN UPPER(NATURE_OF_COMPLAINT) LIKE '%LOAN%AGREEMENT%' THEN 'Loan Agreement'
        WHEN UPPER(NATURE_OF_COMPLAINT) LIKE '%LOAN%REPAY%' THEN 'Loan repayments'
        WHEN UPPER(NATURE_OF_COMPLAINT) LIKE '%LOAN%STATEMENT%' THEN 'Loans statement'
        WHEN UPPER(NATURE_OF_COMPLAINT) LIKE '%LOAN%PROCESS%' THEN 'Loan processing'
        WHEN UPPER(NATURE_OF_COMPLAINT) LIKE '%COLLATERAL%' THEN 'Collateral disposal'

        -- ATM / Card Related
        WHEN UPPER(NATURE_OF_COMPLAINT) LIKE '%SKIM%' THEN 'Skimming'
        WHEN UPPER(NATURE_OF_COMPLAINT) LIKE '%CARD%NOT%RECEIVE%' THEN 'Card not received'
        WHEN UPPER(NATURE_OF_COMPLAINT) LIKE '%STOLEN%CARD%' OR UPPER(NATURE_OF_COMPLAINT) LIKE '%STOLEN%PIN%'
            THEN 'Stolen card and Pin'
        WHEN UPPER(NATURE_OF_COMPLAINT) LIKE '%JAM%' OR UPPER(NATURE_OF_COMPLAINT) LIKE '%STUCK%' THEN 'Jammed Card'
        WHEN UPPER(NATURE_OF_COMPLAINT) LIKE '%INSURANCE%CARD%' THEN 'Insurance on the Card'

        -- Mobile / Network Related
        WHEN UPPER(NATURE_OF_COMPLAINT) LIKE '%NETWORK%' THEN 'Network problem'
        WHEN UPPER(NATURE_OF_COMPLAINT) LIKE '%MOBILE%' OR UPPER(NATURE_OF_COMPLAINT) LIKE '%WAKALA%' OR
             UPPER(NATURE_OF_COMPLAINT) LIKE '%OTHER%NETWORK%' THEN 'Failure to cross to other networks'

        -- Transactional / Account Related
        WHEN UPPER(NATURE_OF_COMPLAINT) LIKE '%FRAUD%' THEN 'Fraud'
        WHEN UPPER(NATURE_OF_COMPLAINT) LIKE '%INCOMPLETE%' THEN 'Incomplete transaction'
        WHEN UPPER(NATURE_OF_COMPLAINT) LIKE '%OVERCHARG%' THEN 'Overcharging'
        WHEN UPPER(NATURE_OF_COMPLAINT) LIKE '%FAILED%TRANSACTION%' THEN 'Failed transaction'
        WHEN UPPER(NATURE_OF_COMPLAINT) LIKE '%DELAY%RESPONSE%' THEN 'Delayed response'
        WHEN UPPER(NATURE_OF_COMPLAINT) LIKE '%WRONG%BENEFICIARY%' THEN 'Wrong beneficiary'
        WHEN UPPER(NATURE_OF_COMPLAINT) LIKE '%DELAY%PAYMENT%' THEN 'Delayed Payment'
        WHEN UPPER(NATURE_OF_COMPLAINT) LIKE '%MISSING%CONTRIBUTION%' THEN 'Missing Contribution'
        WHEN UPPER(NATURE_OF_COMPLAINT) LIKE '%PENSION%' OR UPPER(NATURE_OF_COMPLAINT) LIKE '%SURVIVOR%'
            THEN 'Unpaid Pension/Survivor'
        WHEN UPPER(NATURE_OF_COMPLAINT) LIKE '%UNDERPAY%' THEN 'UnderPayment'
        WHEN UPPER(NATURE_OF_COMPLAINT) LIKE '%ADVERTISING%' THEN 'Misleading Advertising'
        WHEN UPPER(NATURE_OF_COMPLAINT) LIKE '%CONTRACT%' THEN 'Termination of contract'
        WHEN UPPER(NATURE_OF_COMPLAINT) LIKE '%SERVICE%PROVIDER%' THEN 'Change of financial service provider'
        WHEN UPPER(NATURE_OF_COMPLAINT) LIKE '%EXCLUSIVE%' THEN 'Exclusive restrictions'
        WHEN UPPER(NATURE_OF_COMPLAINT) LIKE '%COMMISSION%' THEN 'Unpaid Commission'
        WHEN UPPER(NATURE_OF_COMPLAINT) LIKE '%DATA%BREACH%' THEN 'Data breach'
        WHEN UPPER(NATURE_OF_COMPLAINT) LIKE '%CREDIT%REFERENCE%' THEN 'Credit reference report'
        WHEN UPPER(NATURE_OF_COMPLAINT) LIKE '%ACCESBILITY%' OR UPPER(NATURE_OF_COMPLAINT) LIKE '%SECURITY%'
            THEN 'Accesbility and Security'
        WHEN UPPER(NATURE_OF_COMPLAINT) LIKE '%ACCOUNT%TRANSACTION%' THEN 'Account and transactional'
        WHEN UPPER(NATURE_OF_COMPLAINT) LIKE '%CUSTOMER%SERVICE%' THEN 'Customer Service'
        WHEN UPPER(NATURE_OF_COMPLAINT) LIKE '%DATA%PRIVACY%' THEN 'Data privacy'
        WHEN UPPER(NATURE_OF_COMPLAINT) LIKE '%LEGAL%' OR UPPER(NATURE_OF_COMPLAINT) LIKE '%COMPLIANCE%'
            THEN 'Legal and Compliance'

        ELSE 'Account and transactional' -- Default fall-through for account issues
        END                                                    as complaintType,

    COALESCE(TO_CHAR(LOGGED_DATE, 'DDMMYYYYHH24MI'), '')       as occurrenceDate,

    -- 6. Reporting Date (When incident happened/reported - DDMMYYYYHHMM)
    COALESCE(TO_CHAR(LOGGED_DATE, 'DDMMYYYYHH24MI'), '')       as complaintReportingDate,

    -- 7. Closure Date (DDMMYYYYHHMM)
    COALESCE(TO_CHAR(CLOSING_DATE, 'DDMMYYYYHH24MI'), '')      as closureDate,


    CAST(NULL AS VARCHAR(100))                                 as agentName,


    CAST(NULL AS VARCHAR(50))                                  as tillNumber,


    CAST('TZS' AS VARCHAR(3))                                  as currency,


    CAST(0 AS DECIMAL(18, 2))                                  as tzsAmount,


    CAST(0 AS DECIMAL(18, 2))                                  as orgAmount,


    CAST(0 AS DECIMAL(18, 2))                                  as usdAmount,

    -- 14. Employee Id
    CASE QUERY_OWNER
        WHEN 'JULIETH' THEN 'EIC643'
        WHEN 'LYDIA' THEN 'EIC633'
        END
                                                               as employeeId,


    'Internal Resolution'                                      as referredComplaints,

    CASE STATUS
        WHEN 'CLOSED' THEN 'resolved'
        WHEN 'PENING' THEN 'inprogress'
        ELSE 'unresolved'
        END
                                                               as complaintStatus

FROM PROFITS.COMPLAINT_REGISTER
ORDER BY TMSTAMP DESC;
