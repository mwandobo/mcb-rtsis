-- Digital Credit Data Mapping Query
-- This query maps data from PROFITS database tables to the digitalCredit BOT reporting format
-- Uses CUSTOMER (63,348 records) and LOAN_ACCOUNT (33,998 records) tables
-- Identifies digital credit loans using business logic

SELECT 
    -- Reporting date & time (mandatory)
    CURRENT_TIMESTAMP AS reportingDate,
    
    -- Customer Name (mandatory) - Unique customer number assigned by loan provider
    c.CUST_ID AS customerName,
    
    -- Gender (mandatory for individuals) - See table D93 for lookup value
    CASE 
        WHEN c.SEX = 'M' THEN 'Male'
        WHEN c.SEX = 'F' THEN 'Female'
        ELSE NULL
    END AS gender,
    
    -- Disability Status (optional) - See table D94 for lookup value
    NULL AS disabilityStatus,
    
    -- Customer Identification Number (mandatory)
    COALESCE(
        CASE WHEN TRIM(c.EMPLOYEE_ID) = '' THEN NULL ELSE TRIM(c.EMPLOYEE_ID) END,
        CAST(c.CUST_ID AS VARCHAR(20))
    ) AS customerIdentificationNumber,
    
    -- Institution Code (mandatory) - from BANK_PARAMETERS table
    COALESCE(CAST(bp.BANK_CODE AS VARCHAR(10)), 'MCB') AS institutionCode,
    
    -- Branch code (mandatory)
    COALESCE(la.FK_UNITCODE, c.FKUNIT_IS_SERVICED) AS branchCode,
    
    -- Services Facilitator (mandatory) - BOT D74 lookup values
    CASE 
        -- BOT D74 Digital Service Facilitators mapping
        WHEN UPPER(p.DESCRIPTION) LIKE '%MPESA%' OR UPPER(p.DESCRIPTION) LIKE '%M-PESA%' THEN 'M-Pesa'
        WHEN UPPER(p.DESCRIPTION) LIKE '%AIRTEL%' THEN 'Airtel Money'
        WHEN UPPER(p.DESCRIPTION) LIKE '%TIGO%' THEN 'Tigo Pesa'
        WHEN UPPER(p.DESCRIPTION) LIKE '%HALO%' THEN 'Halopesa'
        WHEN UPPER(p.DESCRIPTION) LIKE '%T-PESA%' OR UPPER(p.DESCRIPTION) LIKE '%TPESA%' THEN 'T-Pesa'
        WHEN UPPER(p.DESCRIPTION) LIKE '%EZY%' THEN 'EzyPesa'
        WHEN UPPER(p.DESCRIPTION) LIKE '%AZAM%' THEN 'AzamPesa'
        WHEN UPPER(p.DESCRIPTION) LIKE '%JUMO%' THEN 'Jumo'
        WHEN UPPER(p.DESCRIPTION) LIKE '%LAINA%' THEN 'Laina Finance Limited'
        -- Default: Internal Facilitated for bank's own digital credit platform
        ELSE 'Internal Facilitated'
    END AS servicesFacilitator,
    
    -- Product Name (mandatory)
    COALESCE(p.DESCRIPTION, 'DIGITAL_CREDIT') AS productName,
    
    -- TZS Loan Disbursed Amount (mandatory)
    COALESCE(la.TOT_DRAWDOWN_AMN, la.ACC_LIMIT_AMN, 0) AS tzsLoanDisbursedAmount,
    
    -- Loan Disbursement Date (mandatory)
    COALESCE(la.ACC_OPEN_DT, la.DRAWDOWN_FST_DT) AS loanDisbursementDate,
    
    -- TZS Loan Balance (mandatory)
    COALESCE(la.NRM_CAP_BAL + la.OV_CAP_BAL, 0) AS tzsLoanBalance,
    
    -- Maturity Date (mandatory)
    COALESCE(la.ACC_EXP_DT, la.DRAWDOWN_EXP_DT) AS maturityDate,
    
    -- Loan Id (mandatory)
    CAST(la.FK_UNITCODE AS VARCHAR(10)) || '-' || CAST(la.ACC_TYPE AS VARCHAR(5)) || '-' || CAST(la.ACC_SN AS VARCHAR(10)) AS loanId,
    
    -- Last Deposit Date (mandatory)
    la.LST_TRX_DT AS lastDepositDate,
    
    -- Last Deposit Amount (mandatory)
    NULL AS lastDepositAmount,
    
    -- Payments Installment (mandatory)
    COALESCE(la.INSTALL_COUNT, 12) AS paymentsInstallment,
    
    -- Frequency Repayment (mandatory) - BOT D09 lookup values
    CASE 
        -- Mapping based on complete INSTALL_FREQ analysis (0-49 observed)
        -- Pattern suggests frequency codes may represent days or periods
        WHEN la.INSTALL_FREQ = 0 THEN 'ON_DEMAND' -- 0 = On-demand/revolving (2,513 records)
        WHEN la.INSTALL_FREQ = 1 THEN 'DAILY' -- 1 = Daily
        WHEN la.INSTALL_FREQ BETWEEN 2 AND 6 THEN 'WEEKLY' -- 2-6 = Weekly variations
        WHEN la.INSTALL_FREQ = 6 THEN 'BI_WEEKLY' -- 6 = Bi-weekly (12,450 records - very common!)
        WHEN la.INSTALL_FREQ = 7 THEN 'WEEKLY' -- 7 = Weekly
        WHEN la.INSTALL_FREQ BETWEEN 8 AND 11 THEN 'WEEKLY' -- 8-11 = Weekly variations
        WHEN la.INSTALL_FREQ = 12 THEN 'MONTHLY' -- 12 = Monthly (12 months/year)
        WHEN la.INSTALL_FREQ BETWEEN 13 AND 23 THEN 'MONTHLY' -- 13-23 = Monthly variations
        WHEN la.INSTALL_FREQ = 24 THEN 'BI_WEEKLY' -- 24 = Bi-weekly (353 records)
        WHEN la.INSTALL_FREQ BETWEEN 25 AND 35 THEN 'MONTHLY' -- 25-35 = Monthly variations
        WHEN la.INSTALL_FREQ = 36 THEN 'MONTHLY' -- 36 = Monthly (660 records)
        WHEN la.INSTALL_FREQ BETWEEN 37 AND 47 THEN 'MONTHLY' -- 37-47 = Monthly variations
        WHEN la.INSTALL_FREQ = 48 THEN 'BI_WEEKLY' -- 48 = Bi-weekly (466 records)
        WHEN la.INSTALL_FREQ = 49 THEN 'MONTHLY' -- 49 = Monthly
        ELSE 'MONTHLY' -- Default to monthly for digital credit
    END AS repaymentsFrequency,
    
    -- Loan Amortization Type (mandatory) - BOT D96 lookup values
    CASE 
        -- BOT D96 Amortization Type mapping
        -- Most digital credit loans use reducing balance method
        WHEN la.INSTALL_COUNT = 1 THEN 'Bullet Loan' -- Single payment = bullet loan
        WHEN la.INSTALL_COUNT > 1 AND COALESCE(la.INTER_RATE_SPRD, 0) > 0 THEN 'Reducing Method' -- Multiple installments with interest
        WHEN la.INSTALL_COUNT > 1 THEN 'Fixed Installment' -- Multiple equal installments
        ELSE 'Reducing Method' -- Default for digital credit
    END AS loanAmotizationType,
    
    -- Cycle Number (mandatory for microfinance group loans)
    NULL AS cycleNumber,
    
    -- Loan Amount Paid (mandatory)
    COALESCE(la.TOT_CAP_AMN + la.TOT_NRM_INT_AMN + la.TOT_PNL_INT_AMN, 0) AS loanAmountPaid,
    
    -- Delinquence Date (optional)
    CASE 
        WHEN la.OV_EXP_DT IS NOT NULL AND la.OV_CAP_BAL > 0 THEN la.OV_EXP_DT
        ELSE NULL
    END AS deliquenceDate,
    
    -- Restructuring Date (optional)
    NULL AS restructuringDate,
    
    -- Interest Rate (mandatory)
    COALESCE(la.INTER_RATE_SPRD, 15.0) AS interestRate,
    
    -- Past Due Days (optional)
    CASE 
        WHEN la.OV_EXP_DT IS NOT NULL AND la.OV_CAP_BAL > 0 
        THEN DAYS(CURRENT_DATE) - DAYS(la.OV_EXP_DT)
        ELSE 0
    END AS pastDueDays,
    
    -- Past Due Amount (optional)
    COALESCE(la.OV_CAP_BAL, 0) AS pastDueAmount,
    
    -- Currency (mandatory)
    'TZS' AS currency,
    
    -- Original Accrued Interest (mandatory)
    COALESCE(la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL, 0) AS orgAccruedInterest,
    
    -- TZS Accrued Interest (mandatory)
    COALESCE(la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL, 0) AS tzsAccruedInterest,
    
    -- USD Equivalent Accrued Interest (optional)
    NULL AS usdAccruedInterest,
    
    -- Asset Classification Category (mandatory) - BOT D32 lookup values
    CASE 
        -- BOT D32 Asset Classification mapping based on past due days
        WHEN COALESCE(la.OV_CAP_BAL, 0) <= 0 THEN 'Current' -- Include negative balances as current
        WHEN la.OV_EXP_DT IS NULL THEN 'Current' -- No overdue date means current
        WHEN DAYS(CURRENT_DATE) - DAYS(la.OV_EXP_DT) BETWEEN 1 AND 30 THEN 'EspeciallyMentioned' -- 1-30 days past due
        WHEN DAYS(CURRENT_DATE) - DAYS(la.OV_EXP_DT) BETWEEN 31 AND 90 THEN 'Substandard' -- 31-90 days past due
        WHEN DAYS(CURRENT_DATE) - DAYS(la.OV_EXP_DT) BETWEEN 91 AND 180 THEN 'Doubtful' -- 91-180 days past due
        WHEN DAYS(CURRENT_DATE) - DAYS(la.OV_EXP_DT) > 180 THEN 'Loss' -- >180 days past due
        ELSE 'Current' -- Default to current
    END AS assetClassification,
    
    -- Allowance Probable Loss (mandatory)
    0 AS allowanceProbableLoss,
    
    -- BOT provision (mandatory)
    0 AS botProvision,
    
    -- Interest Suspended Amount (mandatory)
    COALESCE(la.OV_URL_NRM_INT_BAL + la.OV_URL_PNL_INT_BAL, 0) AS interestSuspended

FROM CUSTOMER c
INNER JOIN LOAN_ACCOUNT la ON c.CUST_ID = la.CUST_ID
LEFT JOIN PRODUCT p ON la.FK_LOANFK_PRODUCTI = p.ID_PRODUCT
LEFT JOIN BANK_PARAMETERS bp ON 1=1 -- Single row table with bank configuration

WHERE 
    -- Filter for loans that could be considered digital credit
    la.LOAN_STATUS IS NOT NULL -- Any loan with a status
    AND la.ACC_OPEN_DT IS NOT NULL -- Must have disbursement date
    AND la.ACC_OPEN_DT >= '2018-01-01' -- Recent loans (since 2018)
    AND (
        c.MOBILE_TEL IS NOT NULL -- Has mobile (digital indicator)
        OR la.TOT_DRAWDOWN_AMN <= 10000000 -- Small-medium amounts (≤10M TZS)
        OR la.INSTALL_COUNT <= 60 -- Medium term loans (≤60 installments)
        OR la.ACC_OPEN_DT >= '2020-01-01' -- Recent loans are likely digital
    )

ORDER BY la.ACC_OPEN_DT DESC, c.CUST_ID;