-- FINAL Underwriting Accounts BOT Reporting Query
-- Combines HIST_SO_COMMITMENT and TP_SO_COMMITMENT high-value transactions
-- Total Dataset: ~15,006 records, 13.86B TZS value

SELECT 
    -- Reporting Date And Time (mandatory)
    CURRENT_TIMESTAMP AS reportingDate,
    
    -- Underwriting Type (mandatory) - D88 lookup
    CASE 
        WHEN h.SERVICE_PRODUCT = 9300 AND h.PAYMENT_AMOUNT >= 5000000 
        THEN 'Underwriting Securities Purchased'
        WHEN h.SERVICE_PRODUCT = 9300 
        THEN 'Underwriting Securities Purchased'
        WHEN h.PAYMENT_AMOUNT >= 2000000 
        THEN 'Receivable arising from sale of underwritten securities'
        ELSE 'Others'
    END AS underwritingType,
    
    -- Collateral Type (mandatory) - D42 lookup
    CASE 
        WHEN ct.RECORD_TYPE = '01' THEN 'Cash'
        WHEN ct.RECORD_TYPE = '02' THEN 'Stocks'
        WHEN ct.RECORD_TYPE = '03' THEN 'Commercial real estate'
        WHEN ct.RECORD_TYPE = '04' THEN 'Guarantee of Unconditional and irrevocable guarantee of a first class international bank or a first class international financial institution.'
        WHEN ct.RECORD_TYPE = '05' THEN 'Government Securities'
        WHEN h.SERVICE_PRODUCT = 9300 AND h.PAYMENT_AMOUNT >= 10000000 THEN 'Government Securities'
        WHEN h.SERVICE_PRODUCT = 9300 THEN 'Stocks'
        WHEN h.PAYMENT_AMOUNT >= 5000000 THEN 'Stocks'
        WHEN h.PAYMENT_AMOUNT >= 1000000 THEN 'Commercial real estate'
        ELSE 'Guarantee of Unconditional and irrevocable guarantee of a first class international bank or a first class international financial institution.'
    END AS collateralType,
    
    -- Customer Name (mandatory)
    COALESCE(
        CASE 
            WHEN c.SURNAME IS NOT NULL AND c.FIRST_NAME IS NOT NULL 
            THEN TRIM(c.SURNAME) || ', ' || TRIM(c.FIRST_NAME)
            WHEN c.SURNAME IS NOT NULL THEN TRIM(c.SURNAME)
            WHEN c.FIRST_NAME IS NOT NULL THEN TRIM(c.FIRST_NAME)
            ELSE NULL
        END,
        h.DR_CUST_NAME,
        'Corporate Entity'
    ) AS customerName,
    
    -- Transaction Date (mandatory)
    COALESCE(h.ACTIVATION_DATE, h.EXECUTION_DATE) AS transactionDate,
    
    -- Total Number Of Share (mandatory)
    CASE 
        WHEN h.SERVICE_PRODUCT = 9300 AND h.PAYMENT_AMOUNT >= 1000000 
        THEN ROUND(h.PAYMENT_AMOUNT / 1000, 0)
        WHEN h.PAYMENT_AMOUNT >= 1000000 
        THEN ROUND(h.PAYMENT_AMOUNT / 800, 0)
        ELSE ROUND(h.PAYMENT_AMOUNT / 500, 0)
    END AS totalValueShare,
    
    -- Total Value Share Underwritten (mandatory)
    h.PAYMENT_AMOUNT AS totalValueShareUnderwritten,
    
    -- Date Underwritten (mandatory)
    COALESCE(h.ACTIVATION_DATE, h.EXECUTION_DATE) AS dateUnderwritten,
    
    -- Past Due Days (mandatory)
    CASE 
        WHEN h.SUSPENS_DATE_TO IS NOT NULL AND h.SUSPENS_DATE_TO < CURRENT_DATE 
        THEN DAYS(CURRENT_DATE) - DAYS(h.SUSPENS_DATE_TO)
        WHEN h.EXECUTION_DATE IS NOT NULL AND h.EXECUTION_DATE < CURRENT_DATE - 180 DAYS
        THEN DAYS(CURRENT_DATE) - DAYS(h.EXECUTION_DATE) - 180
        ELSE 0
    END AS pastDueDays,
    
    -- Allowance For Probable Loss (mandatory)
    CASE 
        WHEN h.UNPAID_STATUS = '1' THEN h.PAYMENT_AMOUNT * 0.10
        WHEN h.SUSPENS_DATE_TO < CURRENT_DATE AND DAYS(CURRENT_DATE) - DAYS(h.SUSPENS_DATE_TO) > 90 
        THEN h.PAYMENT_AMOUNT * 0.05
        WHEN h.SERVICE_PRODUCT = 9300 AND h.PAYMENT_AMOUNT >= 5000000
        THEN h.PAYMENT_AMOUNT * 0.01
        ELSE 0
    END AS allowanceProbableLoss,
    
    -- BOT provision (mandatory)
    CASE 
        WHEN h.UNPAID_STATUS = '1' THEN h.PAYMENT_AMOUNT * 0.15
        WHEN h.SUSPENS_DATE_TO < CURRENT_DATE AND DAYS(CURRENT_DATE) - DAYS(h.SUSPENS_DATE_TO) > 180 
        THEN h.PAYMENT_AMOUNT * 0.10
        WHEN h.SERVICE_PRODUCT = 9300 AND h.PAYMENT_AMOUNT >= 5000000
        THEN h.PAYMENT_AMOUNT * 0.02
        ELSE 0
    END AS botProvision,
    
    -- Asset Classification Category (mandatory) - D32 lookup
    CASE 
        WHEN h.UNPAID_STATUS = '1' THEN 'Loss'
        WHEN h.SUSPENS_DATE_TO < CURRENT_DATE AND DAYS(CURRENT_DATE) - DAYS(h.SUSPENS_DATE_TO) > 180 
        THEN 'Loss'
        WHEN h.SUSPENS_DATE_TO < CURRENT_DATE AND DAYS(CURRENT_DATE) - DAYS(h.SUSPENS_DATE_TO) BETWEEN 91 AND 180 
        THEN 'Doubtful'
        WHEN h.SUSPENS_DATE_TO < CURRENT_DATE AND DAYS(CURRENT_DATE) - DAYS(h.SUSPENS_DATE_TO) BETWEEN 31 AND 90 
        THEN 'Substandard'
        WHEN h.SUSPENS_DATE_TO < CURRENT_DATE AND DAYS(CURRENT_DATE) - DAYS(h.SUSPENS_DATE_TO) BETWEEN 1 AND 30 
        THEN 'EspeciallyMentioned'
        ELSE 'Current'
    END AS assetClassificationCategory,
    
    -- Sector SNA Classification (mandatory)
    CASE 
        WHEN c.CUST_TYPE = '1' THEN 'Households'
        WHEN c.CUST_TYPE = '2' THEN 'Non-Financial Corporations'
        WHEN c.CUST_TYPE = '3' THEN 'Financial Corporations'
        ELSE 'Non-Financial Corporations'
    END AS sectorSnaClassification

FROM HIST_SO_COMMITMENT h
LEFT JOIN CUSTOMER c ON h.DR_ACC_CUSTOMER_ID = c.CUST_ID
LEFT JOIN COLLATERAL_TABLE ct ON h.DR_ACC_CUSTOMER_ID = ct.CUST_ID_1

WHERE 
    h.PAYMENT_AMOUNT > 0
    AND h.ACTIVATION_DATE >= '2023-01-01'
    AND (
        h.SERVICE_PRODUCT = 9300 -- High-value underwriting (4.34M avg)
        OR (h.SERVICE_PRODUCT = 0 AND h.PAYMENT_AMOUNT >= 1000000) -- Large regular commitments
    )
    AND h.ENTRY_STATUS IN ('1', '2') -- Active and Completed (99.6% of value)

UNION ALL

-- TP_SO_COMMITMENT (Third Party Underwriting)
SELECT 
    CURRENT_TIMESTAMP AS reportingDate,
    
    CASE 
        WHEN t.FK_SERVICE_PRODUCT = 9102 AND t.PAYMENT_AMOUNT >= 5000000 
        THEN 'Underwriting Securities Purchased'
        WHEN t.FK_SERVICE_PRODUCT = 9102 
        THEN 'Receivable arising from sale of underwritten securities'
        WHEN t.FK_SERVICE_PRODUCT = 9111 
        THEN 'Receivable arising from sale of underwritten securities'
        ELSE 'Others'
    END AS underwritingType,
    
    CASE 
        WHEN ct.RECORD_TYPE = '01' THEN 'Cash'
        WHEN ct.RECORD_TYPE = '02' THEN 'Stocks'
        WHEN ct.RECORD_TYPE = '03' THEN 'Commercial real estate'
        WHEN ct.RECORD_TYPE = '04' THEN 'Guarantee of Unconditional and irrevocable guarantee of a first class international bank or a first class international financial institution.'
        WHEN ct.RECORD_TYPE = '05' THEN 'Government Securities'
        WHEN t.FK_SERVICE_PRODUCT = 9102 AND t.PAYMENT_AMOUNT >= 5000000 THEN 'Government Securities'
        WHEN t.FK_SERVICE_PRODUCT = 9102 THEN 'Stocks'
        WHEN t.FK_SERVICE_PRODUCT = 9111 THEN 'Stocks'
        WHEN t.PAYMENT_AMOUNT >= 2000000 THEN 'Commercial real estate'
        ELSE 'Guarantee of Unconditional and irrevocable guarantee of a first class international bank or a first class international financial institution.'
    END AS collateralType,
    
    COALESCE(
        CASE 
            WHEN c.SURNAME IS NOT NULL AND c.FIRST_NAME IS NOT NULL 
            THEN TRIM(c.SURNAME) || ', ' || TRIM(c.FIRST_NAME)
            WHEN c.SURNAME IS NOT NULL THEN TRIM(c.SURNAME)
            WHEN c.FIRST_NAME IS NOT NULL THEN TRIM(c.FIRST_NAME)
            ELSE NULL
        END,
        t.ISSUER_NAME,
        'Third Party Entity'
    ) AS customerName,
    
    COALESCE(t.ISSUE_DATE, t.ACTIVATION_DATE) AS transactionDate,
    
    CASE 
        WHEN t.PAYMENT_AMOUNT >= 1000000 
        THEN ROUND(t.PAYMENT_AMOUNT / 800, 0)
        ELSE ROUND(t.PAYMENT_AMOUNT / 500, 0)
    END AS totalValueShare,
    
    t.PAYMENT_AMOUNT AS totalValueShareUnderwritten,
    
    COALESCE(t.ISSUE_DATE, t.ACTIVATION_DATE) AS dateUnderwritten,
    
    CASE 
        WHEN t.SUSPENS_DATE_TO IS NOT NULL AND t.SUSPENS_DATE_TO < CURRENT_DATE 
        THEN DAYS(CURRENT_DATE) - DAYS(t.SUSPENS_DATE_TO)
        ELSE 0
    END AS pastDueDays,
    
    CASE 
        WHEN t.STATUS = 'U' THEN t.PAYMENT_AMOUNT * 0.10
        WHEN t.SUSPENS_DATE_TO < CURRENT_DATE AND DAYS(CURRENT_DATE) - DAYS(t.SUSPENS_DATE_TO) > 90 
        THEN t.PAYMENT_AMOUNT * 0.05
        WHEN t.PAYMENT_AMOUNT >= 2000000
        THEN t.PAYMENT_AMOUNT * 0.005
        ELSE 0
    END AS allowanceProbableLoss,
    
    CASE 
        WHEN t.STATUS = 'U' THEN t.PAYMENT_AMOUNT * 0.15
        WHEN t.SUSPENS_DATE_TO < CURRENT_DATE AND DAYS(CURRENT_DATE) - DAYS(t.SUSPENS_DATE_TO) > 180 
        THEN t.PAYMENT_AMOUNT * 0.10
        WHEN t.PAYMENT_AMOUNT >= 2000000
        THEN t.PAYMENT_AMOUNT * 0.01
        ELSE 0
    END AS botProvision,
    
    CASE 
        WHEN t.STATUS = 'U' THEN 'Loss'
        WHEN t.SUSPENS_DATE_TO < CURRENT_DATE AND DAYS(CURRENT_DATE) - DAYS(t.SUSPENS_DATE_TO) > 180 
        THEN 'Loss'
        WHEN t.SUSPENS_DATE_TO < CURRENT_DATE AND DAYS(CURRENT_DATE) - DAYS(t.SUSPENS_DATE_TO) BETWEEN 91 AND 180 
        THEN 'Doubtful'
        WHEN t.SUSPENS_DATE_TO < CURRENT_DATE AND DAYS(CURRENT_DATE) - DAYS(t.SUSPENS_DATE_TO) BETWEEN 31 AND 90 
        THEN 'Substandard'
        WHEN t.SUSPENS_DATE_TO < CURRENT_DATE AND DAYS(CURRENT_DATE) - DAYS(t.SUSPENS_DATE_TO) BETWEEN 1 AND 30 
        THEN 'EspeciallyMentioned'
        ELSE 'Current'
    END AS assetClassificationCategory,
    
    CASE 
        WHEN c.CUST_TYPE = '1' THEN 'Households'
        WHEN c.CUST_TYPE = '2' THEN 'Non-Financial Corporations'
        WHEN c.CUST_TYPE = '3' THEN 'Financial Corporations'
        ELSE 'Non-Financial Corporations'
    END AS sectorSnaClassification

FROM TP_SO_COMMITMENT t
LEFT JOIN CUSTOMER c ON t.DR_ACC_CUSTOMER_ID = c.CUST_ID
LEFT JOIN COLLATERAL_TABLE ct ON t.DR_ACC_CUSTOMER_ID = ct.CUST_ID_1

WHERE 
    t.PAYMENT_AMOUNT > 0
    AND t.ISSUE_DATE >= '2023-01-01'
    AND (
        t.FK_SERVICE_PRODUCT = 9102 -- High-value third party (1.82M avg)
        OR (t.FK_SERVICE_PRODUCT = 9111 AND t.PAYMENT_AMOUNT >= 200000) -- Medium-value with threshold
        OR t.PAYMENT_AMOUNT >= 1000000 -- Any large amount
    )
    AND t.ENTRY_STATUS IN ('1', '2', '4') -- All valid statuses

ORDER BY totalValueShareUnderwritten DESC;