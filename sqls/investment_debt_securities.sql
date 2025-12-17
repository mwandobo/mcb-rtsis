-- Investment Debt Securities for RTSIS Reporting - WORKING VERSION
-- Using both GLI_TRX_EXTRACT (130% GL accounts) and DEPOSIT_ACCOUNT (specific DEPOSIT_TYPE values)
-- Based on testing: 
-- - GLI_TRX_EXTRACT: 4 GL accounts with 130% pattern (Treasury bonds)
-- - DEPOSIT_ACCOUNT: 51,657 records with DEPOSIT_TYPE 1-5 (mostly Corporate bonds)
-- - Query performance optimized with subquery approach for GL accounts

-- OPTION 1: Government Bonds from GL Transaction Extract
SELECT
    -- Fixed timestamp for reporting
    CURRENT_TIMESTAMP AS reportingDate,
    
    -- Security identification (using GL account + customer combination)
    (gte.FK_GLG_ACCOUNTACCO || '-' || COALESCE(CAST(gte.CUST_ID AS VARCHAR(10)), '0')) AS securityNumber,
    
    -- Security type classification - Treasury bonds (130% GL accounts)
    'Treasury bonds' AS securityType,
    
    -- Issuer information - Government of Tanzania for 130% GL accounts
    'Government of Tanzania' AS securityIssuerName,
    
    -- External rating - AAA for Government securities
    'AAA' AS externalIssuerRatting,
    
    -- Grades for unrated banks - NULL for Government securities
    NULL AS gradesUnratedBanks,
    
    -- Issuer country - Tanzania for Government securities
    'Tanzania' AS securityIssuerCountry,
    
    -- SNA sector classification - Central Government
    'Central Government' AS snaIssuerSector,
    
    -- Currency
    COALESCE(gte.CURRENCY_SHORT_DES, 'TZS') AS currency,
    
    -- Cost value amounts (using DC_AMOUNT as cost basis)
    gte.DC_AMOUNT AS orgCostValueAmount,
    
    -- TZS cost value
    CASE 
        WHEN gte.CURRENCY_SHORT_DES = 'USD' 
            THEN gte.DC_AMOUNT * 2730.50
        WHEN gte.CURRENCY_SHORT_DES = 'EUR'
            THEN gte.DC_AMOUNT * 2950.00
        ELSE gte.DC_AMOUNT
    END AS tzsCostValueAmount,
    
    -- USD cost value
    CASE 
        WHEN gte.CURRENCY_SHORT_DES = 'USD' 
            THEN gte.DC_AMOUNT
        WHEN gte.CURRENCY_SHORT_DES = 'TZS'
            THEN gte.DC_AMOUNT / 2730.50
        WHEN gte.CURRENCY_SHORT_DES = 'EUR'
            THEN gte.DC_AMOUNT * 1.08
        ELSE NULL
    END AS usdCostValueAmount,
    
    -- Face value amounts (same as cost for GL account approach)
    gte.DC_AMOUNT AS orgFaceValueAmount,
    
    -- TZS face value
    CASE 
        WHEN gte.CURRENCY_SHORT_DES = 'USD' 
            THEN gte.DC_AMOUNT * 2730.50
        WHEN gte.CURRENCY_SHORT_DES = 'EUR'
            THEN gte.DC_AMOUNT * 2950.00
        ELSE gte.DC_AMOUNT
    END AS tzsgFaceValueAmount,
    
    -- USD face value
    CASE 
        WHEN gte.CURRENCY_SHORT_DES = 'USD' 
            THEN gte.DC_AMOUNT
        WHEN gte.CURRENCY_SHORT_DES = 'TZS'
            THEN gte.DC_AMOUNT / 2730.50
        WHEN gte.CURRENCY_SHORT_DES = 'EUR'
            THEN gte.DC_AMOUNT * 1.08
        ELSE NULL
    END AS usdgFaceValueAmount,
    
    -- Fair value amounts (same as cost for GL account approach)
    gte.DC_AMOUNT AS orgFairValueAmount,
    
    -- TZS fair value
    CASE 
        WHEN gte.CURRENCY_SHORT_DES = 'USD' 
            THEN gte.DC_AMOUNT * 2730.50
        WHEN gte.CURRENCY_SHORT_DES = 'EUR'
            THEN gte.DC_AMOUNT * 2950.00
        ELSE gte.DC_AMOUNT
    END AS tzsgFairValueAmount,
    
    -- USD fair value
    CASE 
        WHEN gte.CURRENCY_SHORT_DES = 'USD' 
            THEN gte.DC_AMOUNT
        WHEN gte.CURRENCY_SHORT_DES = 'TZS'
            THEN gte.DC_AMOUNT / 2730.50
        WHEN gte.CURRENCY_SHORT_DES = 'EUR'
            THEN gte.DC_AMOUNT * 1.08
        ELSE NULL
    END AS usdgFairValueAmount,
    
    -- Interest rate (placeholder - no bond master data available)
    CAST(0 AS DECIMAL(9, 6)) AS interestRate,
    
    -- Purchase date (using transaction date)
    gte.TRN_DATE AS purchaseDate,
    
    -- Value date (using availability date)
    gte.AVAILABILITY_DATE AS valueDate,
    
    -- Maturity date (using availability date as proxy)
    gte.AVAILABILITY_DATE AS maturityDate,
    
    -- Trading intent - Government securities typically held to maturity
    'Hold to Maturity' AS tradingIntent,
    
    -- Security encumbrance status (placeholder)
    'Unencumbered' AS securityEncumbaranceStatus,
    
    -- Past due days (for securities past maturity)
    CASE 
        WHEN gte.AVAILABILITY_DATE IS NOT NULL AND gte.AVAILABILITY_DATE < CURRENT_DATE
            THEN DAYS(CURRENT_DATE) - DAYS(gte.AVAILABILITY_DATE)
        ELSE 0
    END AS pastDueDays,
    
    -- Allowance for probable loss (placeholder)
    CAST(0 AS DECIMAL(15, 2)) AS allowanceProbableLoss,
    
    -- Asset classification category
    CASE 
        WHEN gte.AVAILABILITY_DATE IS NULL OR gte.AVAILABILITY_DATE >= CURRENT_DATE
            THEN 1  -- Normal
        WHEN DAYS(CURRENT_DATE) - DAYS(gte.AVAILABILITY_DATE) <= 90
            THEN 2  -- Watch
        WHEN DAYS(CURRENT_DATE) - DAYS(gte.AVAILABILITY_DATE) <= 180
            THEN 3  -- Substandard
        WHEN DAYS(CURRENT_DATE) - DAYS(gte.AVAILABILITY_DATE) <= 365
            THEN 4  -- Doubtful
        ELSE 5  -- Loss
    END AS assetClassificationCategory

FROM GLI_TRX_EXTRACT gte

-- Join with GL account for account details
LEFT JOIN GLG_ACCOUNT gl 
    ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID

-- Join with customer information
LEFT JOIN CUSTOMER c 
    ON gte.CUST_ID = c.CUST_ID

WHERE 
    -- Only Government Bond GL accounts (130% pattern - optimized with subquery)
    gte.FK_GLG_ACCOUNTACCO IN (
        SELECT ACCOUNT_ID 
        FROM GLG_ACCOUNT 
        WHERE EXTERNAL_GLACCOUNT LIKE '130%'
    )
    -- Only debit balances (assets)
    AND gte.DC_AMOUNT IS NOT NULL
    AND gte.DC_AMOUNT > 0
    -- Only active records
    AND gte.TRN_DATE IS NOT NULL

UNION ALL

-- OPTION 2: Investment Securities from Deposit Accounts (DEPOSIT_TYPE 2, 4, 5)
SELECT
    -- Fixed timestamp for reporting
    CURRENT_TIMESTAMP AS reportingDate,
    
    -- Security identification (using account number)
    CAST(da.ACCOUNT_NUMBER AS VARCHAR(50)) AS securityNumber,
    
    -- Security type classification based on deposit type (mapped to RTSIS codes)
    CASE 
        WHEN da.DEPOSIT_TYPE = '1' THEN 'Corporate bonds'           -- Code 1
        WHEN da.DEPOSIT_TYPE = '2' THEN 'Treasury bonds'            -- Code 2  
        WHEN da.DEPOSIT_TYPE = '3' THEN 'Treasury bills'            -- Code 3
        WHEN da.DEPOSIT_TYPE = '4' THEN 'RGOZ Treasury bond'        -- Code 4
        WHEN da.DEPOSIT_TYPE = '5' THEN 'Municipal/Local Government bond'  -- Code 5
        ELSE 'Others investments (Specify name)'                   -- Code 16
    END AS securityType,
    
    -- Issuer information based on deposit type
    CASE 
        WHEN da.DEPOSIT_TYPE = '2' THEN 'Government of Tanzania'     -- Treasury bonds
        WHEN da.DEPOSIT_TYPE = '3' THEN 'Bank of Tanzania'           -- Treasury bills
        WHEN da.DEPOSIT_TYPE = '4' THEN 'Government of Tanzania'     -- RGOZ Treasury bond
        WHEN da.DEPOSIT_TYPE = '5' THEN 'Local Government Authority' -- Municipal bonds
        WHEN da.DEPOSIT_TYPE = '1' AND c.CUST_ID IS NOT NULL AND c.CUST_TYPE = 'C'
            THEN TRIM(COALESCE(c.FIRST_NAME, '') || ' ' || COALESCE(c.MIDDLE_NAME, '') || ' ' || COALESCE(c.SURNAME, ''))
        WHEN da.DEPOSIT_TYPE = '1' AND c.CUST_ID IS NOT NULL
            THEN COALESCE(c.SURNAME, 'Unknown Corporate')
        ELSE 'Unknown Issuer'
    END AS securityIssuerName,
    
    -- External rating based on deposit type
    CASE 
        WHEN da.DEPOSIT_TYPE IN ('2', '3', '4') THEN 'AAA'  -- Government/Treasury securities
        WHEN da.DEPOSIT_TYPE = '5' THEN 'A'                 -- Municipal bonds
        WHEN UPPER(COALESCE(c.SURNAME, '')) LIKE '%BANK%' THEN 'A'
        ELSE NULL
    END AS externalIssuerRatting,
    
    -- Grades for unrated banks
    CASE 
        WHEN UPPER(COALESCE(c.SURNAME, '')) LIKE '%BANK%' 
             AND da.DEPOSIT_TYPE = '1'  -- Only for corporate bonds
        THEN 'Grade 2'
        ELSE NULL
    END AS gradesUnratedBanks,
    
    -- Issuer country
    'Tanzania' AS securityIssuerCountry,
    
    -- SNA sector classification
    CASE 
        WHEN da.DEPOSIT_TYPE IN ('2', '3', '4') THEN 'Central Government'  -- Government securities
        WHEN da.DEPOSIT_TYPE = '5' THEN 'Local Government'                 -- Municipal bonds
        WHEN da.DEPOSIT_TYPE = '1' AND UPPER(COALESCE(c.SURNAME, '')) LIKE '%BANK%'
            THEN 'Other Depository Corporations'
        WHEN da.DEPOSIT_TYPE = '1' AND UPPER(COALESCE(c.SURNAME, '')) LIKE '%INSURANCE%'
            THEN 'Insurance Companies'
        WHEN da.DEPOSIT_TYPE = '1' AND c.CUST_ID IS NOT NULL AND c.CUST_TYPE = 'C'
            THEN 'Households'
        ELSE 'Other Non-Financial Corporations'
    END AS snaIssuerSector,
    
    -- Currency
    COALESCE(cur.SHORT_DESCR, 'TZS') AS currency,
    
    -- Cost value amounts (using opening balance as cost basis)
    COALESCE(da.OPENING_BALANCE, da.BOOK_BALANCE, 0) AS orgCostValueAmount,
    
    -- TZS cost value
    CASE 
        WHEN cur.SHORT_DESCR = 'USD' 
            THEN COALESCE(da.OPENING_BALANCE, da.BOOK_BALANCE, 0) * 2730.50
        WHEN cur.SHORT_DESCR = 'EUR'
            THEN COALESCE(da.OPENING_BALANCE, da.BOOK_BALANCE, 0) * 2950.00
        ELSE COALESCE(da.OPENING_BALANCE, da.BOOK_BALANCE, 0)
    END AS tzsCostValueAmount,
    
    -- USD cost value
    CASE 
        WHEN cur.SHORT_DESCR = 'USD' 
            THEN COALESCE(da.OPENING_BALANCE, da.BOOK_BALANCE, 0)
        WHEN cur.SHORT_DESCR = 'TZS'
            THEN COALESCE(da.OPENING_BALANCE, da.BOOK_BALANCE, 0) / 2730.50
        WHEN cur.SHORT_DESCR = 'EUR'
            THEN COALESCE(da.OPENING_BALANCE, da.BOOK_BALANCE, 0) * 1.08
        ELSE NULL
    END AS usdCostValueAmount,
    
    -- Face value amounts (using book balance as face value)
    COALESCE(da.BOOK_BALANCE, 0) AS orgFaceValueAmount,
    
    -- TZS face value
    CASE 
        WHEN cur.SHORT_DESCR = 'USD' 
            THEN COALESCE(da.BOOK_BALANCE, 0) * 2730.50
        WHEN cur.SHORT_DESCR = 'EUR'
            THEN COALESCE(da.BOOK_BALANCE, 0) * 2950.00
        ELSE COALESCE(da.BOOK_BALANCE, 0)
    END AS tzsgFaceValueAmount,
    
    -- USD face value
    CASE 
        WHEN cur.SHORT_DESCR = 'USD' 
            THEN COALESCE(da.BOOK_BALANCE, 0)
        WHEN cur.SHORT_DESCR = 'TZS'
            THEN COALESCE(da.BOOK_BALANCE, 0) / 2730.50
        WHEN cur.SHORT_DESCR = 'EUR'
            THEN COALESCE(da.BOOK_BALANCE, 0) * 1.08
        ELSE NULL
    END AS usdgFaceValueAmount,
    
    -- Fair value amounts (using available balance as fair value)
    COALESCE(da.AVAILABLE_BALANCE, da.BOOK_BALANCE, 0) AS orgFairValueAmount,
    
    -- TZS fair value
    CASE 
        WHEN cur.SHORT_DESCR = 'USD' 
            THEN COALESCE(da.AVAILABLE_BALANCE, da.BOOK_BALANCE, 0) * 2730.50
        WHEN cur.SHORT_DESCR = 'EUR'
            THEN COALESCE(da.AVAILABLE_BALANCE, da.BOOK_BALANCE, 0) * 2950.00
        ELSE COALESCE(da.AVAILABLE_BALANCE, da.BOOK_BALANCE, 0)
    END AS tzsgFairValueAmount,
    
    -- USD fair value
    CASE 
        WHEN cur.SHORT_DESCR = 'USD' 
            THEN COALESCE(da.AVAILABLE_BALANCE, da.BOOK_BALANCE, 0)
        WHEN cur.SHORT_DESCR = 'TZS'
            THEN COALESCE(da.AVAILABLE_BALANCE, da.BOOK_BALANCE, 0) / 2730.50
        WHEN cur.SHORT_DESCR = 'EUR'
            THEN COALESCE(da.AVAILABLE_BALANCE, da.BOOK_BALANCE, 0) * 1.08
        ELSE NULL
    END AS usdgFairValueAmount,
    
    -- Interest rate (using fixed interest rate from deposit account)
    COALESCE(da.FIXED_INTER_RATE, 0) AS interestRate, 
    
    -- Purchase date (using opening date)
    da.OPENING_DATE AS purchaseDate,
    
    -- Value date (using start date)
    COALESCE(da.START_DATE_TD, da.OPENING_DATE) AS valueDate,
    
    -- Maturity date (using expiry date)
    COALESCE(da.EXPIRY_DATE_TD, da.EXPIRY_DATE) AS maturityDate,
    
    -- Trading intent based on deposit type
    CASE 
        WHEN da.DEPOSIT_TYPE IN ('2', '3', '4') THEN 'Hold to Maturity'  -- Government securities
        WHEN da.DEPOSIT_TYPE = '5' THEN 'Hold to Maturity'              -- Municipal bonds
        ELSE 'Available for Sale'  -- Corporate bonds
    END AS tradingIntent,
    
    -- Security encumbrance status based on collateral flag (using numeric values)
    CASE 
        WHEN da.COLLATERAL_FLG = '1' THEN 'Encumbered'
        ELSE 'Unencumbered'
    END AS securityEncumbaranceStatus,
    
    -- Past due days (for securities past maturity)
    CASE 
        WHEN COALESCE(da.EXPIRY_DATE_TD, da.EXPIRY_DATE) IS NOT NULL 
             AND COALESCE(da.EXPIRY_DATE_TD, da.EXPIRY_DATE) < CURRENT_DATE
            THEN DAYS(CURRENT_DATE) - DAYS(COALESCE(da.EXPIRY_DATE_TD, da.EXPIRY_DATE))
        ELSE 0
    END AS pastDueDays,
    
    -- Allowance for probable loss (placeholder)
    CAST(0 AS DECIMAL(15, 2)) AS allowanceProbableLoss,
    
    -- Asset classification category based on maturity and status
    CASE 
        WHEN da.ENTRY_STATUS NOT IN ('1', '6') THEN 5  -- Loss if not active
        WHEN COALESCE(da.EXPIRY_DATE_TD, da.EXPIRY_DATE) IS NULL 
             OR COALESCE(da.EXPIRY_DATE_TD, da.EXPIRY_DATE) >= CURRENT_DATE
            THEN 1  -- Normal
        WHEN DAYS(CURRENT_DATE) - DAYS(COALESCE(da.EXPIRY_DATE_TD, da.EXPIRY_DATE)) <= 90
            THEN 2  -- Watch
        WHEN DAYS(CURRENT_DATE) - DAYS(COALESCE(da.EXPIRY_DATE_TD, da.EXPIRY_DATE)) <= 180
            THEN 3  -- Substandard
        WHEN DAYS(CURRENT_DATE) - DAYS(COALESCE(da.EXPIRY_DATE_TD, da.EXPIRY_DATE)) <= 365
            THEN 4  -- Doubtful
        ELSE 5  -- Loss
    END AS assetClassificationCategory

FROM DEPOSIT_ACCOUNT da

-- Join with customer information
LEFT JOIN CUSTOMER c 
    ON da.FK_CUSTOMERCUST_ID = c.CUST_ID

-- Join with currency information
LEFT JOIN CURRENCY cur 
    ON da.FK_CURRENCYID_CURR = cur.ID_CURRENCY

WHERE 
    -- Only investment securities (DEPOSIT_TYPE 1-5 for debt securities)
    da.DEPOSIT_TYPE IN ('1', '2', '3', '4', '5')
    -- Only active accounts (status 1 and 6 appear to be active based on counts)
    AND da.ENTRY_STATUS IN ('1', '6')
    -- Only accounts with balances
    AND (da.BOOK_BALANCE > 0 OR da.AVAILABLE_BALANCE > 0 OR da.OPENING_BALANCE > 0)

ORDER BY 
    securityType, securityNumber;