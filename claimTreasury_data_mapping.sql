-- claimTreasury Data Extraction Query
-- Based on TREASURY_MM_DEAL table which contains actual treasury data
-- Following the same structure as loan_information.sql, overdraft.sql, etc.

SELECT 
    -- Reporting Date: Format as DDMMYYYYHHMM as per BOT requirement
    VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHH24MI') AS reportingDate,
    
    -- Transaction Date: When the treasury deal was created
    VARCHAR_FORMAT(tmd.DEAL_DATE, 'DDMMYYYYHH24MI') AS transactionDate,
    
    -- Government Institution Name: Extract from customer or deal reference
    CASE 
        WHEN UPPER(COALESCE(c.SURNAME, '')) LIKE '%MINISTRY%' THEN LTRIM(RTRIM(c.SURNAME))
        WHEN UPPER(COALESCE(c.SURNAME, '')) LIKE '%TREASURY%' THEN LTRIM(RTRIM(c.SURNAME))
        WHEN UPPER(COALESCE(c.SURNAME, '')) LIKE '%GOVERNMENT%' THEN LTRIM(RTRIM(c.SURNAME))
        WHEN UPPER(COALESCE(c.SURNAME, '')) LIKE '%COUNCIL%' THEN LTRIM(RTRIM(c.SURNAME))
        WHEN UPPER(COALESCE(c.FIRST_NAME, '')) LIKE '%MINISTRY%' THEN LTRIM(RTRIM(c.FIRST_NAME))
        WHEN UPPER(COALESCE(c.FIRST_NAME, '')) LIKE '%TREASURY%' THEN LTRIM(RTRIM(c.FIRST_NAME))
        WHEN UPPER(COALESCE(c.FIRST_NAME, '')) LIKE '%GOVERNMENT%' THEN LTRIM(RTRIM(c.FIRST_NAME))
        WHEN UPPER(COALESCE(c.FIRST_NAME, '')) LIKE '%COUNCIL%' THEN LTRIM(RTRIM(c.FIRST_NAME))
        WHEN UPPER(COALESCE(tmd.DEAL_REF_NO, '')) LIKE '%TREASURY%' THEN 'Treasury Department'
        WHEN UPPER(COALESCE(tmd.DEAL_REF_NO, '')) LIKE '%GOVERNMENT%' THEN 'Government Institution'
        WHEN UPPER(COALESCE(tmd.BOND_CODE, '')) LIKE '%TREASURY%' THEN 'Treasury Department'
        WHEN UPPER(COALESCE(tmd.BOND_CODE, '')) LIKE '%GOVT%' THEN 'Government Institution'
        ELSE COALESCE(LTRIM(RTRIM(c.SURNAME)), LTRIM(RTRIM(c.FIRST_NAME)), 'Government Institution')
    END AS govInstitutionName,
    
    -- Currency: From currency lookup (following existing pattern)
    COALESCE(cu.SHORT_DESCR, 'TZS') AS currency,
    
    -- Original Amount Claimed: Source amount from treasury deal
    COALESCE(tmd.SOURCE_AMOUNT, 0) AS orgAmountClaimed,
    
    -- USD Amount Claimed: Convert to USD using same pattern as other queries
    CASE 
        WHEN cu.SHORT_DESCR = 'USD' THEN COALESCE(tmd.SOURCE_AMOUNT, 0)
        ELSE COALESCE(tmd.SOURCE_AMOUNT, 0) / 2500.00
    END AS usdAmountClaimed,
    
    -- TZS Amount Claimed: Convert to TZS using same pattern as other queries
    CASE 
        WHEN cu.SHORT_DESCR = 'TZS' THEN COALESCE(tmd.SOURCE_AMOUNT, 0)
        WHEN cu.SHORT_DESCR = 'USD' THEN COALESCE(tmd.SOURCE_AMOUNT, 0) * 2500.00
        ELSE COALESCE(tmd.SOURCE_AMOUNT, 0)
    END AS tzsAmountClaimed,
    
    -- Value Date: When the claim is due (deal value date)
    VARCHAR_FORMAT(tmd.VALUE_DATE, 'DDMMYYYYHH24MI') AS valueDate,
    
    -- Maturity Date: Treasury deal maturity date
    VARCHAR_FORMAT(tmd.MATURITY_DATE, 'DDMMYYYYHH24MI') AS maturityDate,
    
    -- Past Due Days: Calculate days overdue (following existing pattern)
    CASE 
        WHEN tmd.MATURITY_DATE < CURRENT_DATE THEN 
            DAYS(CURRENT_DATE) - DAYS(tmd.MATURITY_DATE)
        ELSE 0
    END AS pastDueDays,
    
    -- Allowance for Probable Loss: IFRS impairment calculation (following existing pattern)
    CASE 
        WHEN DAYS(CURRENT_DATE) - DAYS(tmd.MATURITY_DATE) > 365 THEN
            -- Loss: 100% provision
            COALESCE(tmd.SOURCE_AMOUNT, 0) * 1.00
        WHEN DAYS(CURRENT_DATE) - DAYS(tmd.MATURITY_DATE) > 180 THEN
            -- Doubtful: 50% provision
            COALESCE(tmd.SOURCE_AMOUNT, 0) * 0.50
        WHEN DAYS(CURRENT_DATE) - DAYS(tmd.MATURITY_DATE) > 90 THEN
            -- Substandard: 25% provision
            COALESCE(tmd.SOURCE_AMOUNT, 0) * 0.25
        WHEN DAYS(CURRENT_DATE) - DAYS(tmd.MATURITY_DATE) > 30 THEN
            -- Watch: 5% provision
            COALESCE(tmd.SOURCE_AMOUNT, 0) * 0.05
        ELSE
            -- Current: 1% provision
            COALESCE(tmd.SOURCE_AMOUNT, 0) * 0.01
    END AS allowanceProbableLoss,
    
    -- BOT Provision: Regulatory provision as per BOT requirements (following existing pattern)
    CASE 
        WHEN DAYS(CURRENT_DATE) - DAYS(tmd.MATURITY_DATE) > 365 THEN
            -- Loss: 100% provision
            COALESCE(tmd.SOURCE_AMOUNT, 0) * 1.00
        WHEN DAYS(CURRENT_DATE) - DAYS(tmd.MATURITY_DATE) > 180 THEN
            -- Doubtful: 50% provision
            COALESCE(tmd.SOURCE_AMOUNT, 0) * 0.50
        WHEN DAYS(CURRENT_DATE) - DAYS(tmd.MATURITY_DATE) > 90 THEN
            -- Substandard: 25% provision
            COALESCE(tmd.SOURCE_AMOUNT, 0) * 0.25
        WHEN DAYS(CURRENT_DATE) - DAYS(tmd.MATURITY_DATE) > 30 THEN
            -- Watch: 5% provision
            COALESCE(tmd.SOURCE_AMOUNT, 0) * 0.05
        ELSE
            -- Current: 2% provision (BOT requirement)
            COALESCE(tmd.SOURCE_AMOUNT, 0) * 0.02
    END AS botProvision,
    
    -- Asset Classification Category: Based on past due days (D32 lookup values)
    CASE 
        WHEN DAYS(CURRENT_DATE) - DAYS(tmd.MATURITY_DATE) > 365 THEN 'Loss'
        WHEN DAYS(CURRENT_DATE) - DAYS(tmd.MATURITY_DATE) > 180 THEN 'Doubtful'
        WHEN DAYS(CURRENT_DATE) - DAYS(tmd.MATURITY_DATE) > 90 THEN 'Substandard'
        WHEN DAYS(CURRENT_DATE) - DAYS(tmd.MATURITY_DATE) > 30 THEN 'EspeciallyMentioned'
        ELSE 'Current'
    END AS assetClassificationCategory,
    
    -- Sector SNA Classification: Government sector classification (using official SNA codes)
    CASE 
        -- Central Government (Code 3): Ministries, Treasury, Central Government Agencies
        WHEN UPPER(COALESCE(c.SURNAME, '')) LIKE '%MINISTRY%' OR UPPER(COALESCE(c.FIRST_NAME, '')) LIKE '%MINISTRY%' THEN 'Central Governments'
        WHEN UPPER(COALESCE(c.SURNAME, '')) LIKE '%TREASURY%' OR UPPER(COALESCE(c.FIRST_NAME, '')) LIKE '%TREASURY%' THEN 'Central Governments'
        WHEN UPPER(COALESCE(c.SURNAME, '')) LIKE '%GOVERNMENT%' OR UPPER(COALESCE(c.FIRST_NAME, '')) LIKE '%GOVERNMENT%' THEN 'Central Governments'
        WHEN UPPER(COALESCE(tmd.DEAL_REF_NO, '')) LIKE '%TREASURY%' OR UPPER(COALESCE(tmd.BOND_CODE, '')) LIKE '%TREASURY%' THEN 'Central Governments'
        WHEN UPPER(COALESCE(tmd.DEAL_REF_NO, '')) LIKE '%GOVERNMENT%' OR UPPER(COALESCE(tmd.BOND_CODE, '')) LIKE '%GOVT%' THEN 'Central Governments'
        
        -- Local Government (Code 4): Municipal Councils, District Councils, Local Authorities
        WHEN UPPER(COALESCE(c.SURNAME, '')) LIKE '%COUNCIL%' OR UPPER(COALESCE(c.FIRST_NAME, '')) LIKE '%COUNCIL%' THEN 'Local Governments'
        WHEN UPPER(COALESCE(c.SURNAME, '')) LIKE '%MUNICIPAL%' OR UPPER(COALESCE(c.FIRST_NAME, '')) LIKE '%MUNICIPAL%' THEN 'Local Governments'
        WHEN UPPER(COALESCE(c.SURNAME, '')) LIKE '%DISTRICT%' OR UPPER(COALESCE(c.FIRST_NAME, '')) LIKE '%DISTRICT%' THEN 'Local Governments'
        
        -- General Government (Code 2): Other government entities
        WHEN UPPER(COALESCE(c.SURNAME, '')) LIKE '%AUTHORITY%' OR UPPER(COALESCE(c.FIRST_NAME, '')) LIKE '%AUTHORITY%' THEN 'General government'
        
        -- Public Non-Financial Corporations (Code 6): Government-owned enterprises, parastatals
        WHEN UPPER(COALESCE(c.SURNAME, '')) LIKE '%CORPORATION%' OR UPPER(COALESCE(c.FIRST_NAME, '')) LIKE '%CORPORATION%' THEN 'Public Non-Financial Corporations'
        WHEN UPPER(COALESCE(c.SURNAME, '')) LIKE '%PARASTATAL%' OR UPPER(COALESCE(c.FIRST_NAME, '')) LIKE '%PARASTATAL%' THEN 'Public Non-Financial Corporations'
        
        -- Default to Central Governments for treasury deals
        ELSE 'Central Governments'
    END AS sectorSnaClassification

FROM TREASURY_MM_DEAL tmd
    LEFT JOIN CUSTOMER c ON c.CUST_ID = tmd.FK_CORRESP_CUST
    LEFT JOIN CURRENCY cu ON cu.ID_CURRENCY = tmd.FK_SOURCE_CURRENCY

WHERE 
    -- Filter for government-related treasury deals
    (
        -- Customer-based filtering
        UPPER(COALESCE(c.SURNAME, '')) LIKE '%GOVERNMENT%' OR
        UPPER(COALESCE(c.SURNAME, '')) LIKE '%MINISTRY%' OR
        UPPER(COALESCE(c.SURNAME, '')) LIKE '%TREASURY%' OR
        UPPER(COALESCE(c.SURNAME, '')) LIKE '%COUNCIL%' OR
        UPPER(COALESCE(c.FIRST_NAME, '')) LIKE '%GOVERNMENT%' OR
        UPPER(COALESCE(c.FIRST_NAME, '')) LIKE '%MINISTRY%' OR
        UPPER(COALESCE(c.FIRST_NAME, '')) LIKE '%TREASURY%' OR
        UPPER(COALESCE(c.FIRST_NAME, '')) LIKE '%COUNCIL%' OR
        
        -- Deal reference-based filtering
        UPPER(COALESCE(tmd.DEAL_REF_NO, '')) LIKE '%TREASURY%' OR
        UPPER(COALESCE(tmd.DEAL_REF_NO, '')) LIKE '%GOVERNMENT%' OR
        UPPER(COALESCE(tmd.DEAL_REF_NO, '')) LIKE '%MINISTRY%' OR
        
        -- Bond code-based filtering
        UPPER(COALESCE(tmd.BOND_CODE, '')) LIKE '%TREASURY%' OR
        UPPER(COALESCE(tmd.BOND_CODE, '')) LIKE '%GOVT%' OR
        UPPER(COALESCE(tmd.BOND_CODE, '')) LIKE '%GOV%'
    )
    -- Only active deals with amounts
    AND tmd.STATUS = '1'  -- Active status (assuming 1 = active)
    AND COALESCE(tmd.SOURCE_AMOUNT, 0) > 0
    -- Filter for current reporting period
    AND tmd.DEAL_DATE >= CURRENT_DATE - 2 YEARS  -- Reasonable time range for treasury deals
    -- Only deals that haven't been fully settled
    AND (tmd.MATURE_FLAG IS NULL OR tmd.MATURE_FLAG != '1')

ORDER BY govInstitutionName, orgAmountClaimed DESC;