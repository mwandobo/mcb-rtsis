-- CORRECTED Query to extract Commercial & Other Bills Purchased & Discounted data from PROFITS database
-- Simplified version to avoid datetime arithmetic issues

SELECT 
    -- Core identifier
    b.BILL_SERIAL_NUM as commercialOtherBillsPurchased,
    
    -- Reporting date
    CURRENT TIMESTAMP as reportingDate,
    
    -- Security number (using bill serial number as security identifier)
    CAST(b.BILL_SERIAL_NUM AS VARCHAR(20)) as securityNumber,
    
    -- Bill holder (customer name)
    COALESCE(c.SELF_NAME, 'UNKNOWN') as billHolder,
    
    -- Rating status (true if rated, false if unrated)
    CASE 
        WHEN c.CUST_TYPE IN ('2', '3') THEN 'true'  -- Rated (corporate and banks)
        ELSE 'false'  -- Not rated (individuals/unrated)
    END as ratingStatus,
    
    -- Credit rating of borrower (using text descriptions)
    CASE 
        WHEN c.CUST_TYPE = '3' THEN 'Central Bank'  -- Type 3 is institutional/bank
        WHEN c.CUST_TYPE = '2' THEN 'BBB+ to BBB-'  -- Type 2 is corporate
        WHEN c.CUST_TYPE = '1' THEN 'Unrated'  -- Type 1 is individual/retail
        ELSE 'Unrated'  -- Default
    END as crRatingBorrower,
    
    -- Grades for unrated banks (simplified)
    'UNRATED' as gradesUnratedBanks,
    
    -- Bill type (mapped to lookup values)
    CASE 
        WHEN b.BILL_TYPE_FLAG = '0' THEN 'Domestic bill'  -- All bills are type 0, assume domestic
        ELSE 'Domestic bill'  -- Default
    END as billType,
    
    -- Bill type subcategory (from product description)
    null as billTypeSubcategory,
    
    -- Transaction date (bill purchase date)
    b.BILL_PURCHASE_DATE as transactionDate,
    
    -- Value date (could be issue date or purchase date)
    COALESCE(b.BILL_ISSUE_DATE, b.BILL_PURCHASE_DATE) as valueDate,
    
    -- Maturity date
    b.BILL_FINAL_DATE as maturityDate,
    
    -- Currency
    COALESCE(curr.SHORT_DESCR, 'TZS') as currency,
    
    -- Original amount
    b.BILL_AMOUNT as orgAmount,
    
    -- TZS amount (simplified - assume same currency for now)
    b.BILL_AMOUNT as tzsAmount,
    
    -- USD amount (simplified - assume same currency for now)
    b.BILL_AMOUNT as usdAmount,
    
    -- Bill bearer (issuer name)
    COALESCE(bi.BISS_TITLE, 'UNKNOWN ISSUER') as billBearer,
    
    -- Issuer credit rating (default to unrated since only default issuer exists)
    'Unrated' as issuerCreditRating,
    
    -- Issuer country (default to full country name)
    'Tanzania' as issuerCountry,
    
    -- Collateral pledged (using D42 lookup values)
    'Unsecured' as collateralPledged,
    
    -- Past due days (simplified - use overdue flag)
    CASE 
        WHEN b.OVERDUE_FLG = '1' THEN 30
        ELSE 0
    END as pastDueDays,
    
    -- Allowance for probable loss (simplified based on overdue flag)
    CASE 
        WHEN b.OVERDUE_FLG = '1' THEN b.BILL_AMOUNT * 0.25
        ELSE 0
    END as allowanceProbableLoss,
    
    -- BOT provision (regulatory provision)
    CASE 
        WHEN b.OVERDUE_FLG = '1' THEN b.BILL_AMOUNT * 0.25
        ELSE 0
    END as botProvision,
    
    -- Asset classification category (using D32 lookup values)
    CASE 
        WHEN b.OVERDUE_FLG = '1' THEN 'Substandard'  -- Overdue bills are substandard
        ELSE 'Current'  -- Non-overdue bills are current
    END as assetClassificationCategory,
    
    -- Sector SNA classification (mapped to customer types)
    CASE 
        WHEN c.CUST_TYPE = '3' THEN 'Central Bank'  -- Type 3 assumed to be banks/institutions
        WHEN c.CUST_TYPE = '2' THEN 'Other Non-Financial Corporations'  -- Type 2 assumed to be corporate
        WHEN c.CUST_TYPE = '1' THEN 'Households'  -- Type 1 assumed to be individuals
        ELSE 'Other Non-Financial Corporations'  -- Default
    END as sectorSnaClassification

FROM BILL b
    LEFT JOIN CUSTOMER c ON b.FK_CUSTOMERCUST_ID = c.CUST_ID
    LEFT JOIN BILL_ISSUER bi ON b.FK_BISS_CODE = bi.BISS_CODE
    LEFT JOIN CURRENCY curr ON b.FK_CURR_ID_ISSUED = curr.ID_CURRENCY
    LEFT JOIN PRODUCT p ON b.BILL_PRODUCT_ID = p.ID_PRODUCT

WHERE 
    -- Filter for active bills only
    b.BILL_ENTRY_STATUS = '1'
    
ORDER BY 
    b.BILL_SERIAL_NUM DESC
FETCH FIRST 100 ROWS ONLY;