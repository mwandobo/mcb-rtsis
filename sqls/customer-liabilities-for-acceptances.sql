-- Customer's Liabilities for Acceptances RTSIS Report
-- Based on RTSIS requirements for acceptance liability reporting
-- Created for MCB Bank Tanzania
-- Date: December 18, 2025
-- Simplified version based on actual PROFITS database structure

SELECT
    /* =========================
       REPORTING INFORMATION
       ========================= */

    -- Reporting Date and Time (DDMMYYYYHHMM format)
    VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHH24MI')     AS reportingDate,

    /* =========================
       DRAFT HOLDER INFORMATION
       ========================= */

    -- Draft Holder (Customer holding the draft)
    COALESCE(
            TRIM(c.FIRST_NAME || ' ' || c.SURNAME),
            c.SELF_NAME,
            'UNKNOWN CUSTOMER'
    )                                                        AS draftHolder,

    /* =========================
       TRANSACTION DATES
       ========================= */

    -- Transaction Date (when the transaction occurred)
    VARCHAR_FORMAT(b.BILL_PURCHASE_DATE, 'DDMMYYYYHH24MI')  AS transactionDate,

    -- Value Date (when funds are available for withdrawal)
    VARCHAR_FORMAT(
            COALESCE(b.BILL_CRACC_AVLDT, b.BILL_ISSUE_DATE, b.BILL_PURCHASE_DATE),
            'DDMMYYYYHH24MI'
    )                                                        AS valueDate,

    -- Maturity Date (expiry date of the instrument)
    VARCHAR_FORMAT(b.BILL_FINAL_DATE, 'DDMMYYYYHH24MI')     AS maturityDate,

    /* =========================
       CURRENCY AND AMOUNTS
       ========================= */

    -- Currency
    COALESCE(curr.SHORT_DESCR, 'TZS')                       AS currency,

    -- Original Currency Amount
    b.BILL_AMOUNT                                            AS orgAmount,

    -- USD equivalent Amount
    CASE
        WHEN COALESCE(curr.SHORT_DESCR, 'TZS') = 'USD'
            THEN b.BILL_AMOUNT
        WHEN COALESCE(curr.SHORT_DESCR, 'TZS') = 'TZS'
            THEN b.BILL_AMOUNT / 2730.50
        WHEN COALESCE(curr.SHORT_DESCR, 'TZS') = 'EUR'
            THEN b.BILL_AMOUNT * 1.08
        ELSE b.BILL_AMOUNT / 2730.50
        END                                                      AS usdAmount,

    -- TZS Amount
    CASE
        WHEN COALESCE(curr.SHORT_DESCR, 'TZS') = 'USD'
            THEN b.BILL_AMOUNT * 2730.50
        WHEN COALESCE(curr.SHORT_DESCR, 'TZS') = 'EUR'
            THEN b.BILL_AMOUNT * 2950.00
        ELSE b.BILL_AMOUNT
        END                                                      AS tzsAmount,

    /* =========================
       COLLATERAL INFORMATION
       ========================= */

    -- Collateral (Y/N)
    CASE
        WHEN ct.INTERNAL_SN IS NOT NULL THEN 'Y'
        ELSE 'N'
        END                                                      AS collateral,

    -- Collateral Pledged (lookup value from D42)
    COALESCE(
            gd_coll.LATIN_DESC,
            CASE
                WHEN ct.RECORD_TYPE = '01' THEN 'Real Estate'
                WHEN ct.RECORD_TYPE = '02' THEN 'Vehicle'
                WHEN ct.RECORD_TYPE = '03' THEN 'Cash Deposit'
                WHEN ct.RECORD_TYPE = '04' THEN 'Guarantee'
                WHEN ct.RECORD_TYPE = '05' THEN 'Securities'
                ELSE 'Other'
                END,
            'Unsecured'
    )                                                        AS collateralPledged,

    /* =========================
       OVERDUE AND CLASSIFICATION
       ========================= */

    -- Past Due Days
    CASE
        WHEN b.OVERDUE_FLG = '1' AND b.BILL_FINAL_DATE IS NOT NULL AND b.BILL_FINAL_DATE < CURRENT_DATE
            THEN DAYS(CURRENT_DATE) - DAYS(b.BILL_FINAL_DATE)
        WHEN b.BILL_FINAL_DATE IS NOT NULL AND b.BILL_FINAL_DATE < CURRENT_DATE
            THEN DAYS(CURRENT_DATE) - DAYS(b.BILL_FINAL_DATE)
        ELSE 0
        END                                                      AS pastDueDays,

    /* =========================
       PROVISIONS AND CLASSIFICATION
       ========================= */

    -- Allowance for Probable Loss (IFRS provision)
    CASE
        WHEN b.OVERDUE_FLG = '1' THEN b.BILL_AMOUNT * 0.25  -- 25% for overdue
        WHEN b.BILL_FINAL_DATE IS NULL OR b.BILL_FINAL_DATE >= CURRENT_DATE
            THEN b.BILL_AMOUNT * 0.01  -- 1% for current
        WHEN DAYS(CURRENT_DATE) - DAYS(b.BILL_FINAL_DATE) <= 30
            THEN b.BILL_AMOUNT * 0.01  -- 1% for 0-30 days
        WHEN DAYS(CURRENT_DATE) - DAYS(b.BILL_FINAL_DATE) <= 90
            THEN b.BILL_AMOUNT * 0.05  -- 5% for 31-90 days
        WHEN DAYS(CURRENT_DATE) - DAYS(b.BILL_FINAL_DATE) <= 180
            THEN b.BILL_AMOUNT * 0.20  -- 20% for 91-180 days
        WHEN DAYS(CURRENT_DATE) - DAYS(b.BILL_FINAL_DATE) <= 365
            THEN b.BILL_AMOUNT * 0.50  -- 50% for 181-365 days
        ELSE b.BILL_AMOUNT * 1.00     -- 100% for over 365 days
        END                                                      AS allowanceProbableLoss,

    -- BOT Provision (regulatory provision)
    CASE
        WHEN b.OVERDUE_FLG = '1' THEN b.BILL_AMOUNT * 0.25  -- 25% for overdue
        WHEN b.BILL_FINAL_DATE IS NULL OR b.BILL_FINAL_DATE >= CURRENT_DATE
            THEN b.BILL_AMOUNT * 0.01  -- 1% for current
        WHEN DAYS(CURRENT_DATE) - DAYS(b.BILL_FINAL_DATE) <= 30
            THEN b.BILL_AMOUNT * 0.01  -- 1% for 0-30 days
        WHEN DAYS(CURRENT_DATE) - DAYS(b.BILL_FINAL_DATE) <= 90
            THEN b.BILL_AMOUNT * 0.05  -- 5% for 31-90 days
        WHEN DAYS(CURRENT_DATE) - DAYS(b.BILL_FINAL_DATE) <= 180
            THEN b.BILL_AMOUNT * 0.25  -- 25% for 91-180 days (BOT higher rate)
        WHEN DAYS(CURRENT_DATE) - DAYS(b.BILL_FINAL_DATE) <= 365
            THEN b.BILL_AMOUNT * 0.50  -- 50% for 181-365 days
        ELSE b.BILL_AMOUNT * 1.00     -- 100% for over 365 days
        END                                                      AS botProvision,

    -- Asset Classification Category (D32 lookup)
    CASE
        WHEN b.OVERDUE_FLG = '1' THEN 3  -- Substandard for overdue
        WHEN b.BILL_FINAL_DATE IS NULL OR b.BILL_FINAL_DATE >= CURRENT_DATE THEN 1  -- Normal
        WHEN DAYS(CURRENT_DATE) - DAYS(b.BILL_FINAL_DATE) <= 30 THEN 1   -- Normal
        WHEN DAYS(CURRENT_DATE) - DAYS(b.BILL_FINAL_DATE) <= 90 THEN 2   -- Watch
        WHEN DAYS(CURRENT_DATE) - DAYS(b.BILL_FINAL_DATE) <= 180 THEN 3  -- Substandard
        WHEN DAYS(CURRENT_DATE) - DAYS(b.BILL_FINAL_DATE) <= 365 THEN 4  -- Doubtful
        ELSE 5  -- Loss
        END                                                      AS assetClassificationCategory,

    /* =========================
       SECTOR CLASSIFICATION
       ========================= */

    -- Sector SNA Classification (Economic Sector lookup)
    CASE
        WHEN c.CUST_TYPE = '1' THEN 'Households'
        WHEN c.CUST_TYPE = '2' THEN 'Non-Financial Corporations'
        WHEN c.CUST_TYPE = '3' THEN 'Financial Corporations'
        ELSE 'Other Sectors'
        END                                                      AS sectorSnaClassification,

    /* =========================
       ADDITIONAL INFORMATION FOR TRACKING
       ========================= */

    -- Bill Serial Number (for reference)
    b.BILL_SERIAL_NUM                                        AS billSerialNumber,

    -- Bill Number
    b.BILL_NUMBER                                            AS billNumber,

    -- Bill Type
    CASE
        WHEN b.BILL_TYPE_FLAG = 'A' THEN 'Acceptance'
        WHEN b.BILL_TYPE_FLAG = 'B' THEN 'Bill of Exchange'
        WHEN b.BILL_TYPE_FLAG = 'D' THEN 'Draft'
        ELSE 'Other'
        END                                                      AS billType,

    -- Bill Status
    CASE
        WHEN b.BILL_STATUS_FLAG = '01' THEN 'Active'
        WHEN b.BILL_STATUS_FLAG = '02' THEN 'Partially Paid'
        WHEN b.BILL_STATUS_FLAG = '03' THEN 'Under Collection'
        WHEN b.BILL_STATUS_FLAG = '04' THEN 'Pending Maturity'
        WHEN b.BILL_STATUS_FLAG = '05' THEN 'Other Active'
        ELSE 'Unknown'
        END                                                      AS billStatus,

    -- Branch Code
    CAST(b.FK_UNITCODE AS VARCHAR(10))                      AS branchCode,

    -- Issuer Information
    COALESCE(bi.BISS_TITLE, 'Unknown Issuer')               AS issuerName

FROM BILL b

         /* ===== CUSTOMER INFORMATION ===== */
         LEFT JOIN CUSTOMER c
                   ON c.CUST_ID = b.FK_CUSTOMERCUST_ID

    /* ===== CURRENCY INFORMATION ===== */
         LEFT JOIN CURRENCY curr
                   ON curr.ID_CURRENCY = b.FK_CURR_ID_ISSUED

    /* ===== BILL ISSUER INFORMATION ===== */
         LEFT JOIN PROFITS.BILL_ISSUER bi
                   ON bi.BISS_CODE = b.FK_BISS_CODE

    /* ===== COLLATERAL INFORMATION ===== */
         LEFT JOIN PROFITS.COLLATERAL_TABLE ct
                   ON (ct.CUST_ID_1 = b.FK_CUSTOMERCUST_ID
                       OR ct.CUST_ID_2 = b.FK_CUSTOMERCUST_ID)
                       AND ct.ENTRY_STATUS = '1'

    /* ===== COLLATERAL TYPE LOOKUP ===== */
         LEFT JOIN PROFITS.GENERIC_DETAIL gd_coll
                   ON gd_coll.FK_GENERIC_HEADPAR = ct.GD_PAR_TYPE_1
                       AND gd_coll.SERIAL_NUM = ct.GD_SERIAL_NUM_1
                       AND gd_coll.ENTRY_STATUS = '1'

WHERE
  -- Only active bills
    b.BILL_ENTRY_STATUS = '1'
  -- Only acceptance-related bill types (if they exist)
--   AND (b.BILL_TYPE_FLAG IN ('A', 'B', 'D', '0') OR b.BILL_TYPE_FLAG IS NULL)
--   -- Only active statuses
--   AND (b.BILL_STATUS_FLAG IN ('01', '02', '03', '04', '05') OR b.BILL_STATUS_FLAG IS NULL)
--   -- Only valid amounts
--   AND b.BILL_AMOUNT > 0
--   -- Only active customers (if customer exists)
--   AND (c.CUST_STATUS = '1' OR c.CUST_STATUS IS NULL)
--   -- Exclude very old expired items (older than 2 years)
--   AND (b.BILL_FINAL_DATE IS NULL
--     OR b.BILL_FINAL_DATE >= CURRENT_DATE - 730 DAYS)

ORDER BY
    b.FK_UNITCODE,
    b.BILL_PURCHASE_DATE DESC,
    b.BILL_SERIAL_NUM;