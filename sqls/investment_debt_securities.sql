SELECT
    -- Reporting Date And Time
    MAX(VARCHAR_FORMAT(tdr.TMSTAMP, 'DDMMYYYYHHMM'))    AS reportingDate,

    -- Security Number (ISIN or Bond Code)
    MAX(COALESCE(NULLIF(TRIM(coll.BOND_ISIN), ''),
                 NULLIF(TRIM(coll.BOND_CODE), ''),
                 NULLIF(TRIM(mm.BOND_CODE), ''),
                 NULLIF(TRIM(mm.DEAL_REF_NO), ''),
                 VARCHAR(mm.DEAL_NO)))                  AS securityNumber,

    -- Security Type (Map to Table D27 per BOT spec)
    'Treasury bonds' AS securityType,

    -- Security Issuer Name
    MAX(COALESCE(TRIM(cust_iss.SURNAME), TRIM(cb.BANK_NAME), TRIM(mm.BANK_ID_BIC)))
                                                        AS securityIssuerName,

    -- Rating Status (True/False)
    MAX(CASE WHEN mm.FK_CORRESP_CUST IS NOT NULL THEN 'True' ELSE 'False' END)
                                                        AS ratingStatus,

    -- Placeholders for external ratings
    -- External Credit Rating of the Issuer (Table D67)
    CASE
        WHEN cb.DOM_CENTRAL_BANK = '1' OR cb.BANK_NAME LIKE '%CENTRAL BANK%' OR cb.BANK_NAME LIKE '%BANK OF TANZANIA%' THEN 'Central Bank'
        WHEN TRIM(cer.RATING) IN ('AAA', 'AA+', 'AA', 'AA-') THEN 'AAA to AA-'
        WHEN TRIM(cer.RATING) IN ('A+', 'A', 'A-') THEN 'A+ to A-'
        WHEN TRIM(cer.RATING) IN ('BBB+', 'BBB', 'BBB-') THEN 'BBB+ to BBB-'
        WHEN TRIM(cer.RATING) IN ('BB+', 'BB', 'BB-', 'B+', 'B', 'B-') THEN 'BB+ to B-'
        WHEN cer.RATING IS NOT NULL THEN 'Below B-'
        ELSE 'Unrated'
    END                                                 AS externalIssuerRating,
    -- Internal Grade for Unrated Banks (Table D68)
    CASE
        WHEN cb.DOM_CENTRAL_BANK = '1' OR cb.BANK_NAME LIKE '%CENTRAL BANK%' OR cb.BANK_NAME LIKE '%BANK OF TANZANIA%' THEN 'Grade A'
        WHEN cer.RATING IS NULL THEN 'Grade B'
        ELSE NULL
    END                                                 AS gradesUnratedBanks,

    -- Country of the Security Issuer
    COALESCE(TRIM(cl.COUNTRY_NAME), TRIM(cb.CNTRY_ISO_CODE), '') AS securityIssuerCountry,

    -- Sector Placeholder
    CASE
        WHEN cb.DOM_CENTRAL_BANK = '1' OR cb.BANK_NAME LIKE '%CENTRAL BANK%' OR cb.BANK_NAME LIKE '%BANK OF TANZANIA%' THEN 'Central Bank'
        WHEN prd.DESCRIPTION LIKE '%TREASURY BOND%' OR prd.DESCRIPTION LIKE '%T-BOND%' OR prd.DESCRIPTION LIKE '%TREASURY BILL%' THEN 'Central Governments'
        WHEN cb.BANK_ID IS NOT NULL THEN 'Other Depository Corporations'
        WHEN COALESCE(cust_iss.CUST_TYPE, 0) = 3 THEN 'Other financial Corporations'
        WHEN COALESCE(cust_iss.CUST_TYPE, 0) = 1 THEN 'Households'
        WHEN COALESCE(cust_iss.CUST_TYPE, 0) = 2 THEN 'Other Non-Financial Corporations'
        WHEN cl.COUNTRY_NAME IS NOT NULL AND cl.COUNTRY_NAME NOT LIKE '%TANZANIA%' THEN 'Nonresidents'
        ELSE 'Other Non-Financial Corporations'
    END                                                 AS sectorSnaClassification,

    -- Currency
    TRIM(tdr.BUY_CURRENCY)                              AS currency,

    -- ===================== COST VALUE AMOUNTS =====================

    -- Amount in Cost Value (original currency)
    MAX(COALESCE(NULLIF(mm.SETTLEMENT_AMT, 0), NULLIF(mm.GROSS_PRINC_AMOUNT, 0), NULLIF(mm.SOURCE_AMOUNT, 0), NULLIF(mm.TARGET_AMOUNT, 0), NULLIF(tdr.ACC_AMOUNT_1, 0), 0.00))
                                                        AS orgCostValueAmount,

    -- USD Equivalent Amount Cost Value
    MAX(CASE
        WHEN TRIM(tdr.BUY_CURRENCY) = 'USD' THEN
            COALESCE(NULLIF(mm.SETTLEMENT_AMT, 0), NULLIF(mm.GROSS_PRINC_AMOUNT, 0), NULLIF(mm.SOURCE_AMOUNT, 0), NULLIF(mm.TARGET_AMOUNT, 0), NULLIF(tdr.ACC_AMOUNT_1, 0), 0.00)
        WHEN cur.NATIONAL_FLAG = '1' THEN
            CAST(DOUBLE(COALESCE(NULLIF(mm.SETTLEMENT_AMT, 0), NULLIF(mm.GROSS_PRINC_AMOUNT, 0), NULLIF(mm.SOURCE_AMOUNT, 0), NULLIF(mm.TARGET_AMOUNT, 0), NULLIF(tdr.ACC_AMOUNT_1, 0), 0.00))
                 / DOUBLE(fr_usd.RATE) AS DECIMAL(15,2))
        ELSE
            CAST(DOUBLE(COALESCE(NULLIF(mm.SETTLEMENT_AMT, 0), NULLIF(mm.GROSS_PRINC_AMOUNT, 0), NULLIF(mm.SOURCE_AMOUNT, 0), NULLIF(mm.TARGET_AMOUNT, 0), NULLIF(tdr.ACC_AMOUNT_1, 0), 0.00))
                 * DOUBLE(COALESCE(fr_src.RATE, 1))
                 / DOUBLE(fr_usd.RATE) AS DECIMAL(15,2))
    END)                                                AS usdCostValueAmount,

    -- Amount in Cost Value (TZS equivalent)
    MAX(CASE
        WHEN cur.NATIONAL_FLAG = '1' THEN
            COALESCE(NULLIF(mm.SETTLEMENT_AMT, 0), NULLIF(mm.GROSS_PRINC_AMOUNT, 0), NULLIF(mm.SOURCE_AMOUNT, 0), NULLIF(mm.TARGET_AMOUNT, 0), NULLIF(tdr.ACC_AMOUNT_1, 0), 0.00)
        ELSE
            CAST(DOUBLE(COALESCE(NULLIF(mm.SETTLEMENT_AMT, 0), NULLIF(mm.GROSS_PRINC_AMOUNT, 0), NULLIF(mm.SOURCE_AMOUNT, 0), NULLIF(mm.TARGET_AMOUNT, 0), NULLIF(tdr.ACC_AMOUNT_1, 0), 0.00))
                 * DOUBLE(COALESCE(fr_src.RATE, 1)) AS DECIMAL(15,2))
    END)                                                AS tzsCostValueAmount,

    -- ===================== FACE VALUE AMOUNTS =====================

    -- Amount in Face Value (original currency)
    MAX(COALESCE(NULLIF(mm.GROSS_PRINC_AMOUNT, 0), NULLIF(mm.SETTLEMENT_AMT, 0), NULLIF(mm.SOURCE_AMOUNT, 0), NULLIF(mm.TARGET_AMOUNT, 0), NULLIF(tdr.ACC_AMOUNT_1, 0), 0.00))
                                                        AS faceValueAmount,

    -- USD Equivalent Amount Face Value
    MAX(CASE
        WHEN TRIM(tdr.BUY_CURRENCY) = 'USD' THEN
            COALESCE(NULLIF(mm.GROSS_PRINC_AMOUNT, 0), NULLIF(mm.SETTLEMENT_AMT, 0), NULLIF(mm.SOURCE_AMOUNT, 0), NULLIF(mm.TARGET_AMOUNT, 0), NULLIF(tdr.ACC_AMOUNT_1, 0), 0.00)
        WHEN cur.NATIONAL_FLAG = '1' THEN
            CAST(DOUBLE(COALESCE(NULLIF(mm.GROSS_PRINC_AMOUNT, 0), NULLIF(mm.SETTLEMENT_AMT, 0), NULLIF(mm.SOURCE_AMOUNT, 0), NULLIF(mm.TARGET_AMOUNT, 0), NULLIF(tdr.ACC_AMOUNT_1, 0), 0.00))
                 / DOUBLE(fr_usd.RATE) AS DECIMAL(15,2))
        ELSE
            CAST(DOUBLE(COALESCE(NULLIF(mm.GROSS_PRINC_AMOUNT, 0), NULLIF(mm.SETTLEMENT_AMT, 0), NULLIF(mm.SOURCE_AMOUNT, 0), NULLIF(mm.TARGET_AMOUNT, 0), NULLIF(tdr.ACC_AMOUNT_1, 0), 0.00))
                 * DOUBLE(COALESCE(fr_src.RATE, 1))
                 / DOUBLE(fr_usd.RATE) AS DECIMAL(15,2))
    END)                                                AS usdFaceValueAmount,

    -- TZS Amount Face Value
    MAX(CASE
        WHEN cur.NATIONAL_FLAG = '1' THEN
            COALESCE(NULLIF(mm.GROSS_PRINC_AMOUNT, 0), NULLIF(mm.SETTLEMENT_AMT, 0), NULLIF(mm.SOURCE_AMOUNT, 0), NULLIF(mm.TARGET_AMOUNT, 0), NULLIF(tdr.ACC_AMOUNT_1, 0), 0.00)
        ELSE
            CAST(DOUBLE(COALESCE(NULLIF(mm.GROSS_PRINC_AMOUNT, 0), NULLIF(mm.SETTLEMENT_AMT, 0), NULLIF(mm.SOURCE_AMOUNT, 0), NULLIF(mm.TARGET_AMOUNT, 0), NULLIF(tdr.ACC_AMOUNT_1, 0), 0.00))
                 * DOUBLE(COALESCE(fr_src.RATE, 1)) AS DECIMAL(15,2))
    END)                                                AS tzsFaceValueAmount,

    -- ===================== FAIR VALUE AMOUNTS =====================

    -- Amount in Fair Value (original currency)
    MAX(CASE
        WHEN mkt.PRICE IS NOT NULL THEN
            CAST(DOUBLE(COALESCE(NULLIF(mm.GROSS_PRINC_AMOUNT, 0), NULLIF(mm.SETTLEMENT_AMT, 0), NULLIF(mm.SOURCE_AMOUNT, 0), NULLIF(mm.TARGET_AMOUNT, 0), NULLIF(tdr.ACC_AMOUNT_1, 0), 0.00))
                 * DOUBLE(mkt.PRICE) / 100 AS DECIMAL(15,2))
        WHEN mm.PRICE IS NOT NULL AND mm.PRICE > 0 THEN
            CAST(DOUBLE(COALESCE(NULLIF(mm.GROSS_PRINC_AMOUNT, 0), NULLIF(mm.SETTLEMENT_AMT, 0), NULLIF(mm.SOURCE_AMOUNT, 0), NULLIF(mm.TARGET_AMOUNT, 0), NULLIF(tdr.ACC_AMOUNT_1, 0), 0.00))
                 * DOUBLE(mm.PRICE) / 100 AS DECIMAL(15,2))
        ELSE COALESCE(NULLIF(mm.GROSS_PRINC_AMOUNT, 0), NULLIF(mm.SETTLEMENT_AMT, 0), NULLIF(mm.SOURCE_AMOUNT, 0), NULLIF(mm.TARGET_AMOUNT, 0), NULLIF(tdr.ACC_AMOUNT_1, 0), 0.00)
    END)                                                AS orgFairValueAmount,

    -- USD Equivalent Amount Fair Value
    MAX(CASE
        WHEN TRIM(tdr.BUY_CURRENCY) = 'USD' THEN
            CASE
                WHEN mkt.PRICE IS NOT NULL THEN
                    CAST(DOUBLE(COALESCE(NULLIF(mm.GROSS_PRINC_AMOUNT, 0), NULLIF(mm.SETTLEMENT_AMT, 0), NULLIF(mm.SOURCE_AMOUNT, 0), NULLIF(mm.TARGET_AMOUNT, 0), NULLIF(tdr.ACC_AMOUNT_1, 0), 0.00))
                         * DOUBLE(mkt.PRICE) / 100 AS DECIMAL(15,2))
                WHEN mm.PRICE IS NOT NULL AND mm.PRICE > 0 THEN
                    CAST(DOUBLE(COALESCE(NULLIF(mm.GROSS_PRINC_AMOUNT, 0), NULLIF(mm.SETTLEMENT_AMT, 0), NULLIF(mm.SOURCE_AMOUNT, 0), NULLIF(mm.TARGET_AMOUNT, 0), NULLIF(tdr.ACC_AMOUNT_1, 0), 0.00))
                         * DOUBLE(mm.PRICE) / 100 AS DECIMAL(15,2))
                ELSE COALESCE(NULLIF(mm.GROSS_PRINC_AMOUNT, 0), NULLIF(mm.SETTLEMENT_AMT, 0), NULLIF(mm.SOURCE_AMOUNT, 0), NULLIF(mm.TARGET_AMOUNT, 0), NULLIF(tdr.ACC_AMOUNT_1, 0), 0.00)
            END
        WHEN cur.NATIONAL_FLAG = '1' THEN
            CAST(DOUBLE(
                CASE
                    WHEN mkt.PRICE IS NOT NULL THEN
                        DOUBLE(COALESCE(NULLIF(mm.GROSS_PRINC_AMOUNT, 0), NULLIF(mm.SETTLEMENT_AMT, 0), NULLIF(mm.SOURCE_AMOUNT, 0), NULLIF(mm.TARGET_AMOUNT, 0), NULLIF(tdr.ACC_AMOUNT_1, 0), 0.00))
                        * DOUBLE(mkt.PRICE) / 100
                    WHEN mm.PRICE IS NOT NULL AND mm.PRICE > 0 THEN
                        DOUBLE(COALESCE(NULLIF(mm.GROSS_PRINC_AMOUNT, 0), NULLIF(mm.SETTLEMENT_AMT, 0), NULLIF(mm.SOURCE_AMOUNT, 0), NULLIF(mm.TARGET_AMOUNT, 0), NULLIF(tdr.ACC_AMOUNT_1, 0), 0.00))
                        * DOUBLE(mm.PRICE) / 100
                    ELSE DOUBLE(COALESCE(NULLIF(mm.GROSS_PRINC_AMOUNT, 0), NULLIF(mm.SETTLEMENT_AMT, 0), NULLIF(mm.SOURCE_AMOUNT, 0), NULLIF(mm.TARGET_AMOUNT, 0), NULLIF(tdr.ACC_AMOUNT_1, 0), 0.00))
                END
            ) / DOUBLE(fr_usd.RATE) AS DECIMAL(15,2))
        ELSE
            CAST(DOUBLE(
                CASE
                    WHEN mkt.PRICE IS NOT NULL THEN
                        DOUBLE(COALESCE(NULLIF(mm.GROSS_PRINC_AMOUNT, 0), NULLIF(mm.SETTLEMENT_AMT, 0), NULLIF(mm.SOURCE_AMOUNT, 0), NULLIF(mm.TARGET_AMOUNT, 0), NULLIF(tdr.ACC_AMOUNT_1, 0), 0.00))
                        * DOUBLE(mkt.PRICE) / 100
                    WHEN mm.PRICE IS NOT NULL AND mm.PRICE > 0 THEN
                        DOUBLE(COALESCE(NULLIF(mm.GROSS_PRINC_AMOUNT, 0), NULLIF(mm.SETTLEMENT_AMT, 0), NULLIF(mm.SOURCE_AMOUNT, 0), NULLIF(mm.TARGET_AMOUNT, 0), NULLIF(tdr.ACC_AMOUNT_1, 0), 0.00))
                        * DOUBLE(mm.PRICE) / 100
                    ELSE DOUBLE(COALESCE(NULLIF(mm.GROSS_PRINC_AMOUNT, 0), NULLIF(mm.SETTLEMENT_AMT, 0), NULLIF(mm.SOURCE_AMOUNT, 0), NULLIF(mm.TARGET_AMOUNT, 0), NULLIF(tdr.ACC_AMOUNT_1, 0), 0.00))
                END
            ) * DOUBLE(COALESCE(fr_src.RATE, 1))
              / DOUBLE(fr_usd.RATE) AS DECIMAL(15,2))
    END)                                                AS usdFairValueAmount,

    -- TZS Amount Fair Value
    MAX(CASE
        WHEN cur.NATIONAL_FLAG = '1' THEN
            CASE
                WHEN mkt.PRICE IS NOT NULL THEN
                    CAST(DOUBLE(COALESCE(NULLIF(mm.GROSS_PRINC_AMOUNT, 0), NULLIF(mm.SETTLEMENT_AMT, 0), NULLIF(mm.SOURCE_AMOUNT, 0), NULLIF(mm.TARGET_AMOUNT, 0), NULLIF(tdr.ACC_AMOUNT_1, 0), 0.00))
                         * DOUBLE(mkt.PRICE) / 100 AS DECIMAL(15,2))
                WHEN mm.PRICE IS NOT NULL AND mm.PRICE > 0 THEN
                    CAST(DOUBLE(COALESCE(NULLIF(mm.GROSS_PRINC_AMOUNT, 0), NULLIF(mm.SETTLEMENT_AMT, 0), NULLIF(mm.SOURCE_AMOUNT, 0), NULLIF(mm.TARGET_AMOUNT, 0), NULLIF(tdr.ACC_AMOUNT_1, 0), 0.00))
                         * DOUBLE(mm.PRICE) / 100 AS DECIMAL(15,2))
                ELSE COALESCE(NULLIF(mm.GROSS_PRINC_AMOUNT, 0), NULLIF(mm.SETTLEMENT_AMT, 0), NULLIF(mm.SOURCE_AMOUNT, 0), NULLIF(mm.TARGET_AMOUNT, 0), NULLIF(tdr.ACC_AMOUNT_1, 0), 0.00)
            END
        ELSE
            CAST(DOUBLE(
                CASE
                    WHEN mkt.PRICE IS NOT NULL THEN
                        DOUBLE(COALESCE(NULLIF(mm.GROSS_PRINC_AMOUNT, 0), NULLIF(mm.SETTLEMENT_AMT, 0), NULLIF(mm.SOURCE_AMOUNT, 0), NULLIF(mm.TARGET_AMOUNT, 0), NULLIF(tdr.ACC_AMOUNT_1, 0), 0.00))
                        * DOUBLE(mkt.PRICE) / 100
                    WHEN mm.PRICE IS NOT NULL AND mm.PRICE > 0 THEN
                        DOUBLE(COALESCE(NULLIF(mm.GROSS_PRINC_AMOUNT, 0), NULLIF(mm.SETTLEMENT_AMT, 0), NULLIF(mm.SOURCE_AMOUNT, 0), NULLIF(mm.TARGET_AMOUNT, 0), NULLIF(tdr.ACC_AMOUNT_1, 0), 0.00))
                        * DOUBLE(mm.PRICE) / 100
                    ELSE DOUBLE(COALESCE(NULLIF(mm.GROSS_PRINC_AMOUNT, 0), NULLIF(mm.SETTLEMENT_AMT, 0), NULLIF(mm.SOURCE_AMOUNT, 0), NULLIF(mm.TARGET_AMOUNT, 0), NULLIF(tdr.ACC_AMOUNT_1, 0), 0.00))
                END
            ) * DOUBLE(COALESCE(fr_src.RATE, 1)) AS DECIMAL(15,2))
    END)                                                AS tzsFairValueAmount,

    -- ===================== DATES & DEATILS =====================

    DECIMAL(mm.INTEREST_RATE, 15, 2)                    AS interestRate,
    VARCHAR_FORMAT(mm.DEAL_DATE, 'DDMMYYYYHHMM')        AS purchaseDate,
    VARCHAR_FORMAT(mm.VALUE_DATE, 'DDMMYYYYHHMM')       AS valueDate,
    VARCHAR_FORMAT(mm.MATURITY_DATE, 'DDMMYYYYHHMM')    AS maturityDate,

    CASE mm.SEC_MATCHING_FLG
        WHEN '1' THEN 'Held to Maturity'
        WHEN '2' THEN 'Available for Sale'
        WHEN '3' THEN 'Available for Sale'
        ELSE 'Held to Maturity'
    END                                                 AS tradingIntent,

    CASE WHEN mm.STATUS = 'P' THEN 'Encumbered' ELSE 'Unencumbered' END
                                                         AS securityEncumbranceStatus,

    CASE WHEN mm.MATURITY_DATE < CURRENT DATE THEN DAYS(CURRENT DATE) - DAYS(mm.MATURITY_DATE) ELSE 0 END
                                                         AS pastDueDays,

    DECIMAL(0, 15, 2)                                   AS allowanceProbableLoss,
    DECIMAL(0, 15, 2)                                   AS botProvision,

    CASE
        WHEN mm.MATURITY_DATE < CURRENT DATE AND (DAYS(CURRENT DATE) - DAYS(mm.MATURITY_DATE)) > 90 THEN 'SUBSTANDARD'
        WHEN mm.MATURITY_DATE < CURRENT DATE THEN 'WATCH'
        ELSE 'CURRENT'
    END                                                 AS assetClassificationCategory

FROM TRS_DEAL_RECORDING tdr
JOIN TREASURY_MM_DEAL mm ON tdr.DEAL_NO = mm.DEAL_NO
LEFT JOIN TRS_DEAL_COLLATERAL coll ON tdr.DEAL_NO = coll.FK_DEAL_NO AND coll.ENTRY_STATUS = '1'
LEFT JOIN CURRENCY cur ON mm.FK_SOURCE_CURRENCY = cur.ID_CURRENCY
LEFT JOIN COLLABORATION_BANK cb ON mm.FK_DEAL_COL_BANK = cb.BANK_ID
LEFT JOIN CUSTOMER cust_iss ON mm.FK_CORRESP_CUST = cust_iss.CUST_ID
LEFT JOIN PRODUCT prd ON tdr.ID_PRODUCT = prd.ID_PRODUCT

-- Latest External Rating for the Counterparty
LEFT JOIN (
    SELECT r1.CUST_ID, r1.RATING
    FROM CUST_EXT_RATING r1
    WHERE (r1.CUST_ID, r1.CREATE_DT, r1.RATE_DT) IN (
        SELECT r2.CUST_ID, MAX(r2.CREATE_DT), MAX(r2.RATE_DT)
        FROM CUST_EXT_RATING r2
        GROUP BY r2.CUST_ID
    )
) cer ON mm.FK_CORRESP_CUST = cer.CUST_ID

-- Unique Country Lookup
LEFT JOIN (
    SELECT COUNTRY_CODE, MAX(COUNTRY_NAME) AS COUNTRY_NAME
    FROM COUNTRIES_LOOKUP
    GROUP BY COUNTRY_CODE
) cl ON cb.CNTRY_ISO_CODE = cl.COUNTRY_CODE

-- Latest market price
LEFT JOIN TRS_MARKET_PRICE mkt
    ON COALESCE(NULLIF(TRIM(coll.BOND_ISIN),''), NULLIF(TRIM(coll.BOND_CODE),''), NULLIF(TRIM(mm.BOND_CODE),'')) = mkt.ISIN
    AND mkt.ACTIVATION_DATE = (SELECT MAX(mp2.ACTIVATION_DATE) FROM TRS_MARKET_PRICE mp2
        WHERE mp2.ISIN = COALESCE(NULLIF(TRIM(coll.BOND_ISIN),''), NULLIF(TRIM(coll.BOND_CODE),''), NULLIF(TRIM(mm.BOND_CODE),''))
          AND mp2.ACTIVATION_DATE <= CURRENT DATE)

-- Fixing Rates
LEFT JOIN FIXING_RATE fr_src
    ON fr_src.FK_CURRENCYID_CURR = mm.FK_SOURCE_CURRENCY
    AND fr_src.ACTIVATION_DATE = (SELECT MAX(f1.ACTIVATION_DATE) FROM FIXING_RATE f1
        WHERE f1.FK_CURRENCYID_CURR = mm.FK_SOURCE_CURRENCY AND f1.ACTIVATION_DATE <= CURRENT DATE)

LEFT JOIN CURRENCY cur_usd ON cur_usd.SHORT_DESCR = 'USD'
LEFT JOIN FIXING_RATE fr_usd
    ON fr_usd.FK_CURRENCYID_CURR = cur_usd.ID_CURRENCY
    AND fr_usd.ACTIVATION_DATE = (SELECT MAX(f2.ACTIVATION_DATE) FROM FIXING_RATE f2
        WHERE f2.FK_CURRENCYID_CURR = cur_usd.ID_CURRENCY AND f2.ACTIVATION_DATE <= CURRENT DATE)

WHERE tdr.CANCEL_FLG != '1' AND tdr.DEAL_STATUS != 'C'
GROUP BY
    mm.DEAL_NO,
    mm.DEAL_REF_NO,
    mm.BOND_CODE,
    mm.BANK_ID_BIC,
    mm.FK_CORRESP_CUST,
    mm.INTEREST_RATE,
    mm.DEAL_DATE,
    mm.VALUE_DATE,
    mm.MATURITY_DATE,
    mm.SEC_MATCHING_FLG,
    mm.STATUS,
    tdr.BUY_CURRENCY,
    tdr.ID_PRODUCT,
    cur.NATIONAL_FLAG,
    cb.BANK_NAME,
    cb.BANK_ID,
    cb.CNTRY_ISO_CODE,
    cb.DOM_CENTRAL_BANK,
    cust_iss.SURNAME,
    cl.COUNTRY_NAME,
    prd.DESCRIPTION,
    cust_iss.CUST_TYPE,
    cer.RATING;