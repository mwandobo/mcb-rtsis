
SELECT
    VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHH24MI')     AS reportingDate,
    COALESCE(
            oid.ID_NO,
            CAST(c.CUST_ID AS VARCHAR(20))
    )                                                        AS customerIdentificationNumber,

    -- Account number (loan account)
    CAST(la.FK_UNITCODE AS VARCHAR(10)) ||
    CAST(la.ACC_TYPE AS VARCHAR(5)) ||
    CAST(la.ACC_SN AS VARCHAR(15))                          AS accountNumber,

    -- Client name
    TRIM(
            COALESCE(c.FIRST_NAME, '') || ' ' ||
            COALESCE(c.MIDDLE_NAME, '') || ' ' ||
            COALESCE(c.SURNAME, '')
    )                                                        AS clientName,

    ctl.CUSTOMER_TYPE                                        as clientType,

    -- Gender
    CASE
        WHEN ctl.CUSTOMER_TYPE = 'Corporations' THEN 'Not Applicable'
        ELSE
            CASE
                WHEN c.SEX = 'M' THEN 'Male'
                WHEN c.SEX = 'F' THEN 'Female'
                ELSE 'Not Applicable'
                END
        END                                                      AS gender,

    -- Age (calculated from date of birth)
    CASE
        WHEN ctl.CUSTOMER_TYPE = 'Corporations' THEN 0
        ELSE
            CASE WHEN c.DATE_OF_BIRTH IS NOT NULL
                     THEN YEAR(CURRENT_DATE) - YEAR(c.DATE_OF_BIRTH)
                 ELSE NULL
                END
        END                                                      AS age,

    -- Disability status (placeholder - would need specific table)
    NULL                                                     AS disabilityStatus,

    /* =========================
       LOAN IDENTIFICATION
       ========================= */

    -- Loan number (agreement number)
    COALESCE(
            agr.OLD_AGREEMEMT_NUM,
            CAST(agr.FK_UNITCODE AS VARCHAR(10)) || '-' ||
            CAST(agr.AGR_YEAR AS VARCHAR(4)) || '-' ||
            CAST(agr.AGR_SN AS VARCHAR(10))
    )                                                        AS loanNumber,

    -- Gender
    CASE
        WHEN ctl.CUSTOMER_TYPE = 'Corporations' THEN 'Services'
        ELSE 'Personal Loans'
        END  AS loanIndustryClassification,

    -- Gender
    'Others'  AS loanSubIndustry,

    CASE
        WHEN ctl.CUSTOMER_TYPE = 'Corporations' THEN 'Business Group Loans'
        ELSE 'Business Individual Loans'
        END  AS microfinanceLoansType,

    'Reducing Method'                                         as amortizationType,


    /* =========================
       BRANCH AND OFFICER INFO
       ========================= */

    -- Branch code
    CAST(la.FK_UNITCODE AS VARCHAR(10))                     AS branchCode,

    -- Loan officer
    COALESCE(
            emp.FIRST_NAME || ' ' || emp.LAST_NAME,
            agr.FK_BANKEMPLOYEEID,
            'Not Assigned'
    )                                                        AS loanOfficer,

    -- Loan supervisor
    COALESCE(
            sup.FIRST_NAME || ' ' || sup.LAST_NAME,
            agr.PRV_OFFICER,
            'Not Assigned'
    )                                                        AS loanSupervisor,




    -- Group/Village number (from village or cooperative)
    COALESCE(
            la.VILLAGE_SN,
            la.FKCUS_COOPERATIVE
    )                                                        AS groupVillageNumber,

    -- Cycle number (loan recycling count)
    COALESCE(la.RECYCLING_FRQ, 1)                          AS cycleNumber,

    -- Currency
    COALESCE(curr.SHORT_DESCR, 'TZS')                      AS currency,

    -- Original sanction amount (approved limit)
--     la.ACC_LIMIT_AMN                                        AS orgSanctionAmount,
--
--     -- USD equivalent sanction amount
--     CASE
--         WHEN curr.SHORT_DESCR = 'USD'
--             THEN la.ACC_LIMIT_AMN
--         WHEN curr.SHORT_DESCR = 'TZS'
--             THEN la.ACC_LIMIT_AMN / 2730.50
--         WHEN curr.SHORT_DESCR = 'EUR'
--             THEN la.ACC_LIMIT_AMN * 1.08
--         ELSE la.ACC_LIMIT_AMN / 2730.50
--         END                                                      AS usdSanctionAmount,
--
--     -- TZS sanction amount
--     CASE
--         WHEN curr.SHORT_DESCR = 'USD'
--             THEN la.ACC_LIMIT_AMN * 2730.50
--         WHEN curr.SHORT_DESCR = 'EUR'
--             THEN la.ACC_LIMIT_AMN * 2950.00
--         ELSE la.ACC_LIMIT_AMN
--         END                                                      AS tzsSanctionAmount,




    la.ACC_LIMIT_AMN                                                       as orgSanctionedAmount,
    CASE
        WHEN curr.SHORT_DESCR = 'USD'
            THEN la.ACC_LIMIT_AMN
        ELSE NULL
        END                                                                  AS usdSanctionedAmount,
    CASE
        WHEN curr.SHORT_DESCR = 'USD'
            THEN la.ACC_LIMIT_AMN * 2500 -- <<< replace with your rate
        ELSE
            la.ACC_LIMIT_AMN
        END                                                                  AS tzsSanctionedAmount,

    la.TOT_DRAWDOWN_AMN                                                    as orgDisbursedAmount,
    CASE
        WHEN curr.SHORT_DESCR = 'USD'
            THEN la.TOT_DRAWDOWN_AMN
        ELSE NULL
        END                                                                  AS usdDisbursedAmount,
    CASE
        WHEN curr.SHORT_DESCR = 'USD'
            THEN la.TOT_DRAWDOWN_AMN * 2500 -- <<< replace with your rate
        ELSE
            la.TOT_DRAWDOWN_AMN
        END                                                                  AS tzsDisbursedAmount,

    -- Disbursement date (first drawdown)
    VARCHAR_FORMAT(la.DRAWDOWN_FST_DT, 'DDMMYYYYHH24MI')   AS disbursementDate,

    -- Maturity date
    VARCHAR_FORMAT(la.ACC_EXP_DT, 'DDMMYYYYHH24MI')        AS maturityDate,

    -- Restructuring date (if applicable)
    CASE
        WHEN la.LOAN_STATUS IN ('R', 'S')
            THEN VARCHAR_FORMAT(la.LST_TRX_DT, 'DDMMYYYYHH24MI')
        ELSE NULL
        END                                                      AS restructuringDate,

    -- Amount written off
    CASE
        WHEN la.LOAN_STATUS = '4'
            THEN la.NRM_CAP_BAL + la.OV_CAP_BAL
        ELSE 0
        END                                                      AS writtenOffAmount,


    -- Agreed loan installments
    la.INSTALL_COUNT                                        AS agreedLoanInstallments,


    CASE
        WHEN la.INSTALL_FREQ = 1 THEN 'Daily'
        WHEN la.INSTALL_FREQ = 7 THEN 'Weekly'
        WHEN la.INSTALL_FREQ = 14 THEN 'Bi-weekly'
        WHEN la.INSTALL_FREQ = 30 THEN 'Monthly'
        WHEN la.INSTALL_FREQ = 90 THEN 'Quarterly'
        WHEN la.INSTALL_FREQ = 180 THEN 'Semi-annually'
        WHEN la.INSTALL_FREQ = 365 THEN 'Annually'
        ELSE 'Monthly'
        END                                                      AS repaymentFrequency,
--     'Irregular'                                                              as repaymentFrequency,


    /* =========================
       OUTSTANDING AMOUNTS
       ========================= */

    -- Original outstanding principal amount
--     (la.NRM_CAP_BAL + la.OV_CAP_BAL)                       AS orgOutstandingPrincipalAmount,
--
--     -- USD equivalent outstanding principal
--     CASE
--         WHEN curr.SHORT_DESCR = 'USD'
--             THEN (la.NRM_CAP_BAL + la.OV_CAP_BAL)
--         WHEN curr.SHORT_DESCR = 'TZS'
--             THEN (la.NRM_CAP_BAL + la.OV_CAP_BAL) / 2730.50
--         WHEN curr.SHORT_DESCR = 'EUR'
--             THEN (la.NRM_CAP_BAL + la.OV_CAP_BAL) * 1.08
--         ELSE (la.NRM_CAP_BAL + la.OV_CAP_BAL) / 2730.50
--         END                                                      AS usdOutstandingPrincipalAmount,
--
--     -- TZS outstanding principal
--     CASE
--         WHEN curr.SHORT_DESCR = 'USD'
--             THEN (la.NRM_CAP_BAL + la.OV_CAP_BAL) * 2730.50
--         WHEN curr.SHORT_DESCR = 'EUR'
--             THEN (la.NRM_CAP_BAL + la.OV_CAP_BAL) * 2950.00
--         ELSE (la.NRM_CAP_BAL + la.OV_CAP_BAL)
--         END                                                      AS tzsOutstandingPrincipalAmount,

    (la.NRM_CAP_BAL + la.OV_CAP_BAL)                                     AS orgOutstandingPrincipalAmount,
    CASE
        WHEN curr.SHORT_DESCR = 'USD' THEN (la.NRM_CAP_BAL + la.OV_CAP_BAL)
        ELSE NULL
        END                                                                  AS usdOutstandingPrincipalAmount,
    CASE
        WHEN curr.SHORT_DESCR = 'USD' THEN (la.NRM_CAP_BAL + la.OV_CAP_BAL) * 2500
        ELSE (la.NRM_CAP_BAL + la.OV_CAP_BAL)
        END                                                                  AS tzsOutstandingPrincipalAmount,

    -- Loan installments paid (calculated)
    CASE
        WHEN la.INSTALL_COUNT > 0
            THEN la.INSTALL_COUNT - (la.NRM_INST_CNT + la.OV_INST_CNT)
        ELSE 0
        END                                                      AS loanInstallmentPaid,

    -- Grace period payment principal (placeholder)
    0                                                        AS gracePeriodPaymentPrincipal,

    /* =========================
       INTEREST RATES
       ========================= */

    -- Prime lending rate (base rate)
    COALESCE(br.BASE_RATE_PERC, 15.00)                     AS primeLendingRate,

    -- Annual nominal interest rate
    COALESCE(
            la.INTER_RATE_SPRD + COALESCE(br.BASE_RATE_PERC, 15.00),
            20.00
    )                                                        AS annualInterestRate,

    -- Annual effective interest rate (with fees)
    COALESCE(
            la.INTER_RATE_SPRD + COALESCE(br.BASE_RATE_PERC, 15.00) + 2.00,
            22.00
    )                                                        AS effectiveAnnualInterestRate,

    -- Agreed first installment payment date
    la.INSTALL_FIRST_DT                                     AS firstInstallmentPaymentDate,

    /* =========================
       COLLATERAL INFORMATION
       ========================= */

    -- Collateral (Y/N)
--     CASE
--         WHEN ct.INTERNAL_SN IS NOT NULL THEN 'Y'
--         ELSE 'N'
--         END                                                      AS collateral,
--
--     -- Collateral category
--     COALESCE(
--             gd_coll.LATIN_DESC,
--             CASE
--                 WHEN ct.RECORD_TYPE = '01' THEN 'Real Estate'
--                 WHEN ct.RECORD_TYPE = '02' THEN 'Vehicle'
--                 WHEN ct.RECORD_TYPE = '03' THEN 'Cash Deposit'
--                 WHEN ct.RECORD_TYPE = '04' THEN 'Guarantee'
--                 ELSE 'Other'
--                 END
--     )                                                        AS collateralCategory,
--
--     -- TZS collateral value
--     CASE
--         WHEN ct.CURRENCY_ID IS NOT NULL AND curr_coll.SHORT_DESCR = 'USD'
--             THEN COALESCE(ct.AMOUNT_1, 0) * 2730.50
--         WHEN ct.CURRENCY_ID IS NOT NULL AND curr_coll.SHORT_DESCR = 'EUR'
--             THEN COALESCE(ct.AMOUNT_1, 0) * 2950.00
--         ELSE COALESCE(ct.AMOUNT_1, 0)
--         END                                                      AS tzsCollateralValue,

    /* =========================
       LOAN STATUS AND CLASSIFICATION
       ========================= */

    -- Loan flag type
    CASE
        WHEN la.LOAN_STATUS IN ('R', 'S') THEN 'Restructured'
        ELSE 'Non-restructured'
        END                                                      AS loanFlagType,

    -- Past due days
    CASE
        WHEN la.OV_EXP_DT IS NOT NULL AND la.OV_EXP_DT < CURRENT_DATE
            THEN DAYS(CURRENT_DATE) - DAYS(la.OV_EXP_DT)
        ELSE 0
        END                                                      AS pastDueDays,

    -- Past due amount
    (la.OV_CAP_BAL + la.OV_RL_NRM_INT_BAL + la.OV_RL_PNL_INT_BAL +
     la.OV_COM_BAL + la.OV_EXP_BAL)                        AS pastDueAmount,

    /* =========================
       ACCRUED INTEREST
       ========================= */

    -- Original accrued interest amount
    (la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL +
     la.OV_ACR_PNL_INT_BAL)                                AS orgAccruedInterestAmount,

    -- USD equivalent accrued interest
    CASE
        WHEN curr.SHORT_DESCR = 'USD'
            THEN (la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL)
        WHEN curr.SHORT_DESCR = 'TZS'
            THEN (la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL) / 2500.50
        ELSE (la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL) / 2500.50
        END                                                      AS usdAccruedInterestAmount,

    -- TZS accrued interest amount
    CASE
        WHEN curr.SHORT_DESCR = 'USD'
            THEN (la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL) * 2730.50
        ELSE (la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL)
        END                                                      AS tzsAccruedInterestAmount,
    'Current'                                                                AS assetClassificationCategory,
    -- Allowance for probable loss (provision)
    CASE
        WHEN la.LOAN_STATUS = 'W' THEN la.NRM_CAP_BAL + la.OV_CAP_BAL  -- 100% for written off
        WHEN la.OV_EXP_DT IS NULL OR DAYS(CURRENT_DATE) - DAYS(la.OV_EXP_DT) <= 30
            THEN (la.NRM_CAP_BAL + la.OV_CAP_BAL) * 0.01  -- 1% for Normal
        WHEN DAYS(CURRENT_DATE) - DAYS(la.OV_EXP_DT) <= 90
            THEN (la.NRM_CAP_BAL + la.OV_CAP_BAL) * 0.05  -- 5% for Watch
        WHEN DAYS(CURRENT_DATE) - DAYS(la.OV_EXP_DT) <= 180
            THEN (la.NRM_CAP_BAL + la.OV_CAP_BAL) * 0.20  -- 20% for Substandard
        WHEN DAYS(CURRENT_DATE) - DAYS(la.OV_EXP_DT) <= 365
            THEN (la.NRM_CAP_BAL + la.OV_CAP_BAL) * 0.50  -- 50% for Doubtful
        ELSE (la.NRM_CAP_BAL + la.OV_CAP_BAL) * 1.00      -- 100% for Loss
        END                                                      AS allowanceProbableLoss,
    0                                                     AS botProvision
FROM LOAN_ACCOUNT la

/* ===== CUSTOMER INFORMATION ===== */
         LEFT JOIN CUSTOMER c
                   ON c.CUST_ID = la.CUST_ID
         LEFT JOIN CUSTOMER_TYPES_LOOKUP ctl ON ctl.CUSTOMER_TYPE_CODE = c.CUST_TYPE


/* ===== CUSTOMER ID FROM OTHER_ID TABLE ===== */
         LEFT JOIN OTHER_ID oid
                   ON oid.FK_CUSTOMERCUST_ID = c.CUST_ID

/* ===== AGREEMENT INFORMATION ===== */
         LEFT JOIN AGREEMENT agr
                   ON agr.FK_UNITCODE = la.FK_AGREEMENTFK_UNI
                       AND agr.AGR_YEAR = la.FK_AGREEMENTAGR_YE
                       AND agr.AGR_SN = la.FK_AGREEMENTAGR_SN
                       AND agr.AGR_MEMBERSHIP_SN = la.FK_AGREEMENTAGR_ME

/* ===== PRODUCT INFORMATION ===== */
         LEFT JOIN PRODUCT p
                   ON p.ID_PRODUCT = la.FK_LOANFK_PRODUCTI

/* ===== CURRENCY INFORMATION ===== */
         LEFT JOIN CURRENCY curr
                   ON curr.ID_CURRENCY = la.FKCUR_IS_MOVED_IN

/* ===== BASE RATE INFORMATION ===== */
         LEFT JOIN BASE_RATE br
                   ON br.FK_GH_PAR_TYPE = 'PRIME'
                       AND br.VALIDITY_DATE <= CURRENT_DATE
                       AND br.ENTRY_STATUS = '1'

/* ===== LOAN OFFICER INFORMATION ===== */
         LEFT JOIN BANKEMPLOYEE emp
                   ON emp.ID = agr.FK_BANKEMPLOYEEID

/* ===== LOAN SUPERVISOR INFORMATION ===== */
         LEFT JOIN BANKEMPLOYEE sup
                   ON sup.ID = agr.FK0BANKEMPLOYEEID

/* ===== INDUSTRY CLASSIFICATION ===== */
         LEFT JOIN GENERIC_DETAIL gd_industry
                   ON gd_industry.FK_GENERIC_HEADPAR = la.FKGH_CATEGORY
                       AND gd_industry.SERIAL_NUM = la.FKGD_CATEGORY

/* ===== SUB-INDUSTRY CLASSIFICATION ===== */
         LEFT JOIN GENERIC_DETAIL gd_subind
                   ON gd_subind.FK_GENERIC_HEADPAR = la.FKGH_HAS_AS_FINANC
                       AND gd_subind.SERIAL_NUM = la.FKGD_HAS_AS_FINANC

/* ===== COLLATERAL INFORMATION ===== */
         LEFT JOIN COLLATERAL_TABLE ct
                   ON (ct.PROFITS_ACCOUNT_1 = CAST(la.FK_UNITCODE AS VARCHAR(10)) ||
                                              CAST(la.ACC_TYPE AS VARCHAR(5)) ||
                                              CAST(la.ACC_SN AS VARCHAR(15))
                       OR ct.CUST_ID_1 = la.CUST_ID)

/* ===== COLLATERAL CURRENCY ===== */
         LEFT JOIN CURRENCY curr_coll
                   ON curr_coll.ID_CURRENCY = ct.CURRENCY_ID

/* ===== COLLATERAL TYPE ===== */
         LEFT JOIN GENERIC_DETAIL gd_coll
                   ON gd_coll.FK_GENERIC_HEADPAR = ct.GD_PAR_TYPE_1
                       AND gd_coll.SERIAL_NUM = ct.GD_SERIAL_NUM_1

WHERE
  -- Only active loan accounts
    la.ACC_STATUS IN ('1', '6')  -- Active statuses
  -- Only microfinance/digital credit products
  AND (
    p.PRODUCT_TYPE IN ('MICRO', 'SME', 'CONSUMER', 'AGRI')
        OR p.DESCRIPTION LIKE '%MICRO%'
        OR p.DESCRIPTION LIKE '%DIGITAL%'
        OR p.DESCRIPTION LIKE '%MOBILE%'
        OR la.ACC_LIMIT_AMN <= 50000000  -- Small loans (50M TZS or equivalent)
    )
  -- Exclude fully paid loans unless recently closed
  AND NOT (
    la.LOAN_STATUS = 'P'
        AND (la.NRM_CAP_BAL + la.OV_CAP_BAL) = 0
        AND la.LST_TRX_DT < CURRENT_DATE - 30 DAYS
    )

ORDER BY
    la.FK_UNITCODE,
    la.ACC_TYPE,
    la.ACC_SN;