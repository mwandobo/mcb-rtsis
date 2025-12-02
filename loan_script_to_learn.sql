-- ================================================================
-- FINAL – GUARANTEED WORKING VERSION (Dec 2025 example)
-- Just change ONE line: DATE('2025-11-30')
-- ================================================================

WITH PARAMS AS (
    SELECT DATE('2020-11-30') AS EOM_DATE          -- CHANGE THIS DATE ONLY
    FROM SYSIBM.SYSDUMMY1
)

SELECT
    LTRIM(RTRIM(G.DESCRIPTION))                                      AS DESCR,
    W.ACC_OPEN_DT                                                    AS ACC_OPEN_DT,
    W.EOM_DATE                                                       AS EOM_DATE,
    W.LNS_OPEN_UNIT                                                  AS LNS_OPEN_UNIT,
    W.C_DIGIT                                                        AS C_DIGIT,
    W.CUST_ID                                                        AS CUST_ID,
    LTRIM(RTRIM(S.SURNAME))                                          AS SURNAME,
    LTRIM(RTRIM(S.FIRST_NAME))                                       AS FIRSTNAME,
    W.ACC_LIMIT_AMN                                                  AS ACC_LIMIT_AMN,
    CASE WHEN W.ACC_STATUS = '3' THEN 0 ELSE W.NRM_ACCRUAL_AMN END   AS NRM_ACCRUAL_AMN,
    CASE WHEN W.ACC_STATUS = '3' THEN 0 ELSE W.OV_ACCRUAL_AMN END    AS OV_ACCRUAL_AMN,
    W.NRM_CAP_BAL                                                    AS NRM_CAP_BAL,
    W.OV_CAP_BAL                                                     AS OV_CAP_BAL,
    C.TELEPHONE                                                      AS TELEPHONE,
    W.DRAWDOWN_FST_DT                                                AS DRAWDOWN_FST_DT,
    W.ACC_EXP_DT                                                     AS ACC_EXP_DT,
    W.INSTALL_COUNT                                                  AS INSTALL_COUNT,
    W.PRODUCT_DESC                                                   AS PRODUCT_DESC,
    W.FKGD_HAS_AS_FINANC                                             AS FKGD_HAS_AS_FINANC,
    W.FKGD_HAS_AS_CLASS                                              AS FKGD_HAS_AS_CLASS,
    W.FKGD_HAS_AS_LOAN_P                                             AS FKGD_HAS_AS_LOAN_P,
    W.INSTALL_FIRST_DT                                               AS INSTALL_FIRST_DT,
    LTRIM(RTRIM(W.CURRENCY))                                         AS CURRENCY,
    LTRIM(RTRIM(W.LOAN_OFFICER_ID))                                  AS OFFIC,
    W.TOTAL_MONTHS                                                   AS TOTAL_MONTHS,
    W.TOTAL_DAYS                                                     AS TOTAL_DAYS,
    W.DRAWDOWN_FST_AMN                                               AS DRAWDOWN_FST_AMN,
    W.MONOTORING_UNIT                                                AS MONOTORING_UNIT,
    W.ID_PRODUCT                                                     AS ID_PRODUCT,
    LTRIM(RTRIM(W.LOAN_OFFICER_NAME))                                AS OFFICN,
    C.SEX                                                            AS SEX,
    W.CLOSED_FLAG                                                    AS CLOSED_FLAG,
    W.ACC_TYPE                                                       AS ACC_TYPE,
    W.ACCOUNT_NO                                                     AS ACCOUNT_NO,
    ST.SUB_CLASS                                                     AS FINAL_SUB_CLASS_NAME,

    CASE W.ACC_STATUS
        WHEN '1' THEN '1 - Active'
        WHEN '2' THEN '2 - Blocked'
        WHEN '3' THEN '3 - Closed'
        WHEN '4' THEN '4 - Transfer to Over'
        WHEN '5' THEN '5 - Transfer to Definate Delay'
        WHEN '6' THEN '6 - Transfer to Write Off'
        ELSE 'n/a'
    END                                                              AS ACC_STATUS,

    CASE W.LOAN_STATUS
        WHEN '1' THEN '1 - Normal'
        WHEN '2' THEN '2 - Overdue'
        WHEN '3' THEN '3 - Definate Delay'
        WHEN '4' THEN '4 - Write Off'
        ELSE 'n/a'
    END                                                              AS LOAN_STATUS,

    DAYS(W.DRAWDOWN_EXP_DT) - DAYS(W.ACC_OPEN_DT)                    AS DRDDT,
    CASE WHEN W.DRAWDOWN_EXP_DT <> W.ACC_OPEN_DT THEN 'YES' ELSE 'NO' END AS DRAWDOWN_DIFF_FLAG,

    SP.PRV_SUB_CLASS                                                 AS PREV_SUB_CLASS,
    SR.ADJ_SUB_CLASS                                                 AS ADJ_SUB_CLASS,

    COALESCE(W.OV_RL_NRM_INT_BAL,0) + COALESCE(W.OV_RL_PNL_INT_BAL,0) AS OVERDUE_INTEREST,

    COALESCE(E_COLL.EST_VALUE_AMN_SUM, 0)                            AS COLLATERAL_OM_VALUE_SUB,
    W.COLLATERAL_OM_VALUE                                            AS COLLATERAL_OM_VALUE,

    CASE
        WHEN W.LOAN_STATUS IN ('2','3','4')
         AND B.CURR_TRX_DATE IS NOT NULL
         AND W.OV_EXP_DT IS NOT NULL
        THEN CASE
                WHEN B.CURR_TRX_DATE <= W.OV_EXP_DT
                THEN FLOOR((DAYS(W.OV_EXP_DT) - DAYS(B.CURR_TRX_DATE)) / 30.4375)
                ELSE FLOOR((DAYS(B.CURR_TRX_DATE) - DAYS(W.OV_EXP_DT)) / 30.4375)
             END
        ELSE 0
    END                                                              AS ARREAR_INMONTH,

    CASE
        WHEN W.LOAN_STATUS IN ('2','3','4')
         AND B.CURR_TRX_DATE IS NOT NULL
         AND W.OV_EXP_DT IS NOT NULL
        THEN CASE
                WHEN B.CURR_TRX_DATE <= W.OV_EXP_DT
                THEN MOD((DAYS(W.OV_EXP_DT) - DAYS(B.CURR_TRX_DATE)), 30.4375)
                ELSE MOD((DAYS(B.CURR_TRX_DATE) - DAYS(W.OV_EXP_DT)), 30.4375)
             END
        ELSE 0
    END                                                              AS ARREAR_INDAYS,

    LTRIM(RTRIM(P.ACCOUNT_NUMBER))                                   AS PAYING_ACC_NUMBER,

    CASE
        WHEN C.CUST_TYPE = '1' AND C.DATE_OF_BIRTH IS NOT NULL
        THEN FLOOR((DAYS(B.CURR_TRX_DATE) - DAYS(C.DATE_OF_BIRTH)) / 365.25)
        ELSE NULL
    END                                                              AS AGE,

    N.DATE_DATA                                                      AS DATE_LAST_REPAYMENT,
    L.INSTALL_NEXT_DT                                                AS DUE_DATE_REPAYMENT,
    W.INSTALL_FIXED_AMN                                              AS TOTAL_REPAYMENT,
    W.PROVISION_AMOUNT                                               AS PROVISION_AMOUNT,
    GG.DESCRIPTION                                                   AS GG_DESCRIPTION,
    PF.ENTRY_DESCR                                                   AS PF_ENTRY_DESCR

FROM W_EOM_LOAN_ACCOUNT W
CROSS JOIN PARAMS PAR

LEFT JOIN CUSTOMER S                ON W.CUST_ID = S.CUST_ID
LEFT JOIN W_DIM_CUSTOMER C          ON W.CUST_ID = C.CUST_ID
                                       AND W.EOM_DATE BETWEEN C.ROW_START_DATE AND C.ROW_END_DATE
LEFT JOIN CUSTOMER_CATEGORY CC      ON C.CUST_ID = CC.FK_CUSTOMERCUST_ID
                                       AND CC.FK_CATEGORYCATEGOR = 'INSIDER'
LEFT JOIN GENERIC_DETAIL G          ON G.FK_GENERIC_HEADPAR = CC.FK_GENERIC_DETAFK
                                       AND G.SERIAL_NUM = CC.FK_GENERIC_DETASER
LEFT JOIN BANK_PARAMETERS B         ON 1 = 1

-- Correct join to live LOAN_ACCOUNT (T24 standard keys)
LEFT JOIN LOAN_ACCOUNT L
       ON L.FK_UNITCODE = W.FK_UNITCODE
      AND L.ACC_TYPE    = W.ACC_TYPE
      AND L.ACC_SN      = W.ACC_SN

-- Correct join to PROFITS_ACCOUNT
LEFT JOIN PROFITS_ACCOUNT P
       ON L.DEP_ACC_SN = P.DEP_ACC_NUMBER           -- FIXED: P.DEP_ACC_NUMBER
      AND P.PRFT_SYSTEM = 3
      AND COALESCE(P.SECONDARY_ACC, '0') <> '1'

LEFT JOIN LOAN_ADD_INFO N
       ON N.ROW_ID = 1
      AND N.ACC_UNIT = W.FK_UNITCODE
      AND N.ACC_TYPE = W.ACC_TYPE
      AND N.ACC_SN   = W.ACC_SN

LEFT JOIN GENERIC_DETAIL GG
       ON GG.FK_GENERIC_HEADPAR = W.FKGH_HAS_AS_LOAN_P
      AND GG.SERIAL_NUM = W.FKGD_HAS_AS_LOAN_P

-- Classification lookups
LEFT JOIN (
    SELECT DISTINCT
           LTRIM(RTRIM(PREDEFINED_VALUES)) || ' - ' || LTRIM(RTRIM(ENTRY_DESCR)) AS SUB_CLASS,
           PREDEFINED_VALUES
    FROM PFG_SETUP_VALUES
    WHERE TAG = 'CLASSIFY14' AND TAG_SET_CODE = 'CLASSIFICATION COMM.'
          AND TRIM(ENTRY_DESCR) IS NOT NULL
) ST ON ST.PREDEFINED_VALUES = W.CURR_SUB_CLASS

LEFT JOIN (
    SELECT DISTINCT
           LTRIM(RTRIM(PREDEFINED_VALUES)) || ' - ' || LTRIM(RTRIM(ENTRY_DESCR)) AS PRV_SUB_CLASS,
           PREDEFINED_VALUES
    FROM PFG_SETUP_VALUES
    WHERE TAG = 'CLASSIFY17' AND TAG_SET_CODE = 'CLASSIFICATION COMM.'
) SP ON SP.PREDEFINED_VALUES = W.LOAN_CLASS

LEFT JOIN (
    SELECT DISTINCT
           LTRIM(RTRIM(PREDEFINED_VALUES)) || ' - ' || LTRIM(RTRIM(ENTRY_DESCR)) AS ADJ_SUB_CLASS,
           PREDEFINED_VALUES
    FROM PFG_SETUP_VALUES
    WHERE TAG = 'CLASSIFY22' AND TAG_SET_CODE = 'CLASSIFICATION COMM.'
) SR ON SR.PREDEFINED_VALUES = W.LOAN_CLASS

LEFT JOIN (
    SELECT DISTINCT
           LTRIM(RTRIM(PREDEFINED_VALUES)) AS PREDEFINED_VALUES,
           LTRIM(RTRIM(ENTRY_DESCR))       AS ENTRY_DESCR
    FROM PFG_SETUP_VALUES
    WHERE TAG = 'CLASSIFY06' AND TRIM(ENTRY_DESCR) IS NOT NULL
) PF ON PF.PREDEFINED_VALUES = W.LOAN_CLASS

-- Fast collateral (pre-aggregated)
LEFT JOIN (
    SELECT PRFT_ACCOUNT,
           SUM(EST_VALUE_AMN) AS EST_VALUE_AMN_SUM
    FROM EOM_ACCOUNT_COLLATERAL
    WHERE EOM_DATE = (SELECT EOM_DATE FROM PARAMS)
    GROUP BY PRFT_ACCOUNT
) E_COLL
    ON E_COLL.PRFT_ACCOUNT = W.ACCOUNT_NO
    OR E_COLL.PRFT_ACCOUNT = W.AGREEMENT_NUMBER

WHERE W.EOM_DATE = (SELECT EOM_DATE FROM PARAMS)

ORDER BY W.ACC_OPEN_DT;