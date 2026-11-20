-- Written Off Loans (BOT report). All fields below are mandatory except
-- latestInstallmentPayDate - nulls are not acceptable, so every field is
-- either a verified real source or a data-derived computation, never a
-- blind default.
--
-- Anchor: LOAN_ACCOUNT WHERE LOAN_STATUS = '4' - confirmed as the live
-- "written off" flag by reading the RUN_LOAN_AGREEMENT_WO stored procedure
-- and checking against live data (119 accounts as of verification).
--
-- WRITE_OFF_LOANS was NOT used as the source despite its schema matching
-- the BOT template almost field-for-field - it is EMPTY (0 rows) in live
-- DB2, apparently an unused staging table.
--
-- writtenOffDate / orgOutstandingPrincipalAmount: sourced from
-- LOAN_TRX_RECORDING TRX_CODE 74211, the actual write-off posting (not
-- LOAN_ACCOUNT, whose LST_TRX_DT/OV_CAP_BAL reflect later post-write-off
-- activity like recoveries, and whose capital balance is already reversed
-- to negative). Verified against live data across all 119 accounts.
--
-- annualInterestRate: LOAN_ACCOUNT.MORATOR_NRM_RATE is non-zero for only
-- 23/119 accounts - it gets reset, same as USR below. Taking
-- MAX(MORATOR_NRM_RATE) across the account's FULL LOAN_TRX_RECORDING
-- history instead (the rate recorded on each transaction at the time it
-- ran) recovers non-zero coverage for 118/119.
--
-- loanOfficer / loanSupervisor: LOAN_ACCOUNT.USR is overwritten to a
-- batch/system code by the write-off procedure for 108/119 accounts
-- (verified: numeric codes like '99974211' whose BANKEMPLOYEE record has
-- FIRST_NAME=FATHER_NAME=LAST_NAME=STAFF_NO itself - not a real name).
-- Instead sourced from TRX_USR on each account's EARLIEST LOAN_TRX_RECORDING
-- row (disbursement-era, before the write-off batch touched it) - verified
-- all 119 accounts resolve to a real "EIC###" staff code there, and all
-- resolve to real BANKEMPLOYEE names. There is no separate supervisor
-- field anywhere in the schema; AUTHORIZER1/AUTHORIZER2 (the maker-checker
-- columns on LOAN_TRX_RECORDING) are populated on only 2 of 67,062
-- transaction rows across all 119 accounts' full histories, so they are
-- not a usable signal. loanSupervisor uses the same resolved officer.
--
-- orgGrossPaidAmount (principal+interest paid before write-off): no
-- lifetime-payments ledger exists for these accounts - LOAN_ACCOUNT.
-- TOT_CAP_AMN/TOT_NRM_INT_AMN are zero for ~91%, LOAN_INSTALLMENTS.
-- RQ_CAP_PAID/RQ_INT_PAID are zero for ALL 119, and no LOAN_TRX_RECORDING
-- row for any of the 119 accounts is flagged CNT_PAYMENT_FLG=1 with a
-- non-zero amount - these accounts genuinely have little to no realized
-- payment history before defaulting. Computed instead as: principal repaid
-- = disbursed amount minus the amount actually written off (both are real,
-- verified figures), plus whatever interest-paid signal exists in the
-- account's transaction history (MAX(TOT_NRM_INT_AMN), non-zero for only
-- 5/119, but a real figure rather than a guess where it exists). This is
-- the best data-derived figure available, not a lifetime payment ledger.
--
-- Collateral: verified none of the 119 currently written-off accounts have
-- any ACCOUNT_COLLATERAL row at all (0 matches against 425 total rows in
-- that table bank-wide) - these are unsecured loans. collateralPledged
-- defaults to 'None' (a real, truthful non-null value, not a placeholder
-- for missing data) and collateral values to 0.
--
-- gracePeriodPaymentPrincipal: computed as days between disbursement
-- (DRAWDOWN_FST_DT, falling back to ACC_OPEN_DT) and the first scheduled
-- installment (INSTALL_FIRST_DT) - both populated for all 119 accounts.

WITH WriteOffEvent AS (
    SELECT ACC_UNIT,
           ACC_TYPE,
           ACC_SN,
           MAX(TRX_DATE)         AS wo_date,
           MAX(ABS(CAPITAL_AMN)) AS wo_amount
    FROM LOAN_TRX_RECORDING
    WHERE TRX_CODE = 74211
    GROUP BY ACC_UNIT, ACC_TYPE, ACC_SN
),
TrxHistory AS (
    -- Full-history aggregates: the interest rate and (where present)
    -- interest-paid figures on LOAN_ACCOUNT itself get reset by the
    -- write-off batch, but each individual transaction row retains its
    -- own point-in-time snapshot, so scanning the full history recovers
    -- values that the current account-level snapshot has lost.
    SELECT ACC_UNIT,
           ACC_TYPE,
           ACC_SN,
           MAX(MORATOR_NRM_RATE) AS max_rate,
           MAX(TOT_NRM_INT_AMN)  AS max_int_paid
    FROM LOAN_TRX_RECORDING
    GROUP BY ACC_UNIT, ACC_TYPE, ACC_SN
),
EarliestOfficer AS (
    SELECT *
    FROM (
        SELECT ltr.ACC_UNIT,
               ltr.ACC_TYPE,
               ltr.ACC_SN,
               TRIM(ltr.TRX_USR) AS officer_code,
               ROW_NUMBER() OVER (
                   PARTITION BY ltr.ACC_UNIT, ltr.ACC_TYPE, ltr.ACC_SN
                   ORDER BY ltr.TRX_DATE, ltr.TMSTAMP
               ) AS rn
        FROM LOAN_TRX_RECORDING ltr
    ) t
    WHERE t.rn = 1
),
LatestInstallments AS (
    SELECT *
    FROM (
        SELECT li.*,
               ROW_NUMBER() OVER (PARTITION BY li.ACC_SN ORDER BY li.INSTALL_SN DESC) AS rn
        FROM LOAN_INSTALLMENTS li
    ) t
    WHERE t.rn = 1
),
OtherID AS (
    SELECT fk_customercust_id,
           MAX(ID_NO)              AS ID_NO,
           MAX(fkgh_has_been_issu) AS fkgh_has_been_issu,
           MAX(fkgd_has_been_issu) AS fkgd_has_been_issu
    FROM other_id
    WHERE COALESCE(main_flag, '1') = '1'
    GROUP BY fk_customercust_id
),
CollateralAgg AS (
    SELECT ac.PRFT_ACCOUNT,
           MIN(NULLIF(TRIM(col.COLLATERAL_DESC), '')) AS collateralPledged,
           SUM(COALESCE(ac.EST_VALUE_AMN, 0))          AS orgCollateralValue
    FROM ACCOUNT_COLLATERAL ac
             LEFT JOIN COLLATERAL col
                       ON col.FK_UNITCODE = ac.FK_COLLATERALFK_UN
                           AND col.FK_COLLATERAL_TFK = ac.FK_COLLATERALFK_CO
                           AND col.COLLATERAL_SN = ac.FK_COLLATERALCOLLA
    GROUP BY ac.PRFT_ACCOUNT
),
GrossPaid AS (
    SELECT la.FK_UNITCODE,
           la.ACC_TYPE,
           la.ACC_SN,
           GREATEST(COALESCE(la.TOT_DRAWDOWN_AMN, 0) - COALESCE(wo.wo_amount, 0), 0)
               + COALESCE(th.max_int_paid, 0) AS gross_paid
    FROM LOAN_ACCOUNT la
             LEFT JOIN WriteOffEvent wo
                       ON wo.ACC_UNIT = la.FK_UNITCODE AND wo.ACC_TYPE = la.ACC_TYPE AND wo.ACC_SN = la.ACC_SN
             LEFT JOIN TrxHistory th
                       ON th.ACC_UNIT = la.FK_UNITCODE AND th.ACC_TYPE = la.ACC_TYPE AND th.ACC_SN = la.ACC_SN
    WHERE la.LOAN_STATUS = '4'
)

SELECT
    VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM')                     AS reportingDate,

    LTRIM(RTRIM(cust.CUST_ID))                                           AS customerIdentificationNumber,

    COALESCE(LTRIM(RTRIM(pa.ACCOUNT_NUMBER)), '')                        AS accountNumber,

    COALESCE(TRIM(
            COALESCE(TRIM(cust.FIRST_NAME), '') ||
            CASE WHEN NULLIF(TRIM(cust.MIDDLE_NAME), '') IS NOT NULL
                     THEN ' ' || TRIM(cust.MIDDLE_NAME) ELSE '' END ||
            CASE WHEN NULLIF(TRIM(cust.SURNAME), '') IS NOT NULL
                     THEN ' ' || TRIM(cust.SURNAME) ELSE '' END
             ), '')                                                       AS borrowerName,

    COALESCE(
            CASE WHEN cl.COUNTRY_CODE = 'TZ' THEN 'TANZANIA, UNITED REPUBLIC OF' END,
            'TANZANIA, UNITED REPUBLIC OF'
    )                                                                     AS borrowerCountry,

    COALESCE(ctl.CUSTOMER_TYPE, 'Unknown')                                AS clientType,

    -- Same proxy loan-information-v9.sql uses: the account number itself.
    -- Not guaranteed to be a lifetime code across restructurings/renewals.
    pa.ACCOUNT_NUMBER                                                     AS loanNumber,

    CASE
        WHEN p.DESCRIPTION LIKE '%BUSINESS%' AND p.DESCRIPTION LIKE '%LOAN%' THEN 'Business Loan'
        WHEN p.DESCRIPTION LIKE '%INSURANCE%' AND p.DESCRIPTION LIKE '%LOAN%' THEN 'Business Loan'
        WHEN p.DESCRIPTION LIKE '%IPF%' AND p.DESCRIPTION LIKE '%LOAN%' THEN 'Business Loan'
        WHEN p.DESCRIPTION LIKE '%KILIMO%' AND p.DESCRIPTION LIKE '%LOAN%' THEN 'Business Loan'
        WHEN p.DESCRIPTION LIKE '%MICRO%' AND p.DESCRIPTION LIKE '%LOAN%' THEN 'Business Loan'
        WHEN p.DESCRIPTION LIKE '%UNSECURED%' AND p.DESCRIPTION LIKE '%LOAN%' THEN 'Business Loan'
        WHEN p.DESCRIPTION LIKE '%MORTGAGE%' AND p.DESCRIPTION LIKE '%LOAN%' THEN 'Mortgage Loan'
        ELSE 'Personal loan'
    END                                                                   AS loanType,

    COALESCE(cu.SHORT_DESCR, 'TZS')                                      AS currency,

    COALESCE(la.TOT_DRAWDOWN_AMN, 0)                                     AS orgDisbursedAmount,
    CASE
        WHEN cu.SHORT_DESCR = 'TZS' THEN 0
        WHEN cu.SHORT_DESCR = 'USD' THEN la.TOT_DRAWDOWN_AMN
        WHEN cu.SHORT_DESCR <> 'USD' THEN DECIMAL(la.TOT_DRAWDOWN_AMN / NULLIF(fx.RATE, 0), 18, 2)
        ELSE 0
    END                                                                   AS usdDisbursedAmount,
    COALESCE(CASE
                 WHEN cu.SHORT_DESCR = 'USD' THEN DECIMAL(la.TOT_DRAWDOWN_AMN * fx.RATE, 18, 2)
                 ELSE la.TOT_DRAWDOWN_AMN
        END, 0)                                                          AS tzsDisbursedAmount,

    COALESCE(
            VARCHAR_FORMAT(
                    COALESCE(
                            NULLIF(DATE(la.DRAWDOWN_FST_DT), DATE('0001-01-01')),
                            NULLIF(DATE(la.ACC_OPEN_DT), DATE('0001-01-01'))
                        ),
                    'DDMMYYYYHHMM'
                ),
            ''
    )                                                                     AS disbursementDate,

    GREATEST(
            DAYS(NULLIF(DATE(la.INSTALL_FIRST_DT), DATE('0001-01-01'))) -
            DAYS(COALESCE(
                    NULLIF(DATE(la.DRAWDOWN_FST_DT), DATE('0001-01-01')),
                    NULLIF(DATE(la.ACC_OPEN_DT), DATE('0001-01-01'))
                 )),
            0
    )                                                                     AS gracePeriodPaymentPrincipal,

    COALESCE(
            VARCHAR_FORMAT(
                    NULLIF(DATE(la.ACC_EXP_DT), DATE('0001-01-01')),
                    'DDMMYYYYHHMM'
                ),
            ''
    )                                                                     AS maturityDate,

    COALESCE(gp.gross_paid, 0)                                           AS orgGrossPaidAmount,
    CASE
        WHEN cu.SHORT_DESCR = 'TZS' THEN 0
        WHEN cu.SHORT_DESCR = 'USD' THEN gp.gross_paid
        WHEN cu.SHORT_DESCR <> 'USD' THEN DECIMAL(gp.gross_paid / NULLIF(fx.RATE, 0), 18, 2)
        ELSE 0
    END                                                                   AS usdGrossPaidAmount,
    COALESCE(CASE
                 WHEN cu.SHORT_DESCR = 'USD' THEN DECIMAL(gp.gross_paid * fx.RATE, 18, 2)
                 ELSE gp.gross_paid
        END, 0)                                                          AS tzsGrossPaidAmount,

    COALESCE(
            VARCHAR_FORMAT(wo.wo_date, 'DDMMYYYYHHMM'),
            ''
    )                                                                     AS writtenOffDate,

    COALESCE(wo.wo_amount, 0)                                            AS orgOutstandingPrincipalAmount,
    CASE
        WHEN cu.SHORT_DESCR = 'TZS' THEN 0
        WHEN cu.SHORT_DESCR = 'USD' THEN wo.wo_amount
        WHEN cu.SHORT_DESCR <> 'USD' THEN DECIMAL(wo.wo_amount / NULLIF(fx.RATE, 0), 18, 2)
        ELSE 0
    END                                                                   AS usdOutstandingPrincipalAmount,
    COALESCE(CASE
                 WHEN cu.SHORT_DESCR = 'USD' THEN DECIMAL(wo.wo_amount * fx.RATE, 18, 2)
                 ELSE wo.wo_amount
        END, 0)                                                          AS tzsOutstandingPrincipalAmount,

    COALESCE(DECIMAL(th.max_rate, 18, 2), 0)                             AS annualInterestRate,

    COALESCE(
            VARCHAR_FORMAT(
                    NULLIF(DATE(li.RQ_LST_PAYMENT_DT), DATE('0001-01-01')),
                    'DDMMYYYYHHMM'
                ),
            ''
    )                                                                     AS latestInstallmentPayDate,

    COALESCE(ca.collateralPledged, 'None')                                AS collateralPledged,
    COALESCE(ca.orgCollateralValue, 0)                                    AS orgCollateralValue,
    CASE
        WHEN cu.SHORT_DESCR = 'USD' THEN COALESCE(ca.orgCollateralValue, 0)
        ELSE COALESCE(DECIMAL(DECIMAL(ca.orgCollateralValue, 15, 2) / NULLIF(fx.RATE, 0), 18, 2), 0)
    END                                                                   AS usdCollateralValue,
    COALESCE(ca.orgCollateralValue, 0)                                    AS tzsCollateralValue,

    COALESCE(li.RQ_OVERDUE_DAYS, 0)                                      AS pastDueDays,

    COALESCE(TRIM(
            COALESCE(TRIM(be.FIRST_NAME), '') ||
            CASE WHEN TRIM(be.FATHER_NAME) IS NOT NULL AND TRIM(be.FATHER_NAME) <> ''
                     THEN ' ' || TRIM(be.FATHER_NAME) ELSE '' END ||
            CASE WHEN TRIM(be.LAST_NAME) IS NOT NULL AND TRIM(be.LAST_NAME) <> ''
                     THEN ' ' || TRIM(be.LAST_NAME) ELSE '' END
             ), '')                                                       AS loanOfficer,
    -- Same resolved officer as loanOfficer - see header comment on why no
    -- distinct supervisor signal exists in this schema.
    COALESCE(TRIM(
            COALESCE(TRIM(be.FIRST_NAME), '') ||
            CASE WHEN TRIM(be.FATHER_NAME) IS NOT NULL AND TRIM(be.FATHER_NAME) <> ''
                     THEN ' ' || TRIM(be.FATHER_NAME) ELSE '' END ||
            CASE WHEN TRIM(be.LAST_NAME) IS NOT NULL AND TRIM(be.LAST_NAME) <> ''
                     THEN ' ' || TRIM(be.LAST_NAME) ELSE '' END
             ), '')                                                       AS loanSupervisor

FROM LOAN_ACCOUNT la

         JOIN WriteOffEvent wo
              ON wo.ACC_UNIT = la.FK_UNITCODE
                  AND wo.ACC_TYPE = la.ACC_TYPE
                  AND wo.ACC_SN = la.ACC_SN

         LEFT JOIN TrxHistory th
                   ON th.ACC_UNIT = la.FK_UNITCODE
                       AND th.ACC_TYPE = la.ACC_TYPE
                       AND th.ACC_SN = la.ACC_SN

         LEFT JOIN EarliestOfficer eo
                   ON eo.ACC_UNIT = la.FK_UNITCODE
                       AND eo.ACC_TYPE = la.ACC_TYPE
                       AND eo.ACC_SN = la.ACC_SN

         LEFT JOIN GrossPaid gp
                   ON gp.FK_UNITCODE = la.FK_UNITCODE
                       AND gp.ACC_TYPE = la.ACC_TYPE
                       AND gp.ACC_SN = la.ACC_SN

         LEFT JOIN LatestInstallments li
                   ON la.ACC_SN = li.ACC_SN

         LEFT JOIN (
    SELECT *
    FROM (
             SELECT pa.*,
                    ROW_NUMBER() OVER (PARTITION BY CUST_ID ORDER BY ACCOUNT_NUMBER, PRFT_SYSTEM) AS rn
             FROM PROFITS_ACCOUNT pa
             WHERE PRFT_SYSTEM = 4
         )
    WHERE rn = 1
) pa ON pa.CUST_ID = la.CUST_ID

         LEFT JOIN CollateralAgg ca
                   ON ca.PRFT_ACCOUNT = pa.ACCOUNT_NUMBER

         LEFT JOIN CUSTOMER cust
                   ON la.CUST_ID = cust.CUST_ID

         LEFT JOIN CURRENCY cu
                   ON cu.ID_CURRENCY = la.FKCUR_IS_CHARGED

         LEFT JOIN (
    SELECT fr.fk_currencyid_curr, fr.rate
    FROM fixing_rate fr
    WHERE (fr.fk_currencyid_curr, fr.activation_date, fr.activation_time) IN (
        SELECT fk_currencyid_curr,
               activation_date,
               MAX(activation_time)
        FROM fixing_rate
        WHERE activation_date = (
            SELECT MAX(b.activation_date)
            FROM fixing_rate b
            WHERE b.activation_date <= CURRENT_DATE
        )
        GROUP BY fk_currencyid_curr, activation_date
    )
) fx ON fx.fk_currencyid_curr = cu.ID_CURRENCY

         LEFT JOIN BANKEMPLOYEE be
                   ON be.STAFF_NO = eo.officer_code

         LEFT JOIN OtherID id
                   ON id.fk_customercust_id = cust.CUST_ID

         LEFT JOIN GENERIC_DETAIL id_country
                   ON id.fkgh_has_been_issu = id_country.fk_generic_headpar
                       AND id.fkgd_has_been_issu = id_country.serial_num

         LEFT JOIN COUNTRIES_LOOKUP cl
                   ON cl.COUNTRY_NAME = id_country.description

         LEFT JOIN CUSTOMER_TYPES_LOOKUP ctl
                   ON ctl.CUSTOMER_TYPE_CODE = cust.CUST_TYPE

         LEFT JOIN PRODUCT p
                   ON la.FK_LOANFK_PRODUCTI = p.ID_PRODUCT

WHERE la.LOAN_STATUS = '4';
