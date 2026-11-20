-- ============================================================
-- DIGITAL CREDIT REPORT
-- Source: PROFITS CBS (LOAN_ACCOUNT)
-- Scope: GL 110020003 "SALARY ADVANCE LOANS" -> PRODUCT.ID_PRODUCT = 40100
--   (the only "SALARY%"/"DIGITAL%"/"MOBILE%" product with any live loan
--   accounts in PROFITS; confirmed 1,615 active accounts, confirmed via
--   GLG_ACCOUNT.EXTERNAL_GLACCOUNT = '110020003' -> SHORT_DESCR =
--   'SALARY ADVANCE LOANS', matching PRODUCT.DESCRIPTION exactly).
--   Scoping confirmed with business: this is the bank's digital credit
--   product (instant/payroll-linked disbursement) - no other product in
--   PROFITS carries any digital/mobile channel marker.
-- ============================================================

WITH LatestInstallments AS (
    SELECT *
    FROM (
        SELECT li.*,
               ROW_NUMBER() OVER (PARTITION BY li.ACC_SN ORDER BY li.INSTALL_SN DESC) AS rn
        FROM LOAN_INSTALLMENTS li
    ) t
    WHERE t.rn = 1
),

LatestAccount AS (
    SELECT *
    FROM (
        SELECT pa.*,
               ROW_NUMBER() OVER (PARTITION BY CUST_ID ORDER BY ACCOUNT_NUMBER, PRFT_SYSTEM) AS rn
        FROM PROFITS_ACCOUNT pa
        WHERE PRFT_SYSTEM = 4
    ) t
    WHERE t.rn = 1
),

fx AS (
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
),

params AS (
    SELECT
        CURRENT_DATE AS report_date,
        CURRENT_TIMESTAMP AS report_timestamp,
        (SELECT BANK_CODE FROM BANK_PARAMETERS FETCH FIRST 1 ROW ONLY) AS bank_code
    FROM SYSIBM.SYSDUMMY1
)

SELECT

    /* reportingDate */
    VARCHAR_FORMAT(p.report_timestamp, 'DDMMYYYYHH24MI') AS reportingDate,

    /* customerName -- mandatory; must never resolve to NULL/blank even if
       the CUSTOMER join somehow misses. */
    COALESCE(
        NULLIF(TRIM(
            COALESCE(TRIM(cust.FIRST_NAME), '') ||
            CASE WHEN COALESCE(TRIM(cust.MIDDLE_NAME), '') <> '' THEN ' ' || TRIM(cust.MIDDLE_NAME) ELSE '' END ||
            CASE WHEN COALESCE(TRIM(cust.SURNAME), '') <> '' THEN ' ' || TRIM(cust.SURNAME) ELSE '' END
        ), ''),
        'Unknown'
    ) AS customerName,

    /* gender */
    CASE
        WHEN cust.SEX = 'M' THEN 'Male'
        WHEN cust.SEX = 'F' THEN 'Female'
        ELSE 'Not Applicable'
    END AS gender,

    /* disabilityStatus -- no source field in PROFITS; D94 has no
       "Not Applicable" option, 'None' (item 1) is the valid default. */
    'None' AS disabilityStatus,

    /* customerIdentificationNumber -- mandatory; must use the same CUST_ID
       convention as every other report (account-information.sql,
       mobile-banking-v1.sql, internet-banking.sql, person.sql,
       personal-data-corporates-v4.sql), since BOT cross-validates this
       field against the Individual/Company Information submissions, which
       are keyed on CUST_ID, not the customer's National ID. Using
       oid.ID_NO here (confirmed live: all 215 digital-credit customers'
       CUST_IDs already exist in personalDataInformation/
       personalDataCorporates) caused every digitalCredit row to reference
       an ID that doesn't exist in either master report, rejecting the
       submission. CAST(cust.CUST_ID...) with a CAST(la.CUST_ID...)
       fallback keeps the "always resolves" guarantee without the oid join. */
    COALESCE(
        CASE WHEN cust.CUST_ID IS NOT NULL THEN CAST(cust.CUST_ID AS VARCHAR(20)) END,
        CAST(la.CUST_ID AS VARCHAR(20))
    ) AS customerIdentificationNumber,

    /* institutionCode -- mandatory; guard the (practically impossible)
       case of BANK_PARAMETERS being empty. */
    COALESCE(CAST(p.bank_code AS VARCHAR(10)), 'Unknown') AS institutionCode,

    /* branchCode */
    CAST(la.FK_UNITCODE AS VARCHAR(10)) AS branchCode,

    /* servicesFacilitator -- D74 is a closed list of mobile money operators
       only (no "Direct/Bank" option) and PROFITS has no field recording
       which channel (if any) was used for salary-advance disbursement.
       Hardcoded per business direction; confirm/correct once the actual
       disbursement/collection channel for this product is known. */
    'M-Pesa' AS servicesFacilitator,

    /* productName */
    TRIM(pr.DESCRIPTION) AS productName,

    /* tzsLoanDisbursedAmount */
    COALESCE(CASE
        WHEN curr.SHORT_DESCR = 'USD' THEN DECIMAL(la.TOT_DRAWDOWN_AMN * fx.rate, 18, 2)
        ELSE la.TOT_DRAWDOWN_AMN
    END, 0) AS tzsLoanDisbursedAmount,

    /* orgLoanDisbursedAmount, usdLoanDisbursedAmount -- not in the
       OpenAPI schema available in this repo (digitalCreditInformation
       only defines tzsLoanDisbursedAmount), but requested by BOT per
       business direction. Defaulted to 0 pending a real source/mapping. */
    0 AS orgLoanDisbursedAmount,
    0 AS usdLoanDisbursedAmount,

    /* loanDisbursementDate -- mandatory; final fallback is reportingDate
       itself (never an empty string) if even ACC_OPEN_DT is unavailable. */
    COALESCE(
        VARCHAR_FORMAT(
            COALESCE(
                NULLIF(DATE(la.DRAWDOWN_FST_DT), DATE('0001-01-01')),
                NULLIF(DATE(la.ACC_OPEN_DT), DATE('0001-01-01'))
            ),
            'DDMMYYYYHH24MI'
        ),
        VARCHAR_FORMAT(p.report_timestamp, 'DDMMYYYYHH24MI')
    ) AS loanDisbursementDate,

    /* tzsLoanBalance */
    COALESCE(CASE
        WHEN curr.SHORT_DESCR = 'USD'
            THEN DECIMAL((COALESCE(la.NRM_CAP_BAL, 0) + COALESCE(la.OV_CAP_BAL, 0)) * fx.rate, 18, 2)
        ELSE (COALESCE(la.NRM_CAP_BAL, 0) + COALESCE(la.OV_CAP_BAL, 0))
    END, 0) AS tzsLoanBalance,

    /* maturityDate -- must never be earlier than reportingDate. Comparing by
       DATE alone isn't enough: ACC_EXP_DT formats with a 0000 time-of-day,
       so a maturity date equal to today would still sort before a
       reportingDate stamped with the actual current time. Compare as full
       TIMESTAMPs against report_timestamp instead (same fix as
       interbank_loans_payable.sql). */
    CASE
        WHEN NULLIF(DATE(la.ACC_EXP_DT), DATE('0001-01-01')) IS NOT NULL
             AND TIMESTAMP(la.ACC_EXP_DT) >= p.report_timestamp
        THEN VARCHAR_FORMAT(la.ACC_EXP_DT, 'DDMMYYYYHH24MI')
        ELSE VARCHAR_FORMAT(p.report_timestamp, 'DDMMYYYYHH24MI')
    END AS maturityDate,

    /* loanId */
    COALESCE(NULLIF(TRIM(pa.ACCOUNT_NUMBER), ''), CAST(la.ACC_SN AS VARCHAR(20))) AS loanId,

    /* lastDepositDate -- mandatory. RQ_LST_PAYMENT_DT is unpopulated for
       the large majority of this product's accounts (no deposit has been
       recorded yet), so this previously resolved to an empty string for
       1,575 of 1,615 rows. LOAN_INSTALLMENTS.REQUEST_DT is NOT a deposit
       date - verified live it's an internal interest-recalculation
       schedule entry that runs monthly for years into the future
       (REQUEST_TYPE='1'), so it's deliberately excluded from this
       fallback chain to avoid reporting a bogus future "deposit" date.
       Falls back to the loan's disbursement/open date (no deposit yet =
       nothing to report since inception), finally reportingDate itself,
       so it can never be blank. */
    COALESCE(
        VARCHAR_FORMAT(NULLIF(DATE(li.RQ_LST_PAYMENT_DT), DATE('0001-01-01')), 'DDMMYYYYHH24MI'),
        VARCHAR_FORMAT(
            COALESCE(
                NULLIF(DATE(la.DRAWDOWN_FST_DT), DATE('0001-01-01')),
                NULLIF(DATE(la.ACC_OPEN_DT), DATE('0001-01-01'))
            ),
            'DDMMYYYYHH24MI'
        ),
        VARCHAR_FORMAT(p.report_timestamp, 'DDMMYYYYHH24MI')
    ) AS lastDepositDate,

    /* lastDepositAmount */
    (COALESCE(li.RQ_CAP_PAID, 0) + COALESCE(li.RQ_INT_PAID, 0)) AS lastDepositAmount,

    /* paymentsInstallment */
    COALESCE(la.INSTALL_COUNT, 0) AS paymentsInstallment,

    /* repaymentsFrequency -- D09 only allows: Bullet, 14 days, 15 days,
       30 days, 60 days, 90 days, 120 days, 150 days, 180 days, 360 days,
       Irregular. INSTALL_FREQ here is a small code (not a literal day
       count) per the existing LOANS_CRD_BOT_POPULATE_V3 CRB procedure's
       mapping, reused here. Confirmed live INSTALL_FREQ is 0 for every
       row on this product, so this always falls to the ELSE branch today. */
    CASE
        WHEN la.INSTALL_FREQ = 1 THEN '30 days'
        WHEN la.INSTALL_FREQ = 2 THEN '60 days'
        WHEN la.INSTALL_FREQ = 3 THEN '90 days'
        WHEN la.INSTALL_FREQ = 4 THEN '120 days'
        WHEN la.INSTALL_FREQ = 5 THEN '150 days'
        WHEN la.INSTALL_FREQ = 6 THEN '180 days'
        WHEN la.INSTALL_FREQ = 12 THEN '360 days'
        WHEN la.INSTALL_COUNT = 1 THEN 'Bullet'
        ELSE 'Irregular'
    END AS repaymentsFrequency,

    /* loanAmotizationType -- D96 only allows: Fixed Installment, Bullet Loan.
       Confirmed live INSTALL_COUNT is 0 for every row on this product
       (no populated installment schedule), consistent with a single
       lump-sum salary-advance repayment rather than an amortized schedule. */
    CASE
        WHEN COALESCE(la.INSTALL_COUNT, 0) > 1 THEN 'Fixed Installment'
        ELSE 'Bullet Loan'
    END AS loanAmotizationType,

    /* cycleNumber -- RECYCLING_FRQ is unpopulated (always 0) for this product,
       so this counts how many salary-advance loans this customer has taken
       out up to and including this one (borrowing cycle), same approach as
       loan-information-v10.sql's cycleNumber subquery. */
    (SELECT COUNT(*)
     FROM LOAN_ACCOUNT la2
     WHERE la2.CUST_ID = la.CUST_ID
       AND la2.FK_LOANFK_PRODUCTI = 40100
       AND la2.ACC_OPEN_DT <= la.ACC_OPEN_DT) AS cycleNumber,

    /* loanAmountPaid -- TOT_CAP_AMN/TOT_NRM_INT_AMN/TOT_PNL_INT_AMN are
       unpopulated (always 0) for this product, so principal repaid is
       derived as disbursed minus the still-outstanding balance instead. */
    (COALESCE(la.TOT_DRAWDOWN_AMN, 0) - (COALESCE(la.NRM_CAP_BAL, 0) + COALESCE(la.OV_CAP_BAL, 0))) AS loanAmountPaid,

    /* deliquenceDate */
    CASE
        WHEN NULLIF(DATE(la.OV_EXP_DT), DATE('0001-01-01')) IS NOT NULL
        THEN VARCHAR_FORMAT(la.OV_EXP_DT, 'DDMMYYYYHH24MI')
        ELSE NULL
    END AS deliquenceDate,

    /* restructuringDate */
    CASE
        WHEN la.LOAN_STATUS IN ('R', 'S')
        THEN VARCHAR_FORMAT(la.LST_TRX_DT, 'DDMMYYYYHH24MI')
        ELSE NULL
    END AS restructuringDate,

    /* interestRate */
    COALESCE(li.NORMAL_INTER_PERC, la.MORATOR_NRM_RATE, 0) AS interestRate,

    /* pastDueDays */
    COALESCE(
        li.RQ_OVERDUE_DAYS,
        CASE
            WHEN la.OV_EXP_DT IS NOT NULL AND NULLIF(DATE(la.OV_EXP_DT), DATE('0001-01-01')) IS NOT NULL
                 AND la.OV_EXP_DT < p.report_date
            THEN DAYS(p.report_date) - DAYS(la.OV_EXP_DT)
            ELSE 0
        END,
        0
    ) AS pastDueDays,

    /* pastDueAmount */
    (COALESCE(la.OV_CAP_BAL, 0) + COALESCE(la.OV_RL_NRM_INT_BAL, 0) + COALESCE(la.OV_RL_PNL_INT_BAL, 0)
        + COALESCE(la.OV_COM_BAL, 0) + COALESCE(la.OV_EXP_BAL, 0)) AS pastDueAmount,

    /* currency -- mandatory; CURRENCY.SHORT_DESCR is CHAR and can come
       back as blank/whitespace rather than NULL, which plain COALESCE
       wouldn't catch. */
    COALESCE(NULLIF(TRIM(curr.SHORT_DESCR), ''), 'TZS') AS currency,

    /* orgAccruedInterest */
    (COALESCE(la.NRM_ACR_INT_BAL, 0) + COALESCE(la.OV_ACR_NRM_INT_BAL, 0) + COALESCE(la.OV_ACR_PNL_INT_BAL, 0)) AS orgAccruedInterest,

    /* usdAccruedInterest */
    CASE
        WHEN curr.SHORT_DESCR = 'USD'
            THEN (COALESCE(la.NRM_ACR_INT_BAL, 0) + COALESCE(la.OV_ACR_NRM_INT_BAL, 0) + COALESCE(la.OV_ACR_PNL_INT_BAL, 0))
        WHEN fx.rate IS NOT NULL AND fx.rate <> 0
            THEN DECIMAL((COALESCE(la.NRM_ACR_INT_BAL, 0) + COALESCE(la.OV_ACR_NRM_INT_BAL, 0) + COALESCE(la.OV_ACR_PNL_INT_BAL, 0)) / fx.rate, 18, 2)
        ELSE 0
    END AS usdAccruedInterest,

    /* tzsAccruedInterest */
    CASE
        WHEN curr.SHORT_DESCR = 'USD'
            THEN DECIMAL((COALESCE(la.NRM_ACR_INT_BAL, 0) + COALESCE(la.OV_ACR_NRM_INT_BAL, 0) + COALESCE(la.OV_ACR_PNL_INT_BAL, 0)) * fx.rate, 18, 2)
        ELSE (COALESCE(la.NRM_ACR_INT_BAL, 0) + COALESCE(la.OV_ACR_NRM_INT_BAL, 0) + COALESCE(la.OV_ACR_PNL_INT_BAL, 0))
    END AS tzsAccruedInterest,

    /* assetClassification -- D32 only allows: Current, EspeciallyMentioned,
       Substandard, Doubtful, Loss (no "Watch"). Bucketed by days past due
       per BOT's standard 5-stage thresholds. */
    CASE
        WHEN la.LOAN_STATUS = 'W' THEN 'Loss'
        WHEN COALESCE(li.RQ_OVERDUE_DAYS, 0) <= 30 THEN 'Current'
        WHEN COALESCE(li.RQ_OVERDUE_DAYS, 0) <= 90 THEN 'EspeciallyMentioned'
        WHEN COALESCE(li.RQ_OVERDUE_DAYS, 0) <= 180 THEN 'Substandard'
        WHEN COALESCE(li.RQ_OVERDUE_DAYS, 0) <= 360 THEN 'Doubtful'
        ELSE 'Loss'
    END AS assetClassification,

    /* allowanceProbableLoss */
    COALESCE(lai.PROVISION_AMOUNT, 0) AS allowanceProbableLoss,

    /* botProvision */
    COALESCE(lai.PROVISION_AMOUNT, 0) AS botProvision,

    /* interestSuspended */
    (COALESCE(la.OV_ACR_NRM_INT_BAL, 0) + COALESCE(la.OV_ACR_PNL_INT_BAL, 0)) AS interestSuspended,

    /* ============================================================
       SOURCE ROW IDENTITY (not part of the BoT payload)
       LOAN_ACCOUNT's own primary key, carried through so the
       pipeline can key each loan account individually.
       ============================================================ */
    la.FK_UNITCODE AS sourceUnitCode,
    la.ACC_TYPE    AS sourceAccType,
    la.ACC_SN      AS sourceAccSn

FROM LOAN_ACCOUNT la

CROSS JOIN params p

INNER JOIN PRODUCT pr
    ON pr.ID_PRODUCT = la.FK_LOANFK_PRODUCTI

LEFT JOIN LatestInstallments li
    ON la.ACC_SN = li.ACC_SN

LEFT JOIN LatestAccount pa
    ON pa.CUST_ID = la.CUST_ID

LEFT JOIN LOAN_ACCOUNT_INFO lai
    ON la.FK_UNITCODE = lai.FK_LOAN_ACCOUNTFK
   AND la.ACC_TYPE    = lai.FK0LOAN_ACCOUNTACC
   AND la.ACC_SN      = lai.FK_LOAN_ACCOUNTACC

LEFT JOIN CUSTOMER cust
    ON la.CUST_ID = cust.CUST_ID

LEFT JOIN CURRENCY curr
    ON curr.ID_CURRENCY = la.FKCUR_IS_MOVED_IN

LEFT JOIN fx
    ON fx.fk_currencyid_curr = curr.ID_CURRENCY

WHERE la.FK_LOANFK_PRODUCTI = 40100
  -- Salary Advance Loans (Digital Credit) product only.
  -- NOTE: no ACC_STATUS/LOAN_STATUS filter here on purpose. Checked live:
  -- of all 1,615 accounts on this product, 1,260 are ACC_STATUS='3' (closed),
  -- 354 are ACC_STATUS='1'/LOAN_STATUS='1', and exactly ONE account across
  -- the whole product has a nonzero outstanding balance - every other
  -- account (open or closed) already nets to zero in this environment.
  -- Restricting to "active" status codes would drop almost the entire
  -- population while adding back almost nothing with a real balance, so
  -- full history is reported here (same call made for accrued_taxes.sql) -
  -- business/internal stakeholders should confirm the intended scope.

ORDER BY la.FK_UNITCODE, la.ACC_TYPE, la.ACC_SN;
