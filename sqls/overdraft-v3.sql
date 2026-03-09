WITH CollateralAgg AS (SELECT COLLTBL_CUST_ID,
                              '[' || LISTAGG(
                                      '{'
                                          || '"collateralPledged":"' || CASE
                                                                            WHEN FK_COLLATERAL_TFK = 20030
                                                                                THEN 'MotorVehicles'
                                                                            ELSE 'Cash'
                                          END || '",'
                                          || '"orgCollateralValue":' || COALESCE(TOT_EST_VALUE_AMN, 0) || ','
                                          || '"usdCollateralValue":' ||
                                      CAST(COALESCE(TOT_EST_VALUE_AMN, 0) / 2500 AS DECIMAL(15, 2)) || ','
                                          || '"tzsCollateralValue":' || COALESCE(TOT_EST_VALUE_AMN, 0)
                                          || '}',
                                      ','
                                     ) || ']' AS collateralPledged
                       FROM COLLATERAL
                       GROUP BY COLLTBL_CUST_ID)
SELECT *
FROM (SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHH24MI')                        AS reportingDate,
             LTRIM(RTRIM(pa.ACCOUNT_NUMBER))                                            AS accountNumber,
             LTRIM(RTRIM(gte.CUST_ID))                                                  AS customerIdentificationNumber,
             TRIM(
                     COALESCE(TRIM(c.FIRST_NAME), '') ||
                     CASE
                         WHEN c.MIDDLE_NAME IS NOT NULL AND TRIM(c.MIDDLE_NAME) <> ''
                             THEN ' ' || c.MIDDLE_NAME
                         ELSE ''
                         END ||
                     CASE
                         WHEN c.SURNAME IS NOT NULL AND TRIM(c.SURNAME) <> ''
                             THEN ' ' || c.SURNAME
                         ELSE ''
                         END
             )                                                                          AS clientName,
             CASE
                 WHEN gte.ID_PRODUCT = 40034 THEN 'Corporations'
                 WHEN gte.ID_PRODUCT = 40030 THEN 'Staff'
                 WHEN gte.ID_PRODUCT IN (40035, 40037) THEN 'Individual'
                 END                                                                    AS clientType,
             CASE
                 WHEN cl.COUNTRY_CODE = 'TZ' THEN 'TANZANIA, UNITED REPUBLIC OF'
                 ELSE 'TANZANIA, UNITED REPUBLIC OF'
                 END                                                                    AS borrowerCountry,
             CAST(0 AS SMALLINT)                                                        AS ratingStatus,
             'BBB+ to BBB-'                                                             AS crRatingBorrower,
             'Grade A'                                                                  AS gradesUnratedBanks,
             NULL                                                                       AS groupCode,
             NULL                                                                       AS relatedEntityName,
             NULL                                                                       AS relatedParty,
             NULL                                                                       AS relationshipCategory,
             p.DESCRIPTION                                                              AS loanProductType,
             'OtherServices'                                                            AS overdraftEconomicActivity,

             CASE la.ACC_STATUS
                 WHEN '1' THEN 'Existing'
                 WHEN '2' THEN 'TerminatedInAdvanceCorrectly'
                 WHEN '3' THEN 'TerminatedAccordingTheContract'
                 WHEN '4' THEN 'TerminatedInAdvanceIncorrectly'
                 WHEN '5' THEN 'TerminatedInAdvanceIncorrectly'
                 WHEN '6' THEN 'TerminatedInAdvanceIncorrectly'
                 ELSE 'TerminatedAccordingTheContract'
                 END                                                                    AS loanPhase,
             'NotSpecified'                                                             AS transferStatus,

             CASE p.DESCRIPTION
                 WHEN 'STAFF CURRENT ACCOUNT OVERDRAFT' THEN 'StaffLoan'
                 WHEN 'OVERDRAFT-CORPORATE' THEN 'Business'
                 WHEN 'NGO/CLUB ACCOUNT OVERDRAFT' THEN 'Development'
                 ELSE 'Other'
                 END                                                                    AS purposeOtherLoans,

             VARCHAR_FORMAT(la.ACC_OPEN_DT, 'DDMMYYYYHHMM')                             AS contractDate,
             la.FK_UNITCODE                                                             AS branchCode,

             TRIM(
                     COALESCE(TRIM(be.FIRST_NAME), '') ||
                     CASE
                         WHEN TRIM(be.FATHER_NAME) IS NOT NULL AND TRIM(be.FATHER_NAME) <> ''
                             THEN ' ' || TRIM(be.FATHER_NAME)
                         ELSE ''
                         END ||
                     CASE
                         WHEN TRIM(be.LAST_NAME) IS NOT NULL AND TRIM(be.LAST_NAME) <> ''
                             THEN ' ' || TRIM(be.LAST_NAME)
                         ELSE ''
                         END
             )                                                                          AS loanOfficer,

             TRIM(
                     COALESCE(TRIM(be.FIRST_NAME), '') ||
                     CASE
                         WHEN TRIM(be.FATHER_NAME) IS NOT NULL AND TRIM(be.FATHER_NAME) <> ''
                             THEN ' ' || TRIM(be.FATHER_NAME)
                         ELSE ''
                         END ||
                     CASE
                         WHEN TRIM(be.LAST_NAME) IS NOT NULL AND TRIM(be.LAST_NAME) <> ''
                             THEN ' ' || TRIM(be.LAST_NAME)
                         ELSE ''
                         END
             )                                                                          AS loanSupervisor,

             gte.CURRENCY_SHORT_DES                                                     AS currency,
             ag.AGR_LIMIT                                                               AS orgSanctionedAmount,

             CASE
                 WHEN gte.CURRENCY_SHORT_DES = 'USD'
                     THEN ag.AGR_LIMIT
                 WHEN gte.CURRENCY_SHORT_DES = 'TZS' THEN 0
                 WHEN gte.CURRENCY_SHORT_DES <> 'USD'
                     THEN DECIMAL(ag.AGR_LIMIT / fx.RATE, 18, 2)
                 ELSE NULL
                 END                                                                    AS usdSanctionedAmount,

             CASE
                 WHEN gte.CURRENCY_SHORT_DES = 'USD'
                     THEN DECIMAL(ag.AGR_LIMIT * fx.RATE, 18, 2)
                 ELSE ag.AGR_LIMIT
                 END                                                                    AS tzsSanctionedAmount,

             la.TOT_DRAWDOWN_AMN                                                        AS orgUtilisedAmount,

             CASE
                 WHEN gte.CURRENCY_SHORT_DES = 'USD'
                     THEN la.TOT_DRAWDOWN_AMN
                 WHEN gte.CURRENCY_SHORT_DES = 'TZS' THEN 0
                 WHEN gte.CURRENCY_SHORT_DES <> 'USD'
                     THEN DECIMAL(la.TOT_DRAWDOWN_AMN / fx.RATE, 18, 2)
                 ELSE NULL
                 END                                                                    AS usdUtilisedAmount,

             CASE
                 WHEN gte.CURRENCY_SHORT_DES = 'USD'
                     THEN DECIMAL(la.TOT_DRAWDOWN_AMN * fx.RATE, 18, 2)
                 ELSE la.TOT_DRAWDOWN_AMN
                 END                                                                    AS tzsUtilisedAmount,

             gte.DC_AMOUNT                                                              AS orgCrUsageLast30DaysAmount,

             CASE
                 WHEN gte.CURRENCY_SHORT_DES = 'USD'
                     THEN gte.DC_AMOUNT
                 WHEN gte.CURRENCY_SHORT_DES = 'TZS' THEN 0
                 WHEN gte.CURRENCY_SHORT_DES <> 'USD'
                     THEN DECIMAL(gte.DC_AMOUNT / fx.RATE, 18, 2)
                 END                                                                    AS usdCrUsageLast30DaysAmount,

             CASE
                 WHEN gte.CURRENCY_SHORT_DES = 'USD'
                     THEN DECIMAL(gte.DC_AMOUNT * fx.RATE, 18, 2)
                 ELSE gte.DC_AMOUNT
                 END                                                                    AS tzsCrUsageLast30DaysAmount,

             VARCHAR_FORMAT(la.DRAWDOWN_FST_DT, 'DDMMYYYYHHMM')                         AS disbursementDate,
             VARCHAR_FORMAT(la.ACC_EXP_DT, 'DDMMYYYYHHMM')                              AS expiryDate,
             VARCHAR_FORMAT(COALESCE(la.OV_EXP_DT, la.ACC_EXP_DT), 'DDMMYYYYHHMM')      AS realEndDate,

             (la.NRM_CAP_BAL + la.OV_CAP_BAL)
                 + (la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL) AS orgOutstandingAmount,

             CASE
                 WHEN gte.CURRENCY_SHORT_DES = 'USD'
                     THEN (la.NRM_CAP_BAL + la.OV_CAP_BAL)
                     + (la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL)
                 WHEN gte.CURRENCY_SHORT_DES = 'TZS' THEN 0
                 WHEN gte.CURRENCY_SHORT_DES <> 'USD' THEN
                     DECIMAL(((la.NRM_CAP_BAL + la.OV_CAP_BAL)
                         + (la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL)) / fx.RATE, 18, 2)
                 ELSE 0
                 END                                                                    AS usdOutstandingAmount,

             CASE
                 WHEN gte.CURRENCY_SHORT_DES = 'USD'
                     THEN DECIMAL(
                         (la.NRM_CAP_BAL + la.OV_CAP_BAL)
                             +
                         (la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL)
                             * fx.RATE, 18, 2)
                 ELSE (la.NRM_CAP_BAL + la.OV_CAP_BAL)
                     + (la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL)
                 END                                                                    AS tzsOutstandingAmount,

             (la.NRM_CAP_BAL + la.OV_CAP_BAL)                                           AS orgOutstandingPrincipalAmount,

             CASE
                 WHEN gte.CURRENCY_SHORT_DES = 'USD'
                     THEN (la.NRM_CAP_BAL + la.OV_CAP_BAL)
                 WHEN gte.CURRENCY_SHORT_DES = 'TZS' THEN 0
                 WHEN gte.CURRENCY_SHORT_DES <> 'USD'
                     THEN DECIMAL((la.NRM_CAP_BAL + la.OV_CAP_BAL) / fx.RATE, 18, 2)
                 ELSE 0
                 END                                                                    AS usdOutstandingPrincipalAmount,

             CASE
                 WHEN gte.CURRENCY_SHORT_DES = 'USD'
                     THEN DECIMAL((la.NRM_CAP_BAL + la.OV_CAP_BAL) * fx.RATE, 18, 2)
                 ELSE (la.NRM_CAP_BAL + la.OV_CAP_BAL)
                 END                                                                    AS tzsOutstandingPrincipalAmount,

             VARCHAR_FORMAT(la.DRAWDOWN_FST_DT, 'DDMMYYYYHHMM')                         AS latestCustomerCreditDate,
             la.DRAWDOWN_FST_AMN                                                        AS latestCreditAmount,
             DECIMAL(la.INTER_RATE_SPRD, 18, 2)                                         AS primeLendingRate,
             DECIMAL(la.MORATOR_NRM_RATE, 18, 2)                                        AS annualInterestRate,

             CollateralAgg.collateralPledged                                            AS collateralPledged,

             0                                                                          AS restructuredLoans,
             CASE
                 WHEN la.OV_CAP_BAL > 0 AND la.OV_EXP_DT IS NOT NULL
                     THEN DAYS(CURRENT_DATE) - DAYS(la.OV_EXP_DT)
                 ELSE 0
                 END                                                                    AS pastDueDays,
             la.OV_CAP_BAL                                                              AS pastDueAmount,
             la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL         AS orgAccruedInterestAmount,

             CASE
                 WHEN gte.CURRENCY_SHORT_DES = 'USD'
                     THEN la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL
                 WHEN gte.CURRENCY_SHORT_DES = 'TZS' THEN 0
                 WHEN gte.CURRENCY_SHORT_DES <> 'USD'
                     THEN DECIMAL(
                         (la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL) / fx.RATE, 18, 2)
                 ELSE 0
                 END                                                                    AS usdAccruedInterestAmount,

             CASE
                 WHEN gte.CURRENCY_SHORT_DES = 'USD'
                     THEN DECIMAL(
                         (la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL) * fx.RATE, 18, 2)
                 ELSE la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL
                 END                                                                    AS tzsAccruedInterestAmount,

             la.OV_RL_PNL_INT_BAL + la.OV_URL_PNL_INT_BAL                               AS orgPenaltyChargedAmount,

             CASE
                 WHEN gte.CURRENCY_SHORT_DES = 'USD'
                     THEN la.OV_RL_PNL_INT_BAL + la.OV_URL_PNL_INT_BAL
                 WHEN gte.CURRENCY_SHORT_DES = 'TZS' THEN 0
                 WHEN gte.CURRENCY_SHORT_DES <> 'USD'
                     THEN DECIMAL((la.OV_RL_PNL_INT_BAL + la.OV_URL_PNL_INT_BAL) / fx.RATE, 18, 2)
                 ELSE 0
                 END                                                                    AS usdPenaltyChargedAmount,

             CASE
                 WHEN gte.CURRENCY_SHORT_DES = 'USD'
                     THEN DECIMAL((la.OV_RL_PNL_INT_BAL + la.OV_URL_PNL_INT_BAL) * fx.RATE, 18, 2)
                 ELSE la.OV_RL_PNL_INT_BAL + la.OV_URL_PNL_INT_BAL
                 END                                                                    AS tzsPenaltyChargedAmount,

             COALESCE(la.OV_RL_PNL_INT_BAL, 0)                                          AS orgPenaltyPaidAmount,

             CASE
                 WHEN gte.CURRENCY_SHORT_DES = 'USD'
                     THEN COALESCE(la.OV_RL_PNL_INT_BAL, 0)
                 WHEN gte.CURRENCY_SHORT_DES = 'TZS' THEN 0
                 WHEN gte.CURRENCY_SHORT_DES <> 'USD'
                     THEN DECIMAL(COALESCE(la.OV_RL_PNL_INT_BAL, 0) / fx.RATE, 18, 2)
                 ELSE 0
                 END                                                                    AS usdPenaltyPaidAmount,

             CASE
                 WHEN gte.CURRENCY_SHORT_DES = 'USD'
                     THEN DECIMAL(COALESCE(la.OV_RL_PNL_INT_BAL, 0) * fx.RATE, 18, 2)
                 ELSE COALESCE(la.OV_RL_PNL_INT_BAL, 0)
                 END                                                                    AS tzsPenaltyPaidAmount,

             la.TOT_COMMISSION_AMN                                                      AS orgLoanFeesChargedAmount,

             CASE
                 WHEN gte.CURRENCY_SHORT_DES = 'USD'
                     THEN la.TOT_COMMISSION_AMN
                 WHEN gte.CURRENCY_SHORT_DES = 'TZS' THEN 0
                 WHEN gte.CURRENCY_SHORT_DES <> 'USD'
                     THEN DECIMAL(la.TOT_COMMISSION_AMN / fx.RATE, 18, 2)
                 ELSE 0
                 END                                                                    AS usdLoanFeesChargedAmount,

             CASE
                 WHEN gte.CURRENCY_SHORT_DES = 'USD'
                     THEN DECIMAL(la.TOT_COMMISSION_AMN * fx.RATE, 18, 2)
                 ELSE la.TOT_COMMISSION_AMN
                 END                                                                    AS tzsLoanFeesChargedAmount,

             la.TOT_EXPENSE_AMN                                                         AS orgLoanFeesPaidAmount,

             CASE
                 WHEN gte.CURRENCY_SHORT_DES = 'USD'
                     THEN la.TOT_EXPENSE_AMN
                 WHEN gte.CURRENCY_SHORT_DES = 'TZS' THEN 0
                 WHEN gte.CURRENCY_SHORT_DES <> 'USD'
                     THEN DECIMAL(la.TOT_EXPENSE_AMN / fx.RATE, 18, 2)
                 ELSE 0
                 END                                                                    AS usdLoanFeesPaidAmount,

             CASE
                 WHEN gte.CURRENCY_SHORT_DES = 'USD'
                     THEN DECIMAL(la.TOT_EXPENSE_AMN * fx.RATE)
                 ELSE la.TOT_EXPENSE_AMN
                 END                                                                    AS tzsLoanFeesPaidAmount,

             0.00                                                                       AS orgTotMonthlyPaymentAmount,
             0.00                                                                       AS usdTotMonthlyPaymentAmount,
             0.00                                                                       AS tzsTotMonthlyPaymentAmount,

             la.TOT_NRM_INT_AMN + la.TOT_PNL_INT_AMN                                    AS orgInterestPaidTotal,

             CASE
                 WHEN gte.CURRENCY_SHORT_DES = 'USD'
                     THEN DECIMAL(la.TOT_NRM_INT_AMN + la.TOT_PNL_INT_AMN, 18, 2)
                 WHEN gte.CURRENCY_SHORT_DES = 'TZS' THEN 0
                 WHEN gte.CURRENCY_SHORT_DES <> 'USD'
                     THEN DECIMAL((la.TOT_NRM_INT_AMN + la.TOT_PNL_INT_AMN) / fx.RATE, 18, 2)
                 ELSE 0
                 END                                                                    AS usdInterestPaidTotal,

             CASE
                 WHEN gte.CURRENCY_SHORT_DES = 'USD'
                     THEN DECIMAL((la.TOT_NRM_INT_AMN + la.TOT_PNL_INT_AMN) * fx.RATE, 18, 2)
                 ELSE DECIMAL(la.TOT_NRM_INT_AMN + la.TOT_PNL_INT_AMN, 18, 2)
                 END                                                                    AS tzsInterestPaidTotal,

             'Current'                                                                  AS assetClassificationCategory,
             'Other financial Corporations'                                             AS sectorSnaClassification,
             'NoNegativeStatus'                                                         AS negStatusContract,
             'N/A'                                                                      AS customerRole,
             0                                                                          AS allowanceProbableLoss,
             0                                                                          AS botProvision

      FROM GLI_TRX_EXTRACT gte

               LEFT JOIN CURRENCY curr
                         ON curr.SHORT_DESCR = gte.CURRENCY_SHORT_DES

               LEFT JOIN
           (SELECT fr.fk_currencyid_curr,
                   fr.rate
            FROM fixing_rate fr
            WHERE (fr.fk_currencyid_curr, fr.activation_date, fr.activation_time) IN
                  (SELECT fk_currencyid_curr,
                          activation_date,
                          MAX(activation_time)
                   FROM fixing_rate
                   WHERE activation_date =
                         (SELECT MAX(b.activation_date)
                          FROM fixing_rate b
                          WHERE b.activation_date <= CURRENT_DATE)
                   GROUP BY fk_currencyid_curr, activation_date)) fx
           ON fx.fk_currencyid_curr = curr.ID_CURRENCY

               LEFT JOIN CUSTOMER c
                         ON gte.CUST_ID = c.CUST_ID

               LEFT JOIN CUSTOMER_TYPES_LOOKUP ctl
                         ON ctl.CUSTOMER_TYPE_CODE = c.CUST_TYPE

               JOIN PROFITS_ACCOUNT pa
                    ON pa.CUST_ID = gte.CUST_ID
                        AND pa.PRFT_SYSTEM = 4
                        AND pa.ACCOUNT_NUMBER IS NOT NULL
                        AND pa.PRODUCT_ID IN (40030, 40034, 40035, 40036, 40037, 40040)

               LEFT JOIN PRODUCT p
                         ON p.ID_PRODUCT = gte.ID_PRODUCT

               LEFT JOIN other_id id
                         ON (CASE WHEN (id.serial_no IS NULL) THEN '1' ELSE id.main_flag END = '1'
                             AND id.fk_customercust_id = c.cust_id)

               LEFT JOIN cust_address ca
                         ON ca.fk_customercust_id = c.cust_id
                             AND ca.communication_addr = '1'
                             AND ca.entry_status = '1'

               LEFT JOIN generic_detail id_country
                         ON id.fkgh_has_been_issu = id_country.fk_generic_headpar
                             AND id.fkgd_has_been_issu = id_country.serial_num

               LEFT JOIN COUNTRIES_LOOKUP cl
                         ON cl.COUNTRY_NAME = id_country.description

          --                          LEFT JOIN LOAN_ACCOUNT wela
--                          ON gte.ID_PRODUCT = wela.FK_LOANFK_PRODUCTI
--                              AND wela.CUST_ID = gte.CUST_ID


               INNER JOIN (SELECT la.*,
                                  ROW_NUMBER() OVER (
                                      PARTITION BY la.CUST_ID, la.FK_LOANFK_PRODUCTI
                                      ORDER BY la.TMSTAMP DESC, la.ACC_OPEN_DT DESC
                                      ) AS rn
                           FROM LOAN_ACCOUNT la) la
                          ON gte.ID_PRODUCT = la.FK_LOANFK_PRODUCTI
                              AND la.CUST_ID = gte.CUST_ID
                              AND la.rn = 1 -- Only the most recent loan

               LEFT JOIN (SELECT li.*,
                                 ROW_NUMBER() OVER (PARTITION BY li.ACC_SN ORDER BY li.INSTALL_SN DESC) AS rn
                          FROM LOAN_INSTALLMENTS li) li
                         ON li.ACC_SN = la.ACC_SN AND li.rn = 1

               LEFT JOIN CollateralAgg ON CollateralAgg.COLLTBL_CUST_ID = la.CUST_ID
               JOIN AGREEMENT ag
                    ON ag.FK_UNITCODE = la.FK_AGREEMENTFK_UNI
                        AND ag.AGR_YEAR = la.FK_AGREEMENTAGR_YE
                        AND ag.AGR_SN = la.FK_AGREEMENTAGR_SN
                        AND ag.AGR_MEMBERSHIP_SN = la.FK_AGREEMENTAGR_ME

               LEFT JOIN BANKEMPLOYEE be
                         ON be.STAFF_NO = la.USR

      WHERE gte.FK_GLG_ACCOUNTACCO IN
            ('1.1.0.05.0001', '1.1.0.05.0002', '1.1.0.05.0005')) t
WHERE LENGTH(TRIM(TRANSLATE(t.loanOfficer, '', '0123456789'))) = LENGTH(TRIM(t.loanOfficer))
  AND LENGTH(TRIM(TRANSLATE(t.loanSupervisor, '', '0123456789'))) = LENGTH(TRIM(t.loanSupervisor));