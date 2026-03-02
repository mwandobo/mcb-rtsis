select CURRENT_TIMESTAMP                                                       AS reportingDate,
       LTRIM(RTRIM(c.CUST_ID))                                                 AS borrowersInstitutionCode,
       CASE
           WHEN cl.COUNTRY_CODE = 'TZ' THEN 'TANZANIA, UNITED REPUBLIC OF' END as borrowerCountry,
       'Domestic banks unrelated'                                              as relationshipType,
       CAST(0 AS SMALLINT)                                                     as ratingStatus,
       CASE
           WHEN la.ACC_EXP_DT IS NULL THEN 'UNRATED'
           WHEN DAYS(la.ACC_EXP_DT) - DAYS(CURRENT_DATE) <= 90
               THEN 'SHORT_TERM_UNRATED'
           ELSE 'UNRATED'
           END                                                                 AS externalRatingCorrespondentBorrower,
       'Grade B'                                                               AS gradesUnratedBorrower,
       la.ACC_SN                                                               AS loanNumber,
       'Interbank call loans in Tanzania'                                      AS loanType,
       la.ACC_OPEN_DT                                                          as issueDate,
       la.ACC_EXP_DT                                                           AS loanMaturityDate,
       gte.CURRENCY_SHORT_DES                                                  as currency,
       la.ACC_LIMIT_AMN                                                        AS orgLoanAmount,
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD'
               THEN DECIMAL(gte.DC_AMOUNT, 18, 2)
           WHEN gte.CURRENCY_SHORT_DES <> 'USD'
               THEN DECIMAL(la.ACC_LIMIT_AMN / fx.rate, 18, 2)
           END                                                                 AS usdLoanAmount,
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD'
               THEN DECIMAL(la.ACC_LIMIT_AMN * fx.rate, 18, 2)
           ELSE DECIMAL(la.ACC_LIMIT_AMN, 18, 2)
           END                                                                 AS tzsLoanAmount,
       la.INTER_RATE_SPRD                                                      AS interestRate,
       la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL      AS orgAccruedInterestAmount,
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD'
               THEN DECIMAL(gte.DC_AMOUNT, 18, 2)
           WHEN gte.CURRENCY_SHORT_DES <> 'USD'
               THEN DECIMAL(la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL / fx.rate, 18, 2)
           END                                                                 AS usdAccruedInterestAmount,
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD'
               THEN DECIMAL(la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL * fx.rate, 18, 2)
           ELSE DECIMAL(la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL, 18, 2)
           END                                                                 AS tzsAccruedInterestAmount,

       (la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL)                         AS orgSuspendedInterest,
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD'
               THEN DECIMAL(gte.DC_AMOUNT, 18, 2)
           WHEN gte.CURRENCY_SHORT_DES <> 'USD'
               THEN DECIMAL((la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL) / fx.rate, 18, 2)
           END                                                                 AS usdSuspendedInterest,
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD'
               THEN DECIMAL((la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL) * fx.rate, 18, 2)
           ELSE DECIMAL((la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL), 18, 2)
           END                                                                 AS tzsSuspendedInterest,
       OV_EXP_DT,

       CASE
           WHEN la.ACC_STATUS = '1'        -- active loan only
               AND la.OV_EXP_DT IS NOT NULL
               AND CURRENT_DATE > la.OV_EXP_DT
               THEN DAYS(CURRENT_DATE) - DAYS(la.OV_EXP_DT)
           ELSE 0
           END AS pastDueDays,
       CASE
           WHEN la.OV_EXP_DT IS NOT NULL AND CURRENT_DATE > la.OV_EXP_DT
               THEN DAYS(CURRENT_DATE) - DAYS(la.OV_EXP_DT)
           ELSE 0
           END                                                                 AS pastDueDays,
       0                                                                       AS allowanceProbableLoss,
       0                                                                       AS botProvision,
       'Current'                                                               AS assetClassificationCategory

from GLI_TRX_EXTRACT as gte
         INNER JOIN (SELECT la.*,
                            ROW_NUMBER() OVER (
                                PARTITION BY la.CUST_ID, la.FK_LOANFK_PRODUCTI
                                ORDER BY la.TMSTAMP DESC, la.ACC_OPEN_DT DESC
                                ) AS rn
                     FROM LOAN_ACCOUNT la) la
                    ON gte.ID_PRODUCT = la.FK_LOANFK_PRODUCTI
                        AND la.CUST_ID = gte.CUST_ID
                        AND la.rn = 1 -- Only the most recent loan
         LEFT JOIN CUSTOMER as c ON la.CUST_ID = c.CUST_ID

         LEFT JOIN other_id id ON (CASE WHEN (id.serial_no IS NULL) THEN '1' ELSE id.main_flag END = '1' AND
                                   id.fk_customercust_id = c.cust_id)
--
         LEFT JOIN generic_detail id_country ON (id.fkgh_has_been_issu = id_country.fk_generic_headpar AND
                                                 id.fkgd_has_been_issu = id_country.serial_num)
         LEFT JOIN COUNTRIES_LOOKUP cl ON cl.COUNTRY_NAME = id_country.description

         LEFT JOIN CURRENCY curr
                   ON curr.SHORT_DESCR = gte.CURRENCY_SHORT_DES

         LEFT JOIN (SELECT fr.fk_currencyid_curr,
                           fr.rate
                    FROM fixing_rate fr
                    WHERE (fr.fk_currencyid_curr, fr.activation_date, fr.activation_time) IN
                          (SELECT fk_currencyid_curr,
                                  activation_date,
                                  MAX(activation_time)
                           FROM fixing_rate
                           WHERE activation_date = (SELECT MAX(b.activation_date)
                                                    FROM fixing_rate b
                                                    WHERE b.activation_date <= CURRENT_DATE)
                           GROUP BY fk_currencyid_curr, activation_date)) fx
                   ON fx.fk_currencyid_curr = curr.ID_CURRENCY
WHERE gte.FK_GLG_ACCOUNTACCO IN ('7.0.5.19.0001', '7.0.5.19.0002', '7.0.5.19.0003')
;