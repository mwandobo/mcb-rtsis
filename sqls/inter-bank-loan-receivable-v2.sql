select CURRENT_TIMESTAMP                                                       AS reportingDate,
       LTRIM(RTRIM(id.ID_NO))                                                  AS borrowersInstitutionCode,
       CASE
           WHEN cl.COUNTRY_CODE = 'TZ' THEN 'TANZANIA, UNITED REPUBLIC OF' END as borrowerCountry,
       'Domestic banks unrelated'                                              as relationshipType,
       null                                                                    as ratingStatus,
       null                                                                    as externalRatingCorrespondentBorrower,
       null                                                                    as gradesUnratedBorrower,
       gte.ACCOUNT_NUMBER                                                      as loanNumber,
       'Interbank call loans in Tanzania'                                      AS loanType,
       la.ACC_OPEN_DT                                                          as issueDate,
       la.ACC_EXP_DT                                                           AS loanMaturityDate,
       gte.CURRENCY_SHORT_DES                                                  as currency,
       la.ACC_LIMIT_AMN                                                        as orgLoanAmount,
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN la.ACC_LIMIT_AMN
           ELSE CAST(ROUND(la.ACC_LIMIT_AMN / 2500, 2) AS DECIMAL(15, 2))
           END                                                                 AS usdLoanAmount,
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD'
               THEN la.ACC_LIMIT_AMN * 2500 -- <<< replace with your rate
           ELSE
               la.ACC_LIMIT_AMN
           END                                                                 AS tzsLoanAmount,
       la.INTER_RATE_SPRD                                                      AS interestRate,
       la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL      AS orgAccruedInterestAmount,
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL
           ELSE NULL
           END                                                                 AS usdAccruedInterestAmount,
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN
               (la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL) *
               2500
           ELSE la.NRM_ACR_INT_BAL + la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL
           END                                                                 AS tzsAccruedInterestAmount,
       (la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL)                         AS orgSuspendedInterest,
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN (la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL)
           ELSE NULL
           END                                                                 AS usdSuspendedInterest,
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN (la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL) * 2500
           ELSE (la.OV_ACR_NRM_INT_BAL + la.OV_ACR_PNL_INT_BAL)
           END                                                                 AS tzsSuspendedInterest,
       CASE
           WHEN CURRENT_DATE > la.OV_EXP_DT THEN DAYS(CURRENT_DATE) - DAYS(la.OV_EXP_DT)
           ELSE 0
           END                                                                 AS pastDueDays,
       0                                                                       AS allowanceProbableLoss,
       0                                                                       AS botProvision,
       'Current'                                                               AS assetClassificationCategory

from GLI_TRX_EXTRACT as gte
         LEFT JOIN LOAN_ACCOUNT la
                       ON gte.ID_PRODUCT = la.FK_LOANFK_PRODUCTI
                       and la.CUST_ID = gte.CUST_ID
                       AND TRIM(CHAR(la.UNIT)) = TRIM(CHAR(gte.FK_UNITCODETRXUNIT))
         LEFT JOIN CUSTOMER as c ON la.CUST_ID = c.CUST_ID

         LEFT JOIN other_id id ON (CASE WHEN (id.serial_no IS NULL) THEN '1' ELSE id.main_flag END = '1' AND
                                   id.fk_customercust_id = c.cust_id)
--
         LEFT JOIN generic_detail id_country ON (id.fkgh_has_been_issu = id_country.fk_generic_headpar AND
                                                 id.fkgd_has_been_issu = id_country.serial_num)
         LEFT JOIN COUNTRIES_LOOKUP cl ON cl.COUNTRY_NAME = id_country.description
WHERE gte.FK_GLG_ACCOUNTACCO IN ('7.0.5.19.0001', '7.0.5.19.0002', '7.0.5.19.0003')
;