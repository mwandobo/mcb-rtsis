select CURRENT_TIMESTAMP                                                        AS reportingDate,
       LTRIM(RTRIM(id.ID_NO))                                                   AS borrowersInstitutionCode,
       CASE
           WHEN cl.COUNTRY_CODE = 'TZ' THEN 'TANZANIA, UNITED REPUBLIC OF' END  as borrowerCountry,
       'Domestic banks unrelated'                                               as relationshipType,
       null                                                                     as ratingStatus,
       null                                                                     as externalRatingCorrespondentBorrower,
       null                                                                     as gradesUnratedBorrower,
       wela.ACCOUNT_NUMBER                                                      as loanNumber,
       'Interbank call loans in Tanzania'                                       AS loanType,
       wela.ACC_OPEN_DT                                                         as issueDate,
       wela.ACC_EXP_DT                                                          AS loanMaturityDate,
       wela.CURRENCY                                                            as currency,
       wela.ACC_LIMIT_AMN                                                       as orgLoanAmount,
       CASE
           WHEN wela.CURRENCY = 'USD'
               THEN wela.ACC_LIMIT_AMN
           ELSE NULL
           END                                                                  AS usdLoanAmount,
       CASE
           WHEN wela.CURRENCY = 'USD'
               THEN wela.ACC_LIMIT_AMN * 2500 -- <<< replace with your rate
           ELSE
               wela.ACC_LIMIT_AMN
           END                                                                  AS tzsLoanAmount,
       wela.FINAL_INTEREST                                                      AS interestRate,
       wela.NRM_ACR_INT_BAL + wela.OV_ACR_NRM_INT_BAL + wela.OV_ACR_PNL_INT_BAL AS orgAccruedInterestAmount,
       CASE
           WHEN wela.CURRENCY = 'USD' THEN wela.NRM_ACR_INT_BAL + wela.OV_ACR_NRM_INT_BAL + wela.OV_ACR_PNL_INT_BAL
           ELSE NULL
           END                                                                  AS usdAccruedInterestAmount,
       CASE
           WHEN wela.CURRENCY = 'USD' THEN (wela.NRM_ACR_INT_BAL + wela.OV_ACR_NRM_INT_BAL + wela.OV_ACR_PNL_INT_BAL) *
                                           2500
           ELSE wela.NRM_ACR_INT_BAL + wela.OV_ACR_NRM_INT_BAL + wela.OV_ACR_PNL_INT_BAL
           END                                                                  AS tzsAccruedInterestAmount,
       wela.INTEREST_IN_SUSPENSE                                                AS orgSuspendedInterest,
       CASE
           WHEN wela.CURRENCY = 'USD' THEN wela.INTEREST_IN_SUSPENSE
           ELSE NULL
           END                                                                  AS usdSuspendedInterest,
       CASE
           WHEN wela.CURRENCY = 'USD' THEN wela.INTEREST_IN_SUSPENSE * 2500
           ELSE wela.INTEREST_IN_SUSPENSE
           END                                                                  AS tzsSuspendedInterest,
       wela.OVERDUE_DAYS                                                        AS pastDueDays,
       wela.PROVISION_AMOUNT                                                    AS allowanceProbableLoss,
       wela.PROVISION_AMN                                                       AS botProvision,
       'Current'                                                                AS assetClassificationCategory

from W_EOM_LOAN_ACCOUNT as wela
         LEFT JOIN CUSTOMER as c ON wela.CUST_ID = c.CUST_ID
         LEFT JOIN PROFITS_ACCOUNT pa
                   ON pa.CUST_ID = wela.CUST_ID
         LEFT JOIN other_id id ON (CASE WHEN (id.serial_no IS NULL) THEN '1' ELSE id.main_flag END = '1' AND
                                   id.fk_customercust_id = c.cust_id)
         LEFT JOIN LNS_CRD_CUST_DATA lccd on lccd.CUST_ID = wela.CUST_ID

         LEFT JOIN generic_detail id_country ON (id.fkgh_has_been_issu = id_country.fk_generic_headpar AND
                                                 id.fkgd_has_been_issu = id_country.serial_num)
         LEFT JOIN CUSTOMER_TYPES_LOOKUP ctl ON ctl.CUSTOMER_TYPE_CODE = c.CUST_TYPE
         LEFT JOIN COUNTRIES_LOOKUP cl ON cl.COUNTRY_NAME = id_country.description
         LEFT JOIN GENERIC_DETAIL GG
                   ON GG.FK_GENERIC_HEADPAR = wela.FKGH_HAS_AS_LOAN_P
                       AND GG.SERIAL_NUM = wela.FKGD_HAS_AS_LOAN_P

         LEFT JOIN LOAN_ACCOUNT L
                   ON wela.FK_UNITCODE = L.FK_UNITCODE
                       AND wela.ACC_TYPE = L.ACC_TYPE
                       AND wela.ACC_SN = L.ACC_SN
         LEFT JOIN LOAN_ADD_INFO N
                   ON N.ROW_ID = 1
                       AND wela.FK_UNITCODE = N.ACC_UNIT
                       AND wela.ACC_TYPE = N.ACC_TYPE
                       AND wela.ACC_SN = N.ACC_SN;