select W.AGREEMENT_NUMBER                                           as customerIdentificationNumber,
       W.ACCOUNT_NUMBER                                             as accountNumber,
       LTRIM(RTRIM(S.SURNAME)) || ' ' || LTRIM(RTRIM(S.FIRST_NAME)) as clientName,
       (CASE W.LOAN_STATUS
            WHEN '1' THEN '1 - Normal'
            WHEN '2' THEN '2 - Overdue'
            WHEN '3' THEN '3 - Definate Delay'
            WHEN '4' THEN '4 - Write Off'
            ELSE 'n/a'
           END)                                                     AS LOAN_STATUS,
       W.CURRENCY                                                   as currency,
       W.INSTALL_COUNT                                              as loanInstallment,
       W.FK_UNITCODE                                                as branchCode,
       W.INSTA_PAID                                                 as loanInstallmentPaid
from W_EOM_LOAN_ACCOUNT as W
         LEFT JOIN CUSTOMER as S ON W.CUST_ID = S.CUST_ID
         LEFT JOIN LOAN_ACCOUNT L
                   ON W.FK_UNITCODE = L.FK_UNITCODE
                       AND W.ACC_TYPE = L.ACC_TYPE
                       AND W.ACC_SN = L.ACC_SN
         LEFT JOIN PROFITS_ACCOUNT P
                   ON L.DEP_ACC_SN = P.DEP_ACC_NUMBER
                       AND P.PRFT_SYSTEM = 3
                       AND P.SECONDARY_ACC <> '1'
         LEFT JOIN LOAN_ADD_INFO N
                   ON N.ROW_ID = 1
                       AND W.FK_UNITCODE = N.ACC_UNIT
                       AND W.ACC_TYPE = N.ACC_TYPE
                       AND W.ACC_SN = N.ACC_SN
         LEFT JOIN GENERIC_DETAIL GG
                   ON GG.FK_GENERIC_HEADPAR = W.FKGH_HAS_AS_LOAN_P
                       AND GG.SERIAL_NUM = W.FKGD_HAS_AS_LOAN_P;