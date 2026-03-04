SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') AS reportingDate,
       VARCHAR_FORMAT(gte.TRN_DATE, 'DDMMYYYYHHMM')      AS transactionDate,

       CASE
           WHEN t.CR_BANK_BIC = 'TANZTZTXXXX' THEN 'Bank Of Tanzania'
           ELSE 'Mwalimu Commercial Bank Plc'
           END                                           AS lenderName,
       CASE
           WHEN t.DB_BANK_BIC = 'TANZTZTXXXX' THEN 'Bank Of Tanzania'
           ELSE 'Mwalimu Commercial Bank Plc'
           END                                           AS borrowerName,
       CASE

           WHEN COALESCE(prod.DESCRIPTION, CAST(t.FK_TRX_PRODUCT AS VARCHAR(20))) = 'MONEY MARKET BACK OFFICE'
               THEN 'Off Market'
           ELSE 'Market'
           END                                           AS transactionType,

       gte.DC_AMOUNT                                     AS tzsAmount,

       COALESCE(
               DAYS(t.MATURITY_DATE) - DAYS(t.VALUE_DATE),
               t.DEAL_DURATION_LONG,
               t.DURATION0
       )                                                 AS tenure,

       t.INTEREST_RATE                                   AS interestRate

FROM GLI_TRX_EXTRACT gte
         JOIN TREASURY_MM_DEAL t ON t.DEAL_NO = gte.TRX_SN
         LEFT JOIN SSI_PARTY db_bank ON db_bank.BANK_ID = t.DB_BANK_ID
         LEFT JOIN SSI_PARTY cr_bank ON cr_bank.BANK_ID = t.CR_BANK_ID
         LEFT JOIN PRODUCT prod ON prod.ID_PRODUCT = t.FK_TRX_PRODUCT

WHERE gte.FK_GLG_ACCOUNTACCO = '1.0.2.00.0001'
  AND NOT (t.CR_BANK_BIC IS NULL AND t.DB_BANK_BIC IS NULL);