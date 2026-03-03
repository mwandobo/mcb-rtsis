WITH ce_numbered AS (SELECT ce.*,
                            ROW_NUMBER() OVER (ORDER BY ce.TUN_DATE, ce.CARD_SN, ce.ISO_REF_NUM) AS rn
                     FROM CMS_CARD_EXTRAIT ce)
SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') AS reportingDate,
       ca.FULL_CARD_NO                                   AS cardNumber,
       RIGHT(TRIM(ca.FULL_CARD_NO), 10)                  AS binNumber,
       'Mwalimu Commercial Bank Plc'                     AS transactingBankName,
       TRIM(ce.ISO_REF_NUM) || '-' ||
       TRIM(CAST(ce.CARD_SN AS VARCHAR(20))) || '-' ||
       VARCHAR_FORMAT(ce.TUN_DATE, 'DDMMYYYYHHMM') || '-' ||
       TRIM(CAST(ce.TRANSACTION_AMNT AS VARCHAR(20))) || '-' ||
       TRIM(CAST(ce.rn AS VARCHAR(10)))                  AS transactionId,
       VARCHAR_FORMAT(ce.TUN_DATE, 'DDMMYYYYHHMM')       AS transactionDate,
       'Local Transactions by Locally Issued Cards'      AS transactionNature,
       null                                              AS atmCode,
       null                                              AS posNumber,
       pc.DESCRIPTION                                    AS transactionDescription,
       ca.CARD_NAME_LATIN                                AS beneficiaryName,
       null                                              AS beneficiaryTradeName,
       'TANZANIA, UNITED REPUBLIC OF'                    AS beneficaryCountry,
       'TANZANIA, UNITED REPUBLIC OF'                    AS transactionPlace,
       null                                              AS qtyItemsPurchased,
       null                                              AS unitPrice,
       null                                              AS orgFacilitatorCommissionAmount,
       null                                              AS usdFacilitatorCommissionAmount,
       null                                              AS tzsFacilitatorCommissionAmount,
       curr.SHORT_DESCR                                  AS currency,
       ce.TRANSACTION_AMNT                               AS orgTransactionAmount,
       CASE
           WHEN curr.SHORT_DESCR = 'USD' THEN ce.TRANSACTION_AMNT
           WHEN curr.SHORT_DESCR <> 'USD' THEN DECIMAL(ce.TRANSACTION_AMNT / fx.RATE, 18, 2)
           ELSE NULL
           END                                           AS usdTransactionAmount,

       CASE
           WHEN curr.SHORT_DESCR = 'USD'
               THEN DECIMAL(ce.TRANSACTION_AMNT * fx.rate, 18, 2)

           ELSE DECIMAL(ce.TRANSACTION_AMNT, 18, 2)
           END                                           AS tzsTransactionAmount

FROM ce_numbered ce
         LEFT JOIN CMS_CARD ca
                   ON ca.CARD_SN = ce.CARD_SN
         LEFT JOIN (SELECT ISO_CODE, MIN(DESCRIPTION) AS DESCRIPTION
                    FROM ATM_PROCESS_CODE
                    GROUP BY ISO_CODE) pc
                   ON pc.ISO_CODE = ce.PROCESS_CD
         JOIN CMS_CARD_ACCOUNT card_acc
              ON ce.CARD_SN = card_acc.FK_CARD_SN
         JOIN PROFITS_ACCOUNT pa
              ON pa.PRFT_SYSTEM = card_acc.PRFT_SYSTEM
         JOIN CURRENCY curr
              ON curr.ID_CURRENCY = pa.MOVEMENT_CURRENCY
         LEFT JOIN (SELECT fr.fk_currencyid_curr,
                           fr.rate
                    FROM fixing_rate fr
                    WHERE (fr.fk_currencyid_curr, fr.activation_date, fr.activation_time) IN (SELECT fk_currencyid_curr,
                                                                                                     activation_date,
                                                                                                     MAX(activation_time)
                                                                                              FROM fixing_rate
                                                                                              WHERE activation_date =
                                                                                                    (SELECT MAX(b.activation_date)
                                                                                                     FROM fixing_rate b
                                                                                                     WHERE b.activation_date <= CURRENT_DATE)
                                                                                              GROUP BY fk_currencyid_curr, activation_date)) fx
                   ON fx.fk_currencyid_curr = curr.ID_CURRENCY;