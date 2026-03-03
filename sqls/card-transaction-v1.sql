WITH ce_joined AS (
    SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') AS reportingDate,
           ca.FULL_CARD_NO                                   AS cardNumber,
           RIGHT(TRIM(ca.FULL_CARD_NO), 10)                  AS binNumber,
           'Mwalimu Commercial Bank Plc'                     AS transactingBankName,
           ce.ISO_REF_NUM,
           ce.CARD_SN,
           ce.TUN_DATE,
           VARCHAR_FORMAT(ce.TUN_DATE, 'DDMMYYYYHHMM')       AS transactionDate,
           'Local Transactions by Locally Issued Cards'      AS transactionNature,
           CAST(null AS VARCHAR(1))                          AS atmCode,
           CAST(null AS VARCHAR(1))                          AS posNumber,
           pc.DESCRIPTION                                    AS transactionDescription,
           ca.CARD_NAME_LATIN                                AS beneficiaryName,
           CAST(null AS VARCHAR(1))                          AS beneficiaryTradeName,
           'TANZANIA, UNITED REPUBLIC OF'                    AS beneficiaryCountry,
           'TANZANIA, UNITED REPUBLIC OF'                    AS transactionPlace,
           CAST(null AS VARCHAR(1))                          AS qtyItemsPurchased,
           CAST(null AS VARCHAR(1))                          AS unitPrice,
           CAST(null AS VARCHAR(1))                          AS orgFacilitatorCommissionAmount,
           CAST(null AS VARCHAR(1))                          AS usdFacilitatorCommissionAmount,
           CAST(null AS VARCHAR(1))                          AS tzsFacilitatorCommissionAmount,
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
    FROM CMS_CARD_EXTRAIT ce
             LEFT JOIN CMS_CARD ca
                       ON ca.CARD_SN = ce.CARD_SN
             LEFT JOIN (SELECT ISO_CODE, MIN(DESCRIPTION) AS DESCRIPTION
                        FROM ATM_PROCESS_CODE
                        GROUP BY ISO_CODE) pc
                       ON pc.ISO_CODE = ce.PROCESS_CD
             JOIN (SELECT FK_CARD_SN, MIN(PRFT_SYSTEM) AS PRFT_SYSTEM
                   FROM CMS_CARD_ACCOUNT
                   GROUP BY FK_CARD_SN) card_acc
                  ON ce.CARD_SN = card_acc.FK_CARD_SN
             JOIN (SELECT PRFT_SYSTEM, MIN(MOVEMENT_CURRENCY) AS MOVEMENT_CURRENCY
                   FROM PROFITS_ACCOUNT
                   GROUP BY PRFT_SYSTEM) pa
                  ON pa.PRFT_SYSTEM = card_acc.PRFT_SYSTEM
             JOIN (SELECT ID_CURRENCY, MIN(SHORT_DESCR) AS SHORT_DESCR
                   FROM CURRENCY
                   GROUP BY ID_CURRENCY) curr
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
                       ON fx.fk_currencyid_curr = curr.ID_CURRENCY
),
ce_numbered AS (
    SELECT ce_joined.*,
           ROW_NUMBER() OVER (ORDER BY TUN_DATE, CARD_SN, ISO_REF_NUM) AS rn
    FROM ce_joined
)
SELECT reportingDate,
       cardNumber,
       binNumber,
       transactingBankName,
       TRIM(CAST(ISO_REF_NUM AS VARCHAR(20))) || '-' ||
       TRIM(CAST(CARD_SN AS VARCHAR(20))) || '-' ||
       VARCHAR_FORMAT(TUN_DATE, 'DDMMYYYYHHMM') || '-' ||
       TRIM(CAST(rn AS VARCHAR(10)))                     AS transactionId,
       transactionDate,
       transactionNature,
       atmCode,
       posNumber,
       transactionDescription,
       beneficiaryName,
       beneficiaryTradeName,
       beneficiaryCountry,
       transactionPlace,
       qtyItemsPurchased,
       unitPrice,
       orgFacilitatorCommissionAmount,
       usdFacilitatorCommissionAmount,
       tzsFacilitatorCommissionAmount,
       currency,
       orgTransactionAmount,
       usdTransactionAmount,
       tzsTransactionAmount
FROM ce_numbered;