SELECT VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') AS reportingDate,
       VARCHAR_FORMAT(gte.TRN_DATE, 'DDMMYYYYHHMM')      AS transactionDate,
       pa.ACCOUNT_NUMBER                                 AS accountNumber,
       gte.CUST_ID                                       AS customerIdentificationNumber,

       -- Deposit, Withdraw or Payment
       CASE
           WHEN gte.FK_GLG_ACCOUNTACCO IN ('2.3.0.00.0087', '2.3.0.00.0123', '1.4.4.00.0074') THEN 'Withdraw'
           WHEN gte.FK_GLG_ACCOUNTACCO = '1.4.4.00.0063' THEN 'Deposit'
           WHEN gte.FK_GLG_ACCOUNTACCO IN ('5.0.4.04.0001', '5.0.4.04.0002', '1.4.4.00.0046', '2.3.0.00.0064')
               THEN 'Payment'
           END                                           AS mobileTransactionType,
       'Mobile Banking Transactions'                     AS serviceCategory,
       'Mobile money'                                              AS subServiceCategory,

       --domestic or international

       'domestic'                                        AS serviceStatus,

       VARCHAR(gte.FK_UNITCODETRXUNIT) || '-' ||
       TRIM(gte.FK_USRCODE) || '-' ||
       VARCHAR(gte.LINE_NUM) || '-' ||
       VARCHAR(gte.TRN_DATE) || '-' ||
       VARCHAR(gte.TRN_SNUM)                             AS transactionRef,

       'MWCOTZTZ'                                        AS benBankOrWalletCode,
       wdc.TELEPHONE                                     AS benAccountOrMobileNumber,

       gte.CURRENCY_SHORT_DES                            AS currency,

       -- Original amount (as transacted)
       DECIMAL(gte.DC_AMOUNT, 18, 2)                     AS orgAmount,

       -- Amount converted to TZS
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'TZS'
               THEN DECIMAL(gte.DC_AMOUNT, 18, 2)
           WHEN gte.CURRENCY_SHORT_DES = 'USD'
               THEN DECIMAL(gte.DC_AMOUNT * fx.RATE, 18, 2)
           ELSE NULL
           END                                           AS tzsAmount,

       DECIMAL(gte.DC_AMOUNT * 0.18, 18, 2)              AS valueAddedTaxAmount,
       0                                                 AS exciseDutyAmount,
       0                                                 AS electronicLevyAmount

FROM GLI_TRX_EXTRACT gte
         LEFT JOIN (SELECT *
                    FROM (SELECT pa.*,
                                 ROW_NUMBER() OVER (PARTITION BY CUST_ID ORDER BY ACCOUNT_NUMBER) rn
                          FROM PROFITS_ACCOUNT pa
                          WHERE PRFT_SYSTEM = 3)
                    WHERE rn = 1) pa ON pa.CUST_ID = gte.CUST_ID
         LEFT JOIN (SELECT *
                    FROM (SELECT wdc.*,
                                 ROW_NUMBER() OVER (PARTITION BY CUST_ID ORDER BY CUSTOMER_BEGIN_DAT DESC) rn
                          FROM W_DIM_CUSTOMER wdc)
                    WHERE rn = 1) wdc ON wdc.CUST_ID = gte.CUST_ID

    -- Join Currency Using SHORT_DESCR
-- =========================================
         LEFT JOIN CURRENCY curr
                   ON curr.SHORT_DESCR = gte.CURRENCY_SHORT_DES

    -- =============================================
-- Latest Fixing Rate Per Currency
-- =============================================
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
WHERE gte.FK_GLG_ACCOUNTACCO IN (
                                 '2.3.0.00.0087',
                                 '1.4.4.00.0063',
                                 '2.3.0.00.0064',
                                 '5.0.4.04.0001',
                                 '5.0.4.04.0002',
                                 '1.4.4.00.0046',
                                 '1.4.4.00.0074',
                                 '2.3.0.00.0123'
    );
