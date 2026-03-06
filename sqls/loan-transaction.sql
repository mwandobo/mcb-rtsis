SELECT CURRENT_TIMESTAMP      AS reportingDate,
       gte.PRF_ACCOUNT_NUMBER AS loanNumber,
       gte.TRN_DATE           AS transactionDate,

       CASE
           WHEN gte.JUSTIFIC_DESCR IN ('PAYMENT FROM DEPOSIT ACCOUNT', 'LOAN ACCOUNT PAYMENT')
               THEN 'Installment payment'
           WHEN gte.JUSTIFIC_DESCR IN ('LOAN DRAWDOWN WITH COMMISSION', 'LOAN DRAWDOWN WITH NO COMMISSION')
               THEN 'Loan disbursement'
           WHEN gte.JUSTIFIC_DESCR IN
                ('PRE.FULL PAYMENT OF LOAN(FX)', 'LOANS WRITE OFF GLSYNC', 'LOAN ACCOUNT CLOSING(FX)')
               THEN 'Loan payoff'
           WHEN gte.JUSTIFIC_DESCR IN
                ('LOAN ACC CLASSIFICATION SYNC WITH GL', 'JOURNAL CREDIT',
                 'DR PRINCIPAL (CR REVERSAL) (JOURNAL) (1)', 'JOURNAL DEBIT', 'G/L CREDIT', 'G/L DEBIT')
               THEN 'Reversal'
           WHEN gte.JUSTIFIC_DESCR IN ('PRE.PAYMENT OF NEXT INSTALL(FX)', 'PRE.PARTIAL PAY OF LOAN(FX)')
               THEN 'Prepayments'
           WHEN gte.JUSTIFIC_DESCR = 'LOAN DRAWDOWN' THEN 'Loan disbursement'
           WHEN gte.JUSTIFIC_DESCR = 'AMORTIZATION INSTALLMENT CREATION' THEN 'Interest Accruals'
           WHEN gte.JUSTIFIC_DESCR = 'FX LOAN MULTI PAYMENTS' THEN 'Installment payment'
           WHEN gte.JUSTIFIC_DESCR IN ('SHARING CREDIT BALANCE', 'FUND TRANSFERS (INTRA BANK)', 'DEPOSIT CASH',
                                       'CR FROM MOBILE BANKING-MOB TO ACC')
               THEN 'Deposit'
           WHEN gte.JUSTIFIC_DESCR = 'MULTIPURPOSE JUSTIFICATION' THEN 'Loan administration fees'
           WHEN gte.JUSTIFIC_DESCR = 'ARREARS CAPITALIZATION' THEN 'Interest capitalization'
           WHEN gte.JUSTIFIC_DESCR = 'JD-TRANSFER TO ACCOUNT' THEN 'Withdrawal'
           ELSE 'Installment payment'
           END                AS loanTransactionType,

       CASE
           WHEN gte.JUSTIFIC_DESCR = 'LOAN DRAWDOWN WITH COMMISSION' THEN 'New disbursement'
           WHEN gte.JUSTIFIC_DESCR = 'PRE.FULL PAYMENT OF LOAN(FX)' THEN NULL
           WHEN gte.JUSTIFIC_DESCR = 'LOAN RESTRUCTURING FX' THEN 'Restructuring'
           WHEN gte.JUSTIFIC_DESCR = 'LOAN ENHANCEMENT FX' THEN 'Enhancement'
           ELSE NULL
           END                AS loanTransactionSubType,
       gte.CURRENCY_SHORT_DES AS currency,

       -- Original transaction amount in its native currency
       gte.DC_AMOUNT          AS orgTransactionAmount,

       -- USD amount: only populated when currency is USD
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN gte.DC_AMOUNT
           ELSE NULL
           END                AS usdTransactionAmount,

       -- TZS amount: convert from USD using fixed rate, otherwise use original
       CASE
           WHEN gte.CURRENCY_SHORT_DES = 'USD'
               THEN gte.DC_AMOUNT * fx.RATE
           ELSE gte.DC_AMOUNT
           END                AS tzsTransactionAmount

FROM GLI_TRX_EXTRACT AS gte
         LEFT JOIN LOAN_ACCOUNT AS la
                   ON la.CUST_ID = gte.CUST_ID

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
                                 '1.1.0.00.0001', '1.1.0.00.0005', '1.1.0.01.0001', '1.1.0.01.0012',
                                 '1.1.0.02.0001', '1.1.0.02.0002', '1.1.0.02.0003', '1.1.0.02.0005',
                                 '1.1.0.02.0007', '1.1.0.02.0008', '1.1.0.02.0009', '1.1.0.02.0011',
                                 '1.1.0.03.0001', '1.1.0.03.0002', '1.1.0.03.0003', '1.1.0.03.0004',
                                 '1.2.0.00.0001', '1.2.0.00.0005', '1.2.0.01.0001', '1.2.0.02.0001',
                                 '1.2.0.02.0002', '1.2.0.01.0012', '1.2.0.03.0002', '1.2.0.05.0001',
                                 '1.2.0.03.0003', '1.2.0.03.0006', '1.2.0.05.0005', '1.2.0.02.0007',
                                 '1.3.0.00.0005'
    )
  AND gte.PRF_ACCOUNT_NUMBER IS NOT NULL
  AND TRIM(gte.PRF_ACCOUNT_NUMBER) <> '';