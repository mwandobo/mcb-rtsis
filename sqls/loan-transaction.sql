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
               THEN gte.DC_AMOUNT * 2500.00 -- Update this rate as needed
           ELSE gte.DC_AMOUNT
           END                AS tzsTransactionAmount

FROM GLI_TRX_EXTRACT AS gte
         LEFT JOIN LOAN_ACCOUNT AS la
                   ON la.CUST_ID = gte.CUST_ID
         LEFT JOIN GLG_ACCOUNT AS gl ON gl.ACCOUNT_ID = gte.FK_GLG_ACCOUNTACCO

WHERE gl.EXTERNAL_GLACCOUNT IN (
                                '110000001', '110000005', '110010001', '110010012',
                                '110020001', '110020002', '110020003', '110020005',
                                '110020007', '110020008', '110020009', '110020011',
                                '110030001', '110030002', '110030003', '110030004',
                                '120000001', '120000005', '120010001', '120020001',
                                '120020002', '120010012', '120030002', '120050001',
                                '120030003', '120030006', '120050005', '120020007',
                                '130000005'
    )
  AND gte.PRF_ACCOUNT_NUMBER IS NOT NULL;