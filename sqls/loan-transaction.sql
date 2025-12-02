SELECT
    CURRENT_TIMESTAMP                                           AS reportingDate,
    W.AGREEMENT_NUMBER                                          AS loanNumber,
    gte.TRN_DATE                                                AS transactionDate,
    ''                                                          AS loanTransactionType,
    NULL                                                        AS loanTransactionSubType,
    gte.CURRENCY_SHORT_DES                                      AS currency,

    -- Original transaction amount in its native currency
    gte.DC_AMOUNT                                               AS orgTransactionAmount,

    -- USD amount: only populated when currency is USD
    CASE 
        WHEN gte.CURRENCY_SHORT_DES = 'USD' THEN gte.DC_AMOUNT 
        ELSE NULL 
    END                                                         AS usdTransactionAmount,

    -- TZS amount: convert from USD using fixed rate, otherwise use original
    CASE 
        WHEN gte.CURRENCY_SHORT_DES = 'USD' 
            THEN gte.DC_AMOUNT * 2500.00   -- Update this rate as needed
        ELSE gte.DC_AMOUNT 
    END                                                         AS tzsTransactionAmount

FROM GLI_TRX_EXTRACT AS gte
LEFT JOIN W_EOM_LOAN_ACCOUNT AS W 
    ON W.CUST_ID = gte.CUST_ID
LEFT JOIN GLG_ACCOUNT AS gl 
    gl ON gl.ACCOUNT_ID = gte.FK_GLG_ACCOUNTACCO

WHERE gl.EXTERNAL_GLACCOUNT IN (
    '110000001', '110000005', '110010001', '110010012',
    '110020001', '110020002', '110020003', '110020005',
    '110020007', '110020008', '110020009', '110020011',
    '110030001', '110030002', '110030003', '110030004',
    '120000001', '120000005', '120010001', '120020001',
    '120020002', '120010012', '120030002', '120050001',
    '120030003', '120030006', '120050005', '120020007',
    '130000005'
);