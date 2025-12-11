--CHeque
SELECT
    CURRENT_TIMESTAMP as reportingDate,
    cfc.CHEQUE_NUMBER AS chequeNumber,
    RTRIM( LTRIM(
        COALESCE(ic.FIRST_NAME, '') || ' ' ||
        COALESCE(ic.MIDDLE_NAME, '') || ' ' ||
        COALESCE(ic.SURNAME, '')
    )
) AS issuerName,
    COALESCE(bic.BIC, 'UNKNOWN') AS issuerBankerCode,
        RTRIM( LTRIM(
        COALESCE(pc.FIRST_NAME, '') || ' ' ||
        COALESCE(pc.MIDDLE_NAME, '') || ' ' ||
        COALESCE(pc.SURNAME, '')
    )
) AS payeeName,
    ppa.ACCOUNT_NUMBER as payeeAccountNumber,
    cfc.ISSUE_DATE as chequeDate,
    cfc.TRX_DATE as transactionDate,
    cfc.BEAR_PAYMENT_DATE as settlementDate,
    0 as allowanceProbableLoss,
    0 as botProvision,
    cu.SHORT_DESCR as currency,
    CAST(da.OPENING_BALANCE AS DECFLOAT) AS orgAmountOpening,
    CAST(da.OPENING_BALANCE  AS DECFLOAT)   AS orgAmountOpening,
    0 as usdAmountOpening,
    CAST(da.OPENING_BALANCE AS DECFLOAT)   AS tzsAmountOpening,
    CAST(cfc.CHEQUE_AMOUNT  AS DECFLOAT)    AS orgAmountPayment,
    0 as usdAmountPayment,
    CAST(cfc.CHEQUE_AMOUNT AS DECFLOAT)    AS tzsAmountPayment,
    CAST(da.BOOK_BALANCE AS DECFLOAT)      AS orgAmountBalance,
    0 as usdAmountBalance,
    CAST(da.BOOK_BALANCE AS DECFLOAT)      AS tzsAmountBalance
    FROM
        CHEQUES_FOR_COLLEC AS cfc
    JOIN DEPOSIT_ACCOUNT da
        ON VARCHAR(da.ACCOUNT_NUMBER) = LTRIM(CHAR(cfc.CHEQUE_NUMBER, 20), '0')
    JOIN
        CUSTOMER ic ON da.FK_CUSTOMERCUST_ID = ic.CUST_ID
    JOIN
        CURRENCY cu ON cfc.FK_CURRENCYID_CURR = cu.ID_CURRENCY
    JOIN
        CUSTOMER pc ON cfc.FK_CUSTOMERCUST_ID = pc.CUST_ID
    LEFT JOIN BANK_BIC_LOOKUP bic
       ON UPPER(TRIM(cfc.DRAWN_BANK)) = UPPER(TRIM(bic.BANK_NAME))
    JOIN
        PROFITS_ACCOUNT ppa ON ppa.CUST_ID = pc.CUST_ID;
