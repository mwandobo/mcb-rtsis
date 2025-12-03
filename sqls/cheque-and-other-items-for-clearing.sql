--CHeque
SELECT
    CURRENT_TIMESTAMP as reportingDate,
   cfc.CHEQUE_NUMBER AS chequeNumber,
  null  AS bankCode,
  CASE
    WHEN UPPER(ic.FIRST_NAME) = 'ECOBANK' THEN 'ECOCTZTZXXX'
    WHEN UPPER(ic.FIRST_NAME) = 'BOA' THEN 'EUAFTZTZXXX'
    WHEN UPPER(ic.FIRST_NAME) = 'BANK OF TANZANIA' THEN 'EUAFTZTZXXX'
    WHEN UPPER(ic.FIRST_NAME) = 'TPB' THEN 'TAPBTZTZ'
    WHEN UPPER(ic.FIRST_NAME) = 'TANZANIA POSTAL BANK' THEN 'TAPBTZTZXX'
    ELSE 'MWALIMU COMMERCIAL BANK'
  END AS issuerBankerCode,
    ic.FIRST_NAME as rowIssuerBankerCode,
    pc.FIRST_NAME as payeeName,
    ppa.ACCOUNT_NUMBER as payeeAccountNumber,
    cfc.ISSUE_DATE as chequeDate,
    cfc.TRX_DATE as transactionDate,
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
JOIN
    PROFITS_ACCOUNT ppa ON ppa.CUST_ID = pc.CUST_ID;