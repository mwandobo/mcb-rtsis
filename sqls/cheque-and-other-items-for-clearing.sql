--CHeque
SELECT
    CURRENT_TIMESTAMP as reportingDate,
    '' AS chequeNumber,
   c.FIRST_NAME  AS issuerName,
  CASE
    WHEN UPPER(c.FIRST_NAME) = 'ECOBANK' THEN 'ECOCTZTZXXX'
    WHEN UPPER(c.FIRST_NAME) = 'BOA' THEN 'EUAFTZTZXXX'
    WHEN UPPER(c.FIRST_NAME) = 'TPB' THEN 'TAPBTZTZ'
    WHEN UPPER(c.FIRST_NAME) = 'TANZANIA POSTAL BANK' THEN 'TAPBTZTZXX'
    ELSE VARCHAR(gte.FK0UNITCODE)
END AS bankCode,
    '' as payeeName,
    '' as payeeAccountNumber,
    '' as chequeDate,
    '' as transactionDate,
    0 as allowanceProbableLoss,
    0 as botProvision,
    gte.CURRENCY_SHORT_DES as currency,
    '' as orgAmountOpening,
    '' as usdAmountOpening,
    '' as tzsAmountOpening,
    '' as orgAmountPayment,
    '' as usdAmountPayment,
    '' as orgAmountBalance,
    '' as orgAmountBalance
FROM
    GLI_TRX_EXTRACT AS gte
JOIN
    GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
JOIN
    CUSTOMER c ON gte.CUST_ID = c.CUST_ID
WHERE
    gl.EXTERNAL_GLACCOUNT IN (
        '100007000','100009000','100026000','100027000','100047000','100048000', '100049000', '230000088');