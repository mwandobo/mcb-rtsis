SELECT
    VARCHAR_FORMAT(CURRENT TIMESTAMP,'DDMMYYYYHH24MI')        AS reportingDate,
    g.CUST_ID                                                 AS clientIdentificationNumber,
    PA.ACCOUNT_NUMBER                                         AS accountNumber,
    c.SELF_NAME                                               AS accountName,
    null                                                      AS customerCategory,
    null                                                      AS customerCountry,
    g.FK_UNITCODETRXUNIT                                      AS branchCode,
    null                                                      AS clientType,
    null                                                      AS relationshipType,
    null                                                      AS district,
    null                                                      AS region,
    null                                                      AS accountProductName,
    null                                                      AS accountType,
    null                                                      AS accountSubType,
    null                                                      AS depositCategory,
    null                                                      AS depositAccountStatus,
    null                                                      AS transactionUniqueRef,
    g.TMSTAMP                                                 AS timeStamp,
    null                                                      AS serviceChannel,
    g.CURRENCY_SHORT_DES                                      AS currency,
    null                                                      AS transactionType,
       g.DC_AMOUNT                                               AS orgTransactionAmount,
    CASE
        WHEN g.CURRENCY_SHORT_DES = 'USD' THEN g.DC_AMOUNT
        ELSE NULL
    END                                                         AS usdTransactionAmount,

    -- TZS Amount: convert only if USD, otherwise use as is
    CASE
        WHEN g.CURRENCY_SHORT_DES = 'USD'
            THEN g.DC_AMOUNT * 2500
        ELSE
            g.DC_AMOUNT
    END                                                        AS tzsTransactionAmount,
    null                                                      AS transactionPurposes,
    null                                                      AS sectorSnaClassification,
    null                                                      AS lienNumber,
    null                                                      AS orgAmountLien,
    null                                                      AS usdAmountLien,
    null                                                      AS tzsAmountLien,
    null                                                      AS contractDate,
    null                                                      AS maturityDate,
    null                                                      AS annualInterestRate,
    null                                                      AS interestRateType,
    g.DC_AMOUNT                                               AS orgInterestAmount,
    CASE
        WHEN g.CURRENCY_SHORT_DES = 'USD' THEN g.DC_AMOUNT
        ELSE NULL
    END                                                         AS usdInterestAmount,

    -- TZS Amount: convert only if USD, otherwise use as is
    CASE
        WHEN g.CURRENCY_SHORT_DES = 'USD'
            THEN g.DC_AMOUNT * 2500
        ELSE
            g.DC_AMOUNT
    END                                                        AS tzsInterestAmount
FROM GLI_TRX_EXTRACT g
LEFT JOIN CUSTOMER c
    ON c.CUST_ID = g.CUST_ID
LEFT JOIN PROFITS_ACCOUNT pa ON pa.CUST_ID = g.CUST_ID AND pa.PRFT_SYSTEM=4
WHERE
    g.JUSTIFIC_DESCR = 'JOURNAL CREDIT';
