select
    CURRENT_TIMESTAMP AS reportingDate,
    gte.FK_GLG_ACCOUNTACCO as accountNumber,
    c.FIRST_NAME as accountName,
CASE
    WHEN UPPER(c.FIRST_NAME) = 'ECOBANK' THEN 'ECOCTZTZXXX'
    WHEN UPPER(c.FIRST_NAME) = 'BOA' THEN 'EUAFTZTZXXX'
    WHEN UPPER(c.FIRST_NAME) = 'TPB' THEN 'TAPBTZTZ'
    WHEN UPPER(c.FIRST_NAME) = 'TANZANIA POSTAL BANK' THEN 'TAPBTZTZXX'
    ELSE VARCHAR(gte.FK0UNITCODE)
END AS bankCode,
    'Tanzania' as Country,
    'Domestic bank related' as relationshipType,
    '' as accountType,
    '' as subAccountType,
    gte.CURRENCY_SHORT_DES as currency,
    -- orgAmount: always original DC_AMOUNT
    gte.DC_AMOUNT AS orgAmount,

    -- USD Amount: only if currency is USD, otherwise null
    CASE
        WHEN gte.CURRENCY_SHORT_DES = 'USD'
            THEN gte.DC_AMOUNT
        ELSE NULL
    END AS usdAmount,

    -- TZS Amount: convert only if USD, otherwise use as is
    CASE
        WHEN gte.CURRENCY_SHORT_DES = 'USD'
            THEN gte.DC_AMOUNT * 2500   -- <<< replace with your rate
        ELSE
            gte.DC_AMOUNT
    END AS tzsAmount,
        gte.TRN_DATE as transactionDate,
 (DATE(gte.AVAILABILITY_DATE) - DATE(gte.TRN_DATE)) AS pastDueDays,
    0 as allowanceProbableLoss,
    0 as botProvision,
    'Current' as assetsClassificationCategory,
    gte.TRN_DATE as contractDate,
    gte.AVAILABILITY_DATE as maturityDate,
'Highly rated Multilateral Development Banks' as externalRatingCorrespondentBank,
'' as gradesUnratedBanks

FROM
GLI_TRX_EXTRACT as gte

JOIN
    GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
JOIN
    CUSTOMER c ON gte.CUST_ID = c.CUST_ID


where gl.EXTERNAL_GLACCOUNT IN('100050001');