select
    CURRENT_TIMESTAMP as reportingDate,
    gte.TRN_DATE as transactionDate,
    '' as lenderName,
    '' as borrowerName,
    CASE
        WHEN TIME(gte.TMSTAMP) < TIME('15:30:00') THEN 'market'
        ELSE 'off market'
    END AS transactionType,
    gte.DC_AMOUNT as tzsAmount,
    '' as tenure,
    '' as interestRate
from
    GLI_TRX_EXTRACT as gte
    JOIN GLG_ACCOUNT as gl ON gl.ACCOUNT_ID = gte.FK_GLG_ACCOUNTACCO
WHERE
    gl.EXTERNAL_GLACCOUNT = '102000001';