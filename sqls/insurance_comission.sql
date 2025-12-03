SELECT
    (SELECT CURRENT TIMESTAMP FROM SYSIBM.SYSDUMMY1)                    AS reportingDate,
   ''                                                                   AS policyNumber,
    gte.CURRENCY_SHORT_DES                                              AS currency,
    DECIMAL(gte.DC_AMOUNT, 15, 2)                                       AS orgCommissionReceivedAmount,
    CASE
        WHEN UPPER(gte.CURRENCY_SHORT_DES) = 'USD'
            THEN DECIMAL(gte.DC_AMOUNT, 15, 2) * 2730.50
        ELSE DECIMAL(gte.DC_AMOUNT, 15, 2)
    END                                                                 AS tzsCommissionReceivedAmount,
    gte.TRN_DATE                                                        AS commissionReceivedDate

FROM GLI_TRX_EXTRACT gte
JOIN GLG_ACCOUNT AS gl ON gl.ACCOUNT_ID = gte.FK_GLG_ACCOUNTACCO

WHERE gl.EXTERNAL_GLACCOUNT IN ('503010011', '503010012');