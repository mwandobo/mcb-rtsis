SELECT
    (SELECT CURRENT TIMESTAMP FROM SYSIBM.SYSDUMMY1)                     AS reportingDate,

    -- INTEREST INCOME (all 4xxxxx except NPL interest in 40xxxxx / 41xxxxx)
    SUM(CASE WHEN gl.EXTERNAL_GLACCOUNT LIKE '4%'
             AND gl.EXTERNAL_GLACCOUNT NOT LIKE '40%'
             AND gl.EXTERNAL_GLACCOUNT NOT LIKE '41%'
             THEN DECIMAL(REPLACE(TRIM(gte.DC_AMOUNT), ',', ''), 31, 2)
             ELSE 0 END)                                                AS interestIncome,

    -- INTEREST EXPENSE (all 6xxxxx)
    SUM(CASE WHEN gl.EXTERNAL_GLACCOUNT LIKE '6%'
             THEN DECIMAL(REPLACE(TRIM(gte.DC_AMOUNT), ',', ''), 31, 2)
             ELSE 0 END)                                                AS interestExpense,

    -- BAD DEBTS WRITTEN OFF (not previously provided for)
    SUM(CASE WHEN gl.EXTERNAL_GLACCOUNT IN ('705190002','705190003')
             THEN DECIMAL(REPLACE(TRIM(gte.DC_AMOUNT), ',', ''), 31, 2)
             ELSE 0 END)                                                AS badDebtsWrittenOffNotProvided,

    -- PROVISION FOR BAD & DOUBTFUL DEBTS (ECL / BoT charge)
    SUM(CASE WHEN gl.EXTERNAL_GLACCOUNT IN ('705190001','705190002','705190003')
             THEN DECIMAL(REPLACE(TRIM(gte.DC_AMOUNT), ',', ''), 31, 2)
             ELSE 0 END)                                                AS provisionBadDoubtfulDebts,

    -- IMPAIRMENTS ON INVESTMENTS (none in your list → 0)
    CAST(0 AS DECIMAL(31,2))                                            AS impairmentsInvestments,

    -- NON-INTEREST INCOME (fees, commissions, FX, other income)
    SUM(CASE WHEN gl.EXTERNAL_GLACCOUNT LIKE '50%'
                  OR gl.EXTERNAL_GLACCOUNT LIKE '502%'
                  OR gl.EXTERNAL_GLACCOUNT LIKE '503%'
                  OR gl.EXTERNAL_GLACCOUNT LIKE '504%'
                  OR gl.EXTERNAL_GLACCOUNT LIKE '505%'
             THEN DECIMAL(REPLACE(TRIM(gte.DC_AMOUNT), ',', ''), 31, 2)
             ELSE 0 END)                                                AS nonInterestIncome,

    -- NON-INTEREST EXPENSES (all 7xxxxx operating expenses)
    SUM(CASE WHEN gl.EXTERNAL_GLACCOUNT LIKE '7%'
             THEN DECIMAL(REPLACE(TRIM(gte.DC_AMOUNT), ',', ''), 31, 2)
             ELSE 0 END)                                                AS nonInterestExpenses,

    -- INCOME TAX PROVISION (add your tax GL if exists, otherwise 0)
    CAST(0 AS DECIMAL(31,2))                                            AS incomeTaxProvision,

    -- EXTRAORDINARY CREDITS / CHARGES
    CAST(0 AS DECIMAL(31,2))                                            AS extraordinaryCreditsCharge,

    -- NON-CORE CREDITS / CHARGES (gains on disposal, recoveries, etc.)
    SUM(CASE WHEN gl.EXTERNAL_GLACCOUNT IN ('505010001','505070001')
             THEN DECIMAL(REPLACE(TRIM(gte.DC_AMOUNT), ',', ''), 31, 2)
             ELSE 0 END)                                                AS nonCoreCreditsCharges,

    -- === REPEATED AMOUNT FIELDS (exactly as you requested) ===
    SUM(CASE WHEN gl.EXTERNAL_GLACCOUNT LIKE '4%'
             AND gl.EXTERNAL_GLACCOUNT NOT LIKE '40%'
             AND gl.EXTERNAL_GLACCOUNT NOT LIKE '41%'
             THEN DECIMAL(REPLACE(TRIM(gte.DC_AMOUNT), ',', ''), 31, 2)
             ELSE 0 END)                                                AS amountInterestIncome,

    SUM(CASE WHEN gl.EXTERNAL_GLACCOUNT LIKE '6%'
             THEN DECIMAL(REPLACE(TRIM(gte.DC_AMOUNT), ',', ''), 31, 2)
             ELSE 0 END)                                                AS amountInterestExpenses,

    SUM(CASE WHEN gl.EXTERNAL_GLACCOUNT LIKE '50%'
                  OR gl.EXTERNAL_GLACCOUNT LIKE '502%'
                  OR gl.EXTERNAL_GLACCOUNT LIKE '503%'
                  OR gl.EXTERNAL_GLACCOUNT LIKE '504%'
                  OR gl.EXTERNAL_GLACCOUNT LIKE '505%'
             THEN DECIMAL(REPLACE(TRIM(gte.DC_AMOUNT), ',', ''), 31, 2)
             ELSE 0 END)                                                AS amountNonInterestIncome,

    SUM(CASE WHEN gl.EXTERNAL_GLACCOUNT LIKE '7%'
             THEN DECIMAL(REPLACE(TRIM(gte.DC_AMOUNT), ',', ''), 31, 2)
             ELSE 0 END)                                                AS amountNonInterestExpenses,

    SUM(CASE WHEN gl.EXTERNAL_GLACCOUNT IN ('505010001','505070001')
             THEN DECIMAL(REPLACE(TRIM(gte.DC_AMOUNT), ',', ''), 31, 2)
             ELSE 0 END)                                                AS amountnonCoreCreditsCharges

FROM GLI_TRX_EXTRACT gte
LEFT JOIN GLG_ACCOUNT gl
       ON gl.ACCOUNT_ID = gte.FK_GLG_ACCOUNTACCO

WHERE gl.EXTERNAL_GLACCOUNT IN (
    -- (all your GL accounts – same long list as before)
    '400010001','400010005','400020001','400020012','400030001','400030002','400030003','400030005',
    '400040001','400040002','400040003','400040006','400060001','400060002','400060004','400060005',
    '400061001','400070003','401010001','401010003','401010005','401020001','401020012','401030001',
    '401030002','401030005','401040002','401040003','401040004','401040006','402030002','402030003',
    '402030005','402040001','402040002','403030005','403040002','404020001','408010001','400030007',
    '400030009','500010009','500010010','500010015','500020001','500020003','500020010','500020011',
    '500020014','500020015','500020016','500020017','500020018','500020019','500020020','500020027',
    '500020030','500020036','500020038','500020055','500020056','500020058','500020062','500020065',
    '500020070','502010001','503010001','503010011','503010012','504040001','504040002','504050001',
    '504060001','504080001','504100001','504110001','504120002','504130001','505000000','505010001',
    '505040001','505040002','505040006','505040009','505070001','600220001','600220002','600220003',
    '600220004','600220005','600220006','600220009','600220010','600230001','600230002','600230003',
    '600230004','600230005','600230006','600240001','601060001','601070001','602010001','700010001',
    '700010004','700010005','700020002','700030001','700040002','700050001','700060001','700090001',
    '700090003','700090004','700090005','700090006','700090009','700090011','700090013','700090015',
    '700010017','701010001','701010002','701010003','701010006','701010008','701010009','701010010',
    '701010011','701010012','701010013','701020001','701020005','701020006','702010002','702030001',
    '702030003','702030004','702040002','702040004','702040006','703020001','703020004','703020006',
    '703020008','703020011','704010001','704020001','704020002','704020004','704020007','704020012',
    '704020013','704030004','705010001','705010002','705010003','705020001','705040005','705040009',
    '705040011','705050001','705060002','705060004','705070001','705080002','705090001','705100003',
    '705100004','705110002','705110003','705110004','705110005','705110006','705110007','705120001',
    '705120005','705120006','705160001','705160002','705160003','705170001','705190001','705190002',
    '705190003'
);
