--Cash Information
SELECT
    CURRENT_TIMESTAMP as reportingDate,
    gte.FK_UNITCODETRXUNIT AS branchCode,
    CASE
      WHEN gl.EXTERNAL_GLACCOUNT='101000001' THEN 'Cash in vault'
      WHEN gl.EXTERNAL_GLACCOUNT='101000002' THEN 'Petty cash'
      WHEN gl.EXTERNAL_GLACCOUNT='101000010' OR gl.EXTERNAL_GLACCOUNT='101000015' THEN 'Cash in ATMs'
      WHEN gl.EXTERNAL_GLACCOUNT='101000004' OR gl.EXTERNAL_GLACCOUNT='101000011' THEN 'Cash with Tellers'
      ELSE 'unknown'
    END as cashCategory,

    CASE
        WHEN gl.EXTERNAL_GLACCOUNT='101000001' THEN 'CleanNotes'
        WHEN gl.EXTERNAL_GLACCOUNT='101000002'  OR
             gl.EXTERNAL_GLACCOUNT='101000010'  OR
             gl.EXTERNAL_GLACCOUNT='101000004'  OR
             gl.EXTERNAL_GLACCOUNT='101000015'  OR gl.EXTERNAL_GLACCOUNT='101000011' THEN 'Notes'
        ELSE null
    END as cashSubCategory,
    'Business Hours' as cashSubmissionTime,
    gte.CURRENCY_SHORT_DES as currency,
    null as cashDenomination,
    null as quantityOfCoinsNotes
,
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
    gte.TRX_GL_TRN_DATE as transactionDate,
    gte.AVAILABILITY_DATE as maturityDate,
    0 as allowanceProbableLoss,
    0 as botProvision
FROM
    GLI_TRX_EXTRACT AS gte
JOIN
    GLG_ACCOUNT gl ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID
WHERE
    gl.EXTERNAL_GLACCOUNT IN ('101000001','101000002','101000004','101000007','101000010','101000015');