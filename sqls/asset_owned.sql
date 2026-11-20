SELECT
    VARCHAR_FORMAT(CURRENT_TIMESTAMP,'DDMMYYYYHHMM') AS reportingDate,
    VARCHAR_FORMAT(M.AQUISITION_DATE,'DDMMYYYYHHMM') as acqiusitionDate,
    CU.SHORT_DESCR as currency,
    CASE
        WHEN gl.EXTERNAL_GLACCOUNT = '144000020' OR
             gl.EXTERNAL_GLACCOUNT = '144000052' OR
             gl.EXTERNAL_GLACCOUNT = '170150001' OR
             gl.EXTERNAL_GLACCOUNT = '171030001' OR
             gl.EXTERNAL_GLACCOUNT = '170150002' THEN 'Intangible'
        WHEN gl.EXTERNAL_GLACCOUNT = '171020001' THEN 'Immovable'
        WHEN gl.EXTERNAL_GLACCOUNT = '170050001' OR
             gl.EXTERNAL_GLACCOUNT = '170120001' OR
             gl.EXTERNAL_GLACCOUNT = '171090001' OR
             gl.EXTERNAL_GLACCOUNT = '161020001' OR
             gl.EXTERNAL_GLACCOUNT = '170090001' OR
             gl.EXTERNAL_GLACCOUNT = '170070001' THEN 'Movable'

        ELSE 'Other'
    END AS assetCategory,
    CASE
        WHEN M.GL_ACCOUNT = '1.7.0.12.0001' THEN 'Computer'
        WHEN M.GL_ACCOUNT = '1.7.0.09.0001' THEN 'Motor Vehicle'
        WHEN M.GL_ACCOUNT = '1.7.0.07.0001' THEN 'Machinery And Equipment'
        WHEN M.GL_ACCOUNT = '1.7.0.05.0001' THEN 'Furniture and fittings'
        WHEN M.GL_ACCOUNT = '1.6.1.02.0001' THEN 'Buildings and improvements'
        WHEN M.GL_ACCOUNT = '1.7.0.15.0001' THEN 'Infrastructure investments'
        ELSE 'Other'
    END AS assetType,
    -- orgAmount: always original DC_AMOUNT
    M.AMOUNT AS orgCostValue,

    -- USD amount using the latest active fixing rate
    CASE
        WHEN CU.SHORT_DESCR = 'TZS' THEN 0
        WHEN CU.SHORT_DESCR = 'USD' THEN DECIMAL(M.AMOUNT, 18, 2)
        WHEN CU.SHORT_DESCR <> 'USD' THEN DECIMAL(M.AMOUNT / fx.rate, 18, 2)
    END AS usdCostValue,

    -- TZS amount using the latest active fixing rate
    CASE
        WHEN CU.SHORT_DESCR = 'USD' THEN DECIMAL(M.AMOUNT * fx.rate, 18, 2)
        ELSE DECIMAL(M.AMOUNT, 18, 2)
    END AS tzsCostValue,
    0 as allowanceProbableLoss,
    0 as botProvision
FROM ASSET_MASTER AS M
LEFT JOIN CURRENCY as CU ON CU.ID_CURRENCY = M.CURRENCY_ID
LEFT JOIN GLG_ACCOUNT AS gl
    ON gl.ACCOUNT_ID = M.GL_ACCOUNT
LEFT JOIN (
    SELECT fr.fk_currencyid_curr,
           fr.rate
    FROM fixing_rate fr
    WHERE (fr.fk_currencyid_curr, fr.activation_date, fr.activation_time) IN (
        SELECT fk_currencyid_curr,
               activation_date,
               MAX(activation_time)
        FROM fixing_rate
        WHERE activation_date = (
            SELECT MAX(b.activation_date)
            FROM fixing_rate b
            WHERE b.activation_date <= CURRENT_DATE
        )
        GROUP BY fk_currencyid_curr, activation_date
    )
) fx ON fx.fk_currencyid_curr = CU.ID_CURRENCY;
