-- Debt Securities Investments
-- Based on GL accounts that typically hold debt securities investments
SELECT 
    VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') AS reportingDate,
    gte.FK_UNITCODETRXUNIT AS branchCode,
    
    -- Security Type based on GL Account
    CASE 
        WHEN gl.EXTERNAL_GLACCOUNT LIKE '13%' THEN 'Government Securities'
        WHEN gl.EXTERNAL_GLACCOUNT LIKE '14%' THEN 'Corporate Bonds'
        WHEN gl.EXTERNAL_GLACCOUNT LIKE '15%' THEN 'Treasury Bills'
        WHEN gl.EXTERNAL_GLACCOUNT LIKE '16%' THEN 'Other Debt Securities'
        ELSE 'Unclassified'
    END AS securityType,
    
    -- Classification
    CASE 
        WHEN gl.EXTERNAL_GLACCOUNT LIKE '%01' THEN 'Held to Maturity'
        WHEN gl.EXTERNAL_GLACCOUNT LIKE '%02' THEN 'Available for Sale'
        WHEN gl.EXTERNAL_GLACCOUNT LIKE '%03' THEN 'Trading Securities'
        ELSE 'Other'
    END AS classification,
    
    gl.EXTERNAL_GLACCOUNT AS glAccount,
    gl.ACCOUNT_ID AS glAccountId,
    gte.CURRENCY_SHORT_DES AS currency,
    
    -- Original Amount (always in original currency)
    gte.DC_AMOUNT AS orgAmount,
    
    -- USD Amount: convert if needed
    CASE
        WHEN gte.CURRENCY_SHORT_DES = 'USD'
            THEN DECIMAL(gte.DC_AMOUNT, 18, 2)
        WHEN gte.CURRENCY_SHORT_DES <> 'USD'
            THEN DECIMAL(gte.DC_AMOUNT / fx.rate, 18, 2)
        ELSE NULL
    END AS usdAmount,
    
    -- TZS Amount: convert if needed
    CASE
        WHEN gte.CURRENCY_SHORT_DES = 'USD'
            THEN DECIMAL(gte.DC_AMOUNT * fx.rate, 18, 2)
        ELSE DECIMAL(gte.DC_AMOUNT, 18, 2)
    END AS tzsAmount,
    
    VARCHAR_FORMAT(gte.TRN_DATE, 'DDMMYYYYHHMM') AS transactionDate,
    VARCHAR_FORMAT(gte.VALEUR_DATE, 'DDMMYYYYHHMM') AS valueDate,
    VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') AS maturityDate,
    
    -- Interest and Accruals
    0 AS accruedInterest,
    0 AS unrealizedGainLoss,
    0 AS allowanceProbableLoss,
    0 AS botProvision,
    
    -- Additional Information
    gte.CUST_ID AS customerId,
    gte.FK_JUSTIFICID_JUST AS justificationCode,
    gte.REMARKS AS remarks

FROM GLI_TRX_EXTRACT gte

-- Join GL Account
JOIN GLG_ACCOUNT gl 
    ON gte.FK_GLG_ACCOUNTACCO = gl.ACCOUNT_ID

-- Join Currency
LEFT JOIN CURRENCY curr
    ON curr.SHORT_DESCR = gte.CURRENCY_SHORT_DES

-- Latest Fixing Rate Per Currency
LEFT JOIN (
    SELECT 
        fr.fk_currencyid_curr,
        fr.rate
    FROM fixing_rate fr
    WHERE (fr.fk_currencyid_curr, fr.activation_date, fr.activation_time) IN (
        SELECT 
            fk_currencyid_curr,
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
) fx ON fx.fk_currencyid_curr = curr.ID_CURRENCY

WHERE 
    -- Filter for debt securities GL accounts
    -- Adjust these account patterns based on your chart of accounts
    (
        gl.EXTERNAL_GLACCOUNT LIKE '13%'  -- Government Securities
        OR gl.EXTERNAL_GLACCOUNT LIKE '14%'  -- Corporate Bonds
        OR gl.EXTERNAL_GLACCOUNT LIKE '15%'  -- Treasury Bills
        OR gl.EXTERNAL_GLACCOUNT LIKE '16%'  -- Other Debt Securities
        -- Add specific account numbers if known
        -- OR gl.EXTERNAL_GLACCOUNT IN ('130000001', '140000001', etc.)
    )
    AND gte.DC_AMOUNT <> 0  -- Exclude zero balances
;
