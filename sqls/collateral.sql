SELECT
    p.DESCRIPTION                     AS collateralPledged,
    ac.EST_VALUE_AMN                  AS orgCollateralValue,
    ac.EST_VALUE_AMN / 2500           AS usdCollateralValue,
    ac.EST_VALUE_AMN                  AS tzsCollateralValue

FROM ACCOUNT_COLLATERAL ac
JOIN PRODUCT AS p ON p.ID_PRODUCT = ac.FK_COLLATERALFK_CO;