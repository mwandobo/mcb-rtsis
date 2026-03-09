SELECT
    VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM')       AS reportingDate,

    LEFT(TRIM(ca.FULL_CARD_NO), 6)                          AS binNumber,

    VARCHAR_FORMAT(
        COALESCE(
            NULLIF(MIN(ca.CREATION_DATE), DATE('0001-01-01')),
            NULLIF(MIN(ca.PROD_DATE),     DATE('0001-01-01')),
            MIN(ca.TUN_DATE)
        ), 'DDMMYYYYHHMM'
    )                                                        AS binNumberStartDate,

    'TZS'                                                    AS currency,

    -- cardType mapped to BOT D111
    CASE gd.SERIAL_NUM
        WHEN 1 THEN 'Debit'
        WHEN 2 THEN 'Credit'
        ELSE 'Prepaid'
    END                                                      AS cardType,

    -- cardTypeSubCategory: only Credit cards have subcategories in D111
    CASE gd.SERIAL_NUM
        WHEN 1 THEN NULL          -- Debit has no subcategory
        WHEN 2 THEN 'CreditCard'  -- Standard revolving credit card
        ELSE NULL                 -- Prepaid has no subcategory
    END                                                      AS cardTypeSubCategory,

    'VISA'                                                   AS cardSchemeName,
    'Domestic'                                               AS cardIssuerCategory,
    'Mwalimu Commercial Bank Plc'                            AS cardIssuer

FROM CMS_CARD ca
    LEFT JOIN GENERIC_DETAIL gd
           ON gd.FK_GENERIC_HEADPAR = 'CARTP'
          AND gd.SERIAL_NUM         = ca.FK_CRDTYP_GENERIC_SN

WHERE ca.FULL_CARD_NO IS NOT NULL
  AND LEFT(TRIM(ca.FULL_CARD_NO), 6) <> '000000'
  AND gd.SERIAL_NUM <> 0

GROUP BY
    LEFT(TRIM(ca.FULL_CARD_NO), 6),
    gd.SERIAL_NUM,
    gd.DESCRIPTION

ORDER BY binNumber;