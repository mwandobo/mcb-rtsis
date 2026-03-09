-- DB2 Summary Query for Card Product Pipeline
-- This mirrors the card-product.sql pipeline query
SELECT COUNT(DISTINCT LEFT(TRIM(ca.FULL_CARD_NO), 6)) as record_count
FROM CMS_CARD ca
LEFT JOIN GENERIC_DETAIL gd
       ON gd.FK_GENERIC_HEADPAR = 'CARTP'
      AND gd.SERIAL_NUM = ca.FK_CRDTYP_GENERIC_SN
WHERE ca.FULL_CARD_NO IS NOT NULL
  AND LEFT(TRIM(ca.FULL_CARD_NO), 6) <> '000000'
  AND gd.SERIAL_NUM <> 0