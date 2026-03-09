-- DB2 Summary Query for Cards Pipeline
SELECT COUNT(*) as record_count
FROM CARDS c
         JOIN CUSTOMER crd ON crd.CUST_ID = c.CUST_ID
         JOIN CARD_TYPE ct ON ct.CARD_TYPE_ID = c.CARD_TYPE_ID
         JOIN CARD_PRODUCT cp ON cp.CARD_PRODUCT_ID = c.CARD_PRODUCT_ID