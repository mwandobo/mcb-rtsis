-- Account Information Summary Query
-- Returns total count of account information records
SELECT COUNT(*) as record_count
FROM PROFITS_ACCOUNT pa
WHERE TRIM(pa.ACCOUNT_NUMBER) <> 'DUMMY TRS-LG'
  AND TRIM(pa.ACCOUNT_NUMBER) <> ''
  AND pa.PRFT_SYSTEM <> 19
  AND pa.PRODUCT_ID NOT IN (38220, 38801)
