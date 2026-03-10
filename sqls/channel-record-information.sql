SELECT
    VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') AS reportingDate,
    c.FKUNIT_BELONGS AS branchCode,
    c.CUST_ID AS customerIdentifcationNumber,
    pa.ACCOUNT_NUMBER AS accountNumber,
    CASE c.CUST_TYPE  WHEN '1' THEN 'Individual' ELSE 'Corporate' End as customerCategory,
    'Mobile Banking, Point Of Sale, ATM' AS subscribedChannel,
    VARCHAR_FORMAT(c.TMSTAMP, 'DDMMYYYYHHMM')   AS subscriptionDate,
    VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM') AS lastTransactionDate,
    c.ENTRY_STATUS AS channelStatus
FROM CUSTOMER c

         LEFT JOIN (
    SELECT
        CUST_ID,
        MIN(ACCOUNT_NUMBER) AS ACCOUNT_NUMBER
    FROM PROFITS_ACCOUNT
    GROUP BY CUST_ID
) pa
                   ON c.CUST_ID = pa.CUST_ID;