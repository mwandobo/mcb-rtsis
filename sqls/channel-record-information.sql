SELECT
    VARCHAR_FORMAT(CURRENT_TIMESTAMP, 'DDMMYYYYHHMM')      AS reportingDate,
    c.CUST_ID                        AS branchCode,
    c.CUST_ID                        AS customerIdentifcationNumber,
    c.CUST_ID                        AS accountNumber,
    c.CUST_ID                        AS customerCategory,
    c.CUST_ID                        AS subscribedChannel,
    c.CUST_ID                        AS subscriptionDate,
    c.CUST_ID                        AS lastTransactionDate,
    c.CUST_ID                        AS channelStatus
FROM CUSTOMER c;