create table W_STG_CRM_ACCT_NOTICE
(
    ACCOUNT_NUMBER          CHAR(40) not null
        constraint PK_W_STG_CRM_ACCT_NOTICE
            primary key,
    ARREARS_DATE            DATE,
    LR_NUMBER               VARCHAR(40),
    NOTICE_ISSUE_DATE_90    DATE,
    NOTICE_MATURITY_DATE_90 DATE,
    NOTICE_ISSUE_DATE_40    DATE,
    NOTICE_MATURITY_DATE_40 DATE,
    AUCTION_DATE            DATE,
    COMMENTS                VARCHAR(500),
    AUCTIONEER              VARCHAR(60),
    SALE_PRICE              DECIMAL(20, 2),
    AMOUNT_PAID             DECIMAL(20, 2),
    SALE_DATE               DATE,
    COMPLETION_DATE         DATE,
    CREATE_TIME             TIMESTAMP(6) default '"SYSIBM"."TIMESTAMP"(CURRENT TIMESTAMP)',
    DATE_EFFECTIVE          DATE,
    MONTHLY_RENT_INCOME     DECIMAL(20, 2),
    RENT_EXPECTED           DECIMAL(20, 2),
    RENT_RECIEVED_YTD       DECIMAL(20, 2),
    RENT_RECIEVED_CM        DECIMAL(20, 2),
    MONTHLY_EXPENSES        DECIMAL(20, 2),
    NET_RENT                DECIMAL(20, 2),
    RECEIVER                VARCHAR(35),
    PROPERTY_LOCATION       VARCHAR(35),
    CHANGE                  DECIMAL(20, 2)
);

