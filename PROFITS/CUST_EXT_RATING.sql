create table CUST_EXT_RATING
(
    CUST_ID   INTEGER,
    CREATE_DT DATE,
    RATE_DT   DATE,
    PRV_RATE  CHAR(1),
    CUR_RATE  CHAR(1),
    RATING    CHAR(10)
);

create unique index CIEXTRAT
    on CUST_EXT_RATING (CUST_ID, CREATE_DT, RATE_DT);

