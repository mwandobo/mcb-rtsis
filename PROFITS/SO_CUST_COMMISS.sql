create table SO_CUST_COMMISS
(
    CUST_ID                 INTEGER,
    CUST_CD                 SMALLINT,
    FREQUENCY               SMALLINT,
    COUNT_OF_SO             INTEGER,
    JUSTIFIC_ID             INTEGER,
    NEXT_ACTIV_DATE         DATE,
    LAST_COMMISS_DATE       DATE,
    FREQUENCY_UNITOF_MEAS   CHAR(1),
    LAST_COMMISS_HAS_FAILED CHAR(1)
);

create unique index IXU_SO__007
    on SO_CUST_COMMISS (CUST_ID);

