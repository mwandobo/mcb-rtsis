create table USER_INFORMED
(
    SN             DECIMAL(15)  not null,
    TMSTAMP        TIMESTAMP(6) not null,
    SHOW_USER      CHAR(8)      not null,
    CUST_ID        INTEGER      not null,
    PRFT_SYSTEM    SMALLINT,
    TRX_INFORMED   INTEGER,
    TRX_CODE       INTEGER,
    SHOW_UNIT      INTEGER,
    SHOW_DATE      DATE,
    ACCOUNT_NUMBER CHAR(40),
    SHOW_NO_MORE   CHAR(1),
    constraint IXU_CIS_181
        primary key (SN, TMSTAMP, SHOW_USER, CUST_ID)
);

create unique index USER_INFORMED_CUSTID
    on USER_INFORMED (CUST_ID);

