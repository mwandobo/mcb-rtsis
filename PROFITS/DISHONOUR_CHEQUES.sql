create table DISHONOUR_CHEQUES
(
    CUST_ID        INTEGER     not null,
    CHEQUE_NUMBER  DECIMAL(10) not null,
    ACCOUNT_NUMBER DECIMAL(11) not null,
    CHEQUE_CD      SMALLINT,
    ACCOUNT_CD     SMALLINT,
    CHEQUE_AMOUNT  DECIMAL(15, 2),
    TMSTAMP        TIMESTAMP(6),
    DEP_ON_US_FLAG CHAR(1),
    KAP_FLAG       CHAR(1),
    constraint IXU_DEP_131
        primary key (CUST_ID, CHEQUE_NUMBER, ACCOUNT_NUMBER)
);

