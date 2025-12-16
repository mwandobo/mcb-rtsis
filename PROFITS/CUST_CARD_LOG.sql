create table CUST_CARD_LOG
(
    SN                 DECIMAL(15)  not null,
    TMSTAMP            TIMESTAMP(6) not null,
    TRX_USER           CHAR(8),
    TRX_UNIT           INTEGER,
    TRX_DATE           DATE,
    TRX_CODE           INTEGER,
    CUST_ID            INTEGER,
    APPLICATION_NO     INTEGER      not null,
    APPLICATION_STATUS CHAR(1),
    constraint PK_CUST_CARD_LOG
        primary key (SN, TMSTAMP, APPLICATION_NO)
);

