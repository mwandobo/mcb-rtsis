create table CUSTOMER_CONFIRM_LOG
(
    SN               DECIMAL(10)  not null,
    TMSTAMP          TIMESTAMP(6) not null,
    CUST_ID          INTEGER      not null,
    CONFIRM_UNIT     INTEGER,
    NEXT_CONFIRM_DT  DATE,
    CONFIRM_DATE     DATE,
    CONFIRM_USER     CHAR(8),
    CONFIRM_COMMENTS CHAR(200),
    constraint IXU_CIS_183
        primary key (SN, TMSTAMP, CUST_ID)
);

