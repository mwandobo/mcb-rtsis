create table CUSTOMER_CONFIRM
(
    TMSTAMP          TIMESTAMP(6) not null,
    CUST_ID          INTEGER      not null,
    CONFIRM_UNIT     INTEGER,
    CONFIRM_DATE     DATE,
    NEXT_CONFIRM_DT  DATE,
    CONFIRM_USER     CHAR(8),
    CONFIRM_COMMENTS CHAR(200),
    constraint IXU_CIS_171
        primary key (TMSTAMP, CUST_ID)
);

