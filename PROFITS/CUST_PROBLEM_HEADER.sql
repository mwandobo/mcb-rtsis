create table CUST_PROBLEM_HEADER
(
    CUST_ID       INTEGER      not null,
    TRX_DATE      DATE         not null,
    TMSTAMP       TIMESTAMP(6) not null,
    UNIT_CODE     INTEGER,
    PROBLEM_FLAGS CHAR(80)
);

create unique index IXU_CUS_030
    on CUST_PROBLEM_HEADER (CUST_ID, TRX_DATE, TMSTAMP);

