create table CUST_PROBLEM_HEA_U
(
    TMSTAMP       TIMESTAMP(6) not null,
    TRX_DATE      DATE         not null,
    CUST_ID       INTEGER      not null,
    UNIT_CODE     INTEGER,
    PROBLEM_FLAGS CHAR(80),
    constraint IXU_CIU_033
        primary key (CUST_ID, TRX_DATE, TMSTAMP)
);

