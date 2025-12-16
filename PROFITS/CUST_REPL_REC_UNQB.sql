create table CUST_REPL_REC_UNQB
(
    TBNAME       VARCHAR(40)  not null,
    TBCOL        VARCHAR(40)  not null,
    CH_ID1       VARCHAR(40),
    CH_ID2       VARCHAR(40),
    CH_ID3       VARCHAR(40),
    CH_ID4       VARCHAR(40),
    CH_ID5       VARCHAR(40),
    NU_ID1       DECIMAL(12),
    NU_ID2       DECIMAL(12),
    NU_ID3       DECIMAL(12),
    NU_ID4       DECIMAL(12),
    NU_ID5       DECIMAL(12),
    DT_ID1       DATE,
    DT_ID2       DATE,
    TS_ID1       TIMESTAMP(6) not null,
    CUST_OLD     INTEGER      not null,
    CUST_NEW     INTEGER      not null,
    ENTRY_STATUS CHAR(1)
);

