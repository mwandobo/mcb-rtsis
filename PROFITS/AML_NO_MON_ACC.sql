create table AML_NO_MON_ACC
(
    PRFT_ACCOUNT_NUM   CHAR(40)     not null,
    PRFT_SYSTEM        SMALLINT     not null,
    TRX_DATE           DATE         not null,
    TRX_CODE           INTEGER      not null,
    TRX_USR_SN         INTEGER      not null,
    LF_INSTITUTE       CHAR(4)      not null,
    LF_EMPL_NO         CHAR(8)      not null,
    LF_TIMESTAMP       TIMESTAMP(6) not null,
    LF_CUSTNO          INTEGER      not null,
    LF_BUSINESSTYPE    INTEGER      not null,
    AML_PARAMTYPE      CHAR(5)      not null,
    LF_ACCNO           DECIMAL(11)  not null,
    LF_BUSINESSNO      CHAR(11)     not null,
    LF_ACC_CURRENCYISO CHAR(3)      not null,
    LF_ACTION          CHAR(32)     not null,
    LF_ACTION_CODE     CHAR(32),
    PROC_FLAG          CHAR(1),
    constraint PK_AML_NO_MON_ACC
        primary key (LF_TIMESTAMP, LF_EMPL_NO, TRX_USR_SN, TRX_CODE, TRX_DATE, PRFT_SYSTEM, PRFT_ACCOUNT_NUM, LF_ACTION)
);

