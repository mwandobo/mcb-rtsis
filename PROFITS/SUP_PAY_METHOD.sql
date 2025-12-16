create table SUP_PAY_METHOD
(
    SUP_PAY_ID     DECIMAL(5) default 1     not null,
    ID_PRODUCT     DECIMAL(5) default 55000 not null,
    TRX_CODE       DECIMAL(5) default 55000 not null,
    ID_JUSTIFIC    DECIMAL(5) default 55000 not null,
    ACCOUNT_NUMBER CHAR(40),
    ID_CURRENCY    DECIMAL(5) default 19    not null,
    TRX_LUNIT      DECIMAL(5),
    TRX_UNIT       DECIMAL(5),
    TRX_USER       CHAR(8),
    TRX_LUSER      CHAR(8),
    TRX_LDATE      DATE,
    TRX_DATE       DATE,
    ACCOUNT_CD     DECIMAL(2),
    constraint IXU_SUP_PAYMETHD_PK
        primary key (SUP_PAY_ID, ID_CURRENCY)
);

