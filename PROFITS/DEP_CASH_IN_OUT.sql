create table DEP_CASH_IN_OUT
(
    ACCOUNT_NUMBER     DECIMAL(11) not null,
    TRX_UNIT           INTEGER     not null,
    TRX_DATE           DATE        not null,
    TRX_USR            CHAR(8)     not null,
    TRX_USR_SN         INTEGER     not null,
    TUN_INTERNAL_SN    SMALLINT    not null,
    TRN_TYPE           CHAR(1),
    AVAILABILITY_DATE  DATE,
    AMOUNT             DECIMAL(15, 2),
    USED_AMOUNT        DECIMAL(15, 2),
    AVAILABLE          DECIMAL(15, 2),
    TRX_CODE           INTEGER,
    CUST_ID            INTEGER,
    BENEFICIARY_SN     SMALLINT,
    TRANSACTION_STATUS CHAR(1),
    OTHER_SYSTEM       SMALLINT,
    TMSTAMP            TIMESTAMP(6),
    constraint PK_DEP_CASH_IN_OUT
        primary key (ACCOUNT_NUMBER, TRX_UNIT, TUN_INTERNAL_SN, TRX_USR, TRX_USR_SN, TRX_DATE)
);

