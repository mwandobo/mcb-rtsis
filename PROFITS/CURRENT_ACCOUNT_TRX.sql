create table CURRENT_ACCOUNT_TRX
(
    CUST_ID          INTEGER      not null,
    TMSTAMP          TIMESTAMP(6) not null,
    CR_TUN_INTER_SN  SMALLINT     not null,
    CR_TRX_USR_SN    INTEGER      not null,
    DB_TUN_INTER_SN  SMALLINT     not null,
    DB_TRX_USR_SN    INTEGER      not null,
    TRX_USER         CHAR(8)      not null,
    TRX_DATE         DATE         not null,
    TRX_UNIT         INTEGER      not null,
    SECOND_ACCOUNT   CHAR(40)     not null,
    FIRST_ACCOUNT    CHAR(40)     not null,
    DB_TRX_CODE      INTEGER,
    CR_JUSTIFIC_CODE INTEGER,
    CR_TRX_CODE      INTEGER,
    DB_JUSTIFIC_CODE INTEGER,
    SECOND_ACC_UNIT  INTEGER,
    FIRST_ACC_UNIT   INTEGER,
    FX_TRX_USR_SN    INTEGER,
    TRX_AMOUNT       DECIMAL(15, 2),
    CREDIT_AMOUNT    DECIMAL(15, 2),
    DEBIT_AMOUNT     DECIMAL(15, 2),
    VALUE_DATE       DATE,
    REVERSED_FLG     CHAR(1),
    CREDIT_CURRENCY  CHAR(5),
    DEBIT_CURRENCY   CHAR(5),
    TRX_CURRENCY     CHAR(5),
    TRX_COMMENTS     CHAR(40),
    constraint IXU_DEP_156
        primary key (CUST_ID, TMSTAMP, CR_TUN_INTER_SN, CR_TRX_USR_SN, DB_TUN_INTER_SN, DB_TRX_USR_SN, TRX_USER,
                     TRX_DATE, TRX_UNIT, SECOND_ACCOUNT, FIRST_ACCOUNT)
);

