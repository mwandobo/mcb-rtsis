create table ATM_DELTA_ACC_BALANCES
(
    PRFT_ACCOUNT_NUM  CHAR(40)    not null,
    PRFT_SYSTEM       SMALLINT    not null,
    PRFT_ACCOUNT_CD   SMALLINT    not null,
    ACCOUNT_NUMBER    DECIMAL(15) not null,
    TYPE_OF_ACCOUNT   CHAR(1)     not null,
    FILE_CODE         SMALLINT    not null,
    ACC_TYPE          CHAR(4)     not null,
    BOOK_BALANCE      DECIMAL(15, 2),
    AVAILABLE_BALANCE DECIMAL(15, 2),
    TMSTAMP           TIMESTAMP(6),
    constraint ATM_DELTA_ACC_BAL_PK
        primary key (PRFT_SYSTEM, PRFT_ACCOUNT_NUM)
);

