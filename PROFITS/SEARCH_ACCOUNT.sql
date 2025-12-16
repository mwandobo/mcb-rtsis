create table SEARCH_ACCOUNT
(
    PRFT_SYSTEM          SMALLINT     not null,
    ACCOUNT_NUMBER       CHAR(40)     not null,
    CUSTOMER_SEARCH      CHAR(2)      not null,
    TMSTAMP              TIMESTAMP(6) not null,
    TRX_USR              CHAR(8)      not null,
    SN                   INTEGER      not null,
    ACCOUNT_CD           SMALLINT,
    MOVEMENT_CURRENCY    INTEGER,
    PRODUCT_ID           INTEGER,
    MONOTORING_UNIT      INTEGER,
    MAIN_BENEF_ID        INTEGER,
    INTERNAL_SN          INTEGER      not null,
    ACC_STATUS           CHAR(1),
    RELATIONSHIP_TYPE    VARCHAR(12),
    DESCRIPTION          CHAR(60),
    CUSTOMER_SEARCH_DESC CHAR(30),
    COBENEF_COUNT        INTEGER,
    ACC_TYPE_DESC        CHAR(20),
    ACC_STATUS_DESC      CHAR(20),
    BOOK_BALANCE         DECIMAL(15, 2),
    LOAN_STATUS          CHAR(1),
    LOAN_STATUS_DESC     VARCHAR(20),
    constraint PK_SEARCH_ACCOUNT
        primary key (PRFT_SYSTEM, ACCOUNT_NUMBER, CUSTOMER_SEARCH, TMSTAMP, TRX_USR, SN)
);

