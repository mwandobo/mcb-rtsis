create table SWT_TRANSACTION
(
    PRFT_REF_NO      CHAR(16) not null,
    TRX_REF_NO_20    CHAR(16) not null,
    MSG_TYPE         CHAR(20) not null,
    MSG_CATEGORY     CHAR(1)  not null,
    SN               INTEGER  not null,
    ID_TRANSACT      INTEGER  not null,
    ID_JUSTIFIC      INTEGER  not null,
    TRX_CURRENCY     CHAR(3),
    TRX_AMOUNT_DB    DECIMAL(15, 2),
    TRX_AMOUNT_CR    DECIMAL(15, 2),
    TRX_SUM_DB       DECIMAL(15, 2),
    TRX_SUM_CR       DECIMAL(15, 2),
    TRX_ACCOUNT      CHAR(40),
    TRX_VALEUR       DATE,
    TRX_VALEUR_DAYS  SMALLINT,
    TRX_AVAILABILITY DATE,
    TRX_AVAIL_DAYS   SMALLINT,
    TRX_CUSTOMER_BIC CHAR(11) not null,
    COMMENTS         VARCHAR(40),
    ACCOUNT_NUMBER   CHAR(40) not null,
    ACCOUNT_CD       SMALLINT,
    CUST_ID          INTEGER  not null,
    C_DIGIT          SMALLINT not null,
    constraint PK_SWT_MSG_TRX
        primary key (ID_JUSTIFIC, ID_TRANSACT, SN, PRFT_REF_NO)
);

