create table CLC_PROSPECT_CASE
(
    TRX_UNIT          INTEGER      not null,
    TRX_DATE          DATE         not null,
    TRX_USER          CHAR(8)      not null,
    TRX_USR_SN        INTEGER      not null,
    TMSTAMP           TIMESTAMP(6) not null,
    PRFT_SYSTEM       SMALLINT,
    ACCOUNT_NUMBER    CHAR(40),
    ACCOUNT_CD        SMALLINT,
    ACCOUNT_BAL       DECIMAL(18, 2),
    OVERDUE_BAL       DECIMAL(18, 2),
    OVERDUE_DAYS      DECIMAL(10),
    LOAN_STATUS       CHAR(1),
    ACC_CLASS         CHAR(5),
    ACC_SUBCLASS      CHAR(5),
    ACC_UNIT          INTEGER,
    ACC_PRODUCT       INTEGER,
    ACC_CURRENCY      INTEGER,
    DOMESTIC_ACC_BAL  DECIMAL(18, 2),
    DOMESTIC_OVER_BAL DECIMAL(18, 2),
    UPDATED_TMSTAMP   TIMESTAMP(6),
    CASE_PROCESSED    CHAR(1),
    constraint CLC_COLLECT_PK_40
        primary key (TRX_UNIT, TRX_DATE, TRX_USER, TRX_USR_SN, TMSTAMP)
);

