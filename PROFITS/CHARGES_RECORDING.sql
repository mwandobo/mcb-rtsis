create table CHARGES_RECORDING
(
    TRX_DATE            DATE     not null,
    TRX_UNIT            INTEGER  not null,
    TRX_USER            CHAR(8)  not null,
    TRX_USR_SN          INTEGER  not null,
    GRP_SUBSCRIPT       SMALLINT not null,
    CHARGE_TYPE         CHAR(1)  not null,
    CHARGE_CODE         INTEGER  not null,
    DB_CR_FLG           CHAR(1)  not null,
    CHARGED_AMN         DECIMAL(15, 2),
    DISCOUNTED_AMN      DECIMAL(15, 2),
    ACCOUNT_NUMBER      CHAR(40),
    PRFT_SYSTEM         SMALLINT,
    REQUEST_SN          SMALLINT,
    REQUEST_TYPE        CHAR(1),
    REQUEST_LOAN_STS    CHAR(1),
    CHARGES_CURR_ID     INTEGER  not null,
    CUST_ID             INTEGER,
    UNEARNED_COMMISSION SMALLINT default 0,
    TMSTAMP_ARTICLE     TIMESTAMP(6),
    ARTICLE_SN          DECIMAL(15),
    constraint CH_RECORDING_PK
        primary key (CHARGES_CURR_ID, DB_CR_FLG, CHARGE_CODE, CHARGE_TYPE, GRP_SUBSCRIPT, TRX_USR_SN, TRX_USER,
                     TRX_UNIT, TRX_DATE)
);

create unique index IX_CHARG_REC
    on CHARGES_RECORDING (TRX_DATE, TRX_UNIT, TRX_USER, TRX_USR_SN, GRP_SUBSCRIPT);

