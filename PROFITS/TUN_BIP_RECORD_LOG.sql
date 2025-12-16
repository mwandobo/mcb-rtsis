create table TUN_BIP_RECORD_LOG
(
    TRX_UNIT              INTEGER  not null,
    TRX_DATE              DATE     not null,
    TRX_USR               CHAR(8)  not null,
    TRX_USR_SN            INTEGER  not null,
    TUN_INTERNAL_SN       SMALLINT not null,
    GRP_SN                INTEGER  not null,
    ERROR_MESSAGE         CHAR(80),
    CREDIT_ACCOUNT_NUMBER CHAR(40),
    LOAN_ACCOUNT          CHAR(40),
    INTERNAL_USER_SN      INTEGER,
    TMSTAMP               TIMESTAMP(6),
    constraint PK_TUN_LOG
        primary key (GRP_SN, TUN_INTERNAL_SN, TRX_USR_SN, TRX_USR, TRX_DATE, TRX_UNIT)
);

