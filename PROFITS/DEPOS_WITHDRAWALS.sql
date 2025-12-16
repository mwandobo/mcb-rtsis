create table DEPOS_WITHDRAWALS
(
    ACCOUNT_NUMBER     DECIMAL(11) not null,
    CR_TRX_UNIT        INTEGER     not null,
    CR_TRX_DATE        DATE        not null,
    CR_TRX_USR         CHAR(8)     not null,
    CR_TRX_USR_SN      INTEGER     not null,
    CR_TUN_INTERNAL_SN SMALLINT    not null,
    DB_TRX_UNIT        INTEGER     not null,
    DB_TRX_DATE        DATE        not null,
    DB_TRX_USR         CHAR(8)     not null,
    DB_TRX_USR_SN      INTEGER     not null,
    DB_TUN_INTERNAL_SN SMALLINT    not null,
    TRANSACTION_STATUS CHAR(1),
    PART_AMOUNT        DECIMAL(15, 2),
    TMSTAMP            TIMESTAMP(6),
    constraint PK_DEPOS_WITHDRAWALS
        primary key (ACCOUNT_NUMBER, CR_TRX_UNIT, CR_TRX_DATE, CR_TRX_USR, CR_TRX_USR_SN, DB_TUN_INTERNAL_SN,
                     DB_TRX_UNIT, DB_TRX_DATE, DB_TRX_USR, DB_TRX_USR_SN, CR_TUN_INTERNAL_SN)
);

