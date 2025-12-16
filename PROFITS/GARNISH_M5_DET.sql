create table GARNISH_M5_DET
(
    TD_CREATION_TMSTAMP TIMESTAMP(6) not null,
    TD_BATCH_REFERENCE  INTEGER      not null,
    TD_SERIAL_NUMBER    INTEGER      not null,
    INTERNAL_SN         INTEGER      not null,
    ACCOUNT_NUMBER      CHAR(40)     not null,
    PRFT_SYSTEM         SMALLINT,
    DEP_ACC_NUMBER      DECIMAL(11),
    TRX_UNIT            INTEGER      not null,
    TRX_DATE            DATE         not null,
    TRX_USR             CHAR(8)      not null,
    TRX_USR_SN          INTEGER      not null,
    AMOUNT              DECIMAL(15, 2),
    ID_JUSTIFIC         INTEGER,
    TIMESTMP            TIMESTAMP(6),
    constraint PK_GARNISH_M5_DET
        primary key (INTERNAL_SN, TD_SERIAL_NUMBER, TD_BATCH_REFERENCE, TD_CREATION_TMSTAMP)
);

