create table MG_TF_SETTL
(
    FILE_NAME           CHAR(50)    not null,
    SERIAL_NO           DECIMAL(10) not null,
    MIGR_STATUS         CHAR(1),
    MIGR_ERROR_DESC     CHAR(80),
    MIGR_TIMESTAMP      TIMESTAMP(6),
    LEGACY_SYSTEM       CHAR(40),
    SETTLEMENT_SN       SMALLINT,
    SETTLEMENT_DATE     DATE,
    SETTLEMENT_STATUS   CHAR(1),
    SETTLEMENT_CURRENCY CHAR(5),
    PAYMENT_ACCOUNT     CHAR(40),
    PAYMENT_DATE        DATE,
    PAYMENT_AMOUNT      DECIMAL(15, 2),
    COMMENTS            CHAR(70),
    TFLC_ACC            CHAR(40),
    constraint PK_MG_TF_SETTL
        primary key (SERIAL_NO, FILE_NAME)
);

create unique index IXN_MIG_TF2
    on MG_TF_SETTL (LEGACY_SYSTEM);

