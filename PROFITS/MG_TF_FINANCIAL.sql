create table MG_TF_FINANCIAL
(
    FILE_NAME         CHAR(50)    not null,
    SERIAL_NO         DECIMAL(10) not null,
    MIGR_STATUS       CHAR(1),
    MIGR_ERROR_DESC   CHAR(80),
    MIGR_TIMESTAMP    TIMESTAMP(6),
    LEGACY_SYSTEM     CHAR(40),
    REQUEST_SN        SMALLINT,
    STATUS            CHAR(1),
    CREATION_DATE     DATE,
    EXPIRATION_DATE   DATE,
    COMMISSIONS       DECIMAL(15, 2),
    EXPENSES          DECIMAL(15, 2),
    INTEREST_ACCRUALS DECIMAL(15, 2),
    PENALTY_ACCRUALS  DECIMAL(15, 2),
    TFLC_ACC          CHAR(40),
    constraint PK_MG_TF_FINANCIAL
        primary key (SERIAL_NO, FILE_NAME)
);

create unique index IXN_MIG_TF1
    on MG_TF_FINANCIAL (LEGACY_SYSTEM);

