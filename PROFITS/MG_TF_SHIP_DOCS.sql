create table MG_TF_SHIP_DOCS
(
    FILE_NAME            CHAR(50)    not null,
    SERIAL_NO            DECIMAL(10) not null,
    MIGR_STATUS          CHAR(1),
    MIGR_ERROR_DESC      CHAR(80),
    MIGR_TIMESTAMP       TIMESTAMP(6),
    LEGACY_SYSTEM        CHAR(40),
    DOCUMENT_TYPE        CHAR(30),
    DOCUMENT_SN          SMALLINT,
    DOCUMENT_NUMBER      CHAR(40),
    COPY_SN              SMALLINT,
    NUMBER_OF_COPIES     SMALLINT,
    AMOUNT               DECIMAL(15, 2),
    CURRENCY             CHAR(5),
    EXPIRATION_DATE      DATE,
    DOCUMENT_STATUS      CHAR(1),
    DISCREPANCY_COMMENTS CHAR(80),
    TFLC_ACC             CHAR(40),
    constraint PK_MG_TF_SHIP_DOCS
        primary key (SERIAL_NO, FILE_NAME)
);

create unique index IXN_MIG_TF3
    on MG_TF_SHIP_DOCS (LEGACY_SYSTEM);

