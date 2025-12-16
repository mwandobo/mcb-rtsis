create table ITF_TRX_DETAIL
(
    ORGANISATION_CODE CHAR(10)    not null,
    RECORD_TYPE       CHAR(2)     not null,
    EXPIRY_DATE       DATE        not null,
    INTERNAL_SN       DECIMAL(13) not null,
    BANK_ID           INTEGER,
    TRX_DATE          DATE,
    DTL_STATUS        CHAR(2),
    AMOUNT            DECIMAL(15, 2),
    TRX_AMOUNT        DECIMAL(15, 2),
    KEY_FIELD_1       CHAR(30),
    KEY_FIELD_2       CHAR(30),
    KEY_FIELD_3       CHAR(30),
    KEY_FIELD_4       CHAR(30),
    ACCOUNT_NUMBER    CHAR(40),
    TIMESTMP          TIMESTAMP(6),
    ERROR_DESCRIPTION CHAR(40),
    ENTRY_COMMENTS    CHAR(40),
    INPUT_RECORD      CHAR(100),
    REPLY_RECORD      CHAR(100),
    constraint IXU_CP__054
        primary key (EXPIRY_DATE, RECORD_TYPE, ORGANISATION_CODE, INTERNAL_SN)
);

