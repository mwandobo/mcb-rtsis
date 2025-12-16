create table ITF_HIST_DETAIL
(
    ORGANISATION_CODE CHAR(10)    not null,
    RECORD_TYPE       CHAR(2)     not null,
    EXPIRY_DATE       DATE        not null,
    INTERNAL_SN       DECIMAL(13) not null,
    BANK_ID           INTEGER,
    AMOUNT            DECIMAL(15, 2),
    TRX_AMOUNT        DECIMAL(15, 2),
    TRX_DATE          DATE,
    TIMESTMP          TIMESTAMP(6),
    DTL_STATUS        CHAR(2),
    KEY_FIELD_4       CHAR(30),
    KEY_FIELD_3       CHAR(30),
    KEY_FIELD_2       CHAR(30),
    KEY_FIELD_1       CHAR(30),
    ERROR_DESCRIPTION CHAR(40),
    ENTRY_COMMENTS    CHAR(40),
    ACCOUNT_NUMBER    CHAR(40),
    REPLY_RECORD      CHAR(100),
    INPUT_RECORD      CHAR(100),
    constraint IXU_CP_097
        primary key (ORGANISATION_CODE, RECORD_TYPE, EXPIRY_DATE, INTERNAL_SN)
);

