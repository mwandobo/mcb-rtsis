create table CP_TEMP_EPAYROLL
(
    CP_AGREEMENT_NO DECIMAL(10) not null,
    EXPIRY_DATE     DATE        not null,
    INTERNAL_SN     DECIMAL(13) not null,
    ACCOUNT_NO      DECIMAL(10),
    ACCOUNT_CD      SMALLINT,
    IBAN            CHAR(37),
    AMOUNT          DECIMAL(15, 2),
    ENTRY_COMMENTS  CHAR(40),
    CURRENCY_ID     INTEGER,
    ENTRY_STATUS    CHAR(1),
    DATA_FIELD_1    CHAR(30),
    DATA_FIELD_2    CHAR(30),
    DATA_FIELD_3    CHAR(30),
    DATA_FIELD_4    CHAR(30),
    TIMESTMP        TIMESTAMP(6),
    constraint IXU_TRA_020
        primary key (CP_AGREEMENT_NO, INTERNAL_SN, EXPIRY_DATE)
);

