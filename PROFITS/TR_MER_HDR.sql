create table TR_MER_HDR
(
    MER_CODE           INTEGER not null
        constraint IXU_DEP_147
            primary key,
    FK_JUSTIFICID_JUST INTEGER,
    ID_CURRENCY        INTEGER,
    MERHDR_AMOUNT      DECIMAL(15, 6),
    TMSTAMP            TIMESTAMP(6),
    PAYMENT_DATE       DATE,
    COMPLETE_DATE      DATE,
    RECORD_DATE        DATE,
    CUT_OFF_DATE       DATE,
    CREATION_DATE      DATE,
    ENTRY_STATUS       CHAR(1),
    CR_RESIDENT_FLG    CHAR(1),
    FK_TRBONDBOND_CODE CHAR(15),
    MER_DESC           CHAR(40),
    REMARKS            VARCHAR(250)
);

