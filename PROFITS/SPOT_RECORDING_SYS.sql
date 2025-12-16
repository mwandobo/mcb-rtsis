create table SPOT_RECORDING_SYS
(
    TRANSACTION_REFERE CHAR(15) not null
        constraint PK_SPOT_RECORD_SYST
            primary key,
    TRX_CODE           INTEGER  not null,
    ENTRY_TYPE         CHAR(1)  not null,
    VALUE_DATE         DATE,
    CORRESPONDENT_BANK INTEGER,
    OWNER              CHAR(1),
    DOMICILE           CHAR(1)  not null,
    CBM_CODE           CHAR(5)  not null,
    REMARKS            VARCHAR(40),
    SRS_CLASS          CHAR(1)  not null
);

