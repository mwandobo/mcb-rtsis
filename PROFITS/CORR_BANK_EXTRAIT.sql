create table CORR_BANK_EXTRAIT
(
    SN                 DECIMAL(10)    not null,
    VALUE_DATE         DATE,
    AVAILABILITY_DATE  DATE,
    CRED_DEB_FLG       CHAR(1),
    AMOUNT             DECIMAL(15, 2) not null,
    REFERENCE_NO       CHAR(16),
    FK_CORRESP_BANK    INTEGER        not null,
    FK_CURRENCYID_CURR INTEGER,
    ENTRY_STATUS       CHAR(1),
    SETTL_FLAG         CHAR(1),
    TRX_DATE           DATE           not null,
    FK_UNITCODE        INTEGER,
    constraint IXU_MT940_004
        primary key (FK_CORRESP_BANK, SN)
);

