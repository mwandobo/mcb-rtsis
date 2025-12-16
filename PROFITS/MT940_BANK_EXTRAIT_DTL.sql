create table MT940_BANK_EXTRAIT_DTL
(
    SN                 DECIMAL(10) not null,
    REC_DATE           DATE        not null,
    CRED_DEB_FLG       CHAR(1),
    AMOUNT             DECIMAL(15, 2),
    REFERENCE_NO       CHAR(16),
    ACC_OWNER_REF      CHAR(50),
    ENTRY_STATUS       CHAR(1),
    INFO_TO_ACC_OWNER  CHAR(65),
    FK_COLLAB_BANK     INTEGER     not null,
    FK_STATEMENT_NUM   CHAR(11)    not null,
    FK_MT940_YEAR      SMALLINT    not null,
    FK_CURRENCYID_CURR INTEGER,
    constraint IXU_MT940_001
        primary key (FK_COLLAB_BANK, FK_MT940_YEAR, FK_STATEMENT_NUM, SN)
);

