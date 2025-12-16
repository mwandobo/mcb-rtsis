create table HSDB_TYPE
(
    FK_HPRODUCTID_PROD INTEGER        not null,
    FK_HPRODUCTVALIDIT DATE           not null,
    RENTAL             DECIMAL(15, 2) not null,
    RENEWAL_FREQUENCY  SMALLINT       not null,
    TMSTAMP            TIMESTAMP(6),
    FK_CURRENCYID_CURR INTEGER,
    GL_ACCOUNT_CR      CHAR(21),
    GL_ACCOUNT_DB      CHAR(21),
    constraint IXU_SDB_002
        primary key (FK_HPRODUCTID_PROD, FK_HPRODUCTVALIDIT)
);

