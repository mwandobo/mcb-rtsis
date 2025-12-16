create table SDB_TYPE
(
    FK_PRODUCTID_PRODU INTEGER        not null
        constraint IXU_SDB_001
            primary key,
    RENTAL             DECIMAL(15, 2) not null,
    RENEWAL_FREQUENCY  SMALLINT       not null,
    TMSTAMP            TIMESTAMP(6),
    FK_CURRENCYID_CURR INTEGER,
    GL_ACCOUNT_CR      CHAR(21),
    GL_ACCOUNT_DB      CHAR(21)
);

