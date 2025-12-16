create table VAT_MASTER
(
    ID_VAT             DECIMAL(5) not null
        constraint VAT_MASTER_PK
            primary key,
    DESCRIPTION        VARCHAR(40),
    SHORT_DESCR        CHAR(5),
    FK_CURRENCYID_CURR DECIMAL(5),
    FKGH_HAS_COUNTRY   CHAR(5),
    FKGD_HAS_COUNTRY   DECIMAL(5),
    ENTRY_STATUS       CHAR(1),
    VAT_TYPE           DECIMAL(5) not null,
    FK_GL_ACCOUNT      CHAR(21),
    COMMENTS           VARCHAR(100)
);

