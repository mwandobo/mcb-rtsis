create table SPEC_AGR_SWEEPS
(
    CATEGORY_CODE       CHAR(8) not null,
    FK_GH_TYPE          CHAR(5) not null,
    FK_GH_SN            INTEGER not null,
    DESTINATION_PRODUCT INTEGER not null,
    ID_CURRENCY         INTEGER not null,
    SOURCE_PRODUCT      INTEGER not null,
    SN                  INTEGER not null,
    CREATE_UNIT         INTEGER,
    CREATE_USER         CHAR(8),
    CREATE_DATE         DATE,
    CREATE_TMSTAMP      TIMESTAMP(6),
    UPDATE_UNIT         INTEGER,
    UPDATE_USER         CHAR(8),
    UPDATE_DATE         DATE,
    UPDATE_TMSTAMP      TIMESTAMP(6),
    ENTRY_COMMENTS      VARCHAR(80),
    ENTRY_STATUS        CHAR(1),
    constraint PK_SPEC_SWEEP
        primary key (SOURCE_PRODUCT, ID_CURRENCY, DESTINATION_PRODUCT, FK_GH_SN, FK_GH_TYPE, CATEGORY_CODE)
);

