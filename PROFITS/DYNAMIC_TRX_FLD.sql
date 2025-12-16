create table DYNAMIC_TRX_FLD
(
    TABLE_ENTITY       CHAR(40) not null,
    TABLE_ATTRIBUTE    CHAR(40) not null,
    TRX_CODE           INTEGER  not null,
    LANGUAGE_USED      INTEGER,
    PRFT_SYSTEM        SMALLINT,
    DESCRIPTION        CHAR(40),
    FIELD_TYPE         CHAR(2),
    FIELD_LENGTH       INTEGER,
    VARYING_LENGTH     CHAR(1),
    DEC_PLACES         INTEGER,
    FUNCTIONALITY_DESC CHAR(240),
    constraint PK_DYNAM_TRX_FLD
        primary key (TRX_CODE, TABLE_ENTITY, TABLE_ATTRIBUTE)
);

