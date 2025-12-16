create table DFM_STRUCT
(
    ID              INTEGER  not null
        constraint IXU_DFM_STRUCT_001
            primary key,
    DESCRIPTION     CHAR(40),
    FIELD_TYPE      CHAR(1),
    FIELD_LENGTH    INTEGER,
    DEC_PLACES      SMALLINT,
    DYNAMIC_IND     CHAR(1),
    TABLE_DESCR     CHAR(20) not null,
    ATTRIBUTE_DESCR CHAR(20) not null,
    STRUCT_ALIAS    CHAR(20)
);

