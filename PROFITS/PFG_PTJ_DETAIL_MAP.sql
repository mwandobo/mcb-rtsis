create table PFG_PTJ_DETAIL_MAP
(
    TAG_SET_CODE    CHAR(20)    not null,
    ID_PRODUCT      INTEGER     not null,
    ID_TRANSACT     INTEGER     not null,
    ID_JUSTIFIC     INTEGER     not null,
    INTERNAL_SN     DECIMAL(10) not null,
    HEADER_ENTRY_SN DECIMAL(10),
    TABLE_ENTITY    CHAR(40),
    TABLE_ATTRIBUTE CHAR(40),
    FIELD_TYPE      CHAR(2),
    TAG             CHAR(10),
    SET_SN          INTEGER,
    SET_CATEGORY    CHAR(1),
    SUBTAG_SN       SMALLINT,
    DEFAULT_VALUE   CHAR(40),
    DESCRIPTION     VARCHAR(500),
    constraint PK_PTJ_DETAIL_MAP
        primary key (INTERNAL_SN, ID_JUSTIFIC, ID_TRANSACT, ID_PRODUCT, TAG_SET_CODE)
);

