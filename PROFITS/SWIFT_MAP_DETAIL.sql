create table SWIFT_MAP_DETAIL
(
    INTERNAL_SN     DECIMAL(10) not null,
    MESSAGE_TYPE    CHAR(20)    not null,
    ID_TRANSACT     INTEGER     not null,
    MSG_CATEGORY    CHAR(1)     not null,
    SUBTAG_SN       SMALLINT,
    FIELD_TYPE      CHAR(2),
    TAG             CHAR(10),
    DEFAULT_VALUE   CHAR(40),
    TABLE_ENTITY    CHAR(40),
    TABLE_ATTRIBUTE CHAR(40),
    constraint IXU_FX_028
        primary key (INTERNAL_SN, MESSAGE_TYPE, ID_TRANSACT, MSG_CATEGORY)
);

