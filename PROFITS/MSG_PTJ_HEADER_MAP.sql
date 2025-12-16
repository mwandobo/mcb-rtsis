create table MSG_PTJ_HEADER_MAP
(
    HEADER_ENTRY_SN DECIMAL(10) not null,
    ID_JUSTIFIC     INTEGER     not null,
    ID_TRANSACT     INTEGER     not null,
    ID_PRODUCT      INTEGER     not null,
    MSG_CATEGORY    CHAR(1)     not null,
    MESSAGE_TYPE    CHAR(20)    not null,
    RUN_ORDER       SMALLINT,
    DESCRIPTION     VARCHAR(500),
    constraint IXU_CIS_184
        primary key (HEADER_ENTRY_SN, ID_JUSTIFIC, ID_TRANSACT, ID_PRODUCT, MSG_CATEGORY, MESSAGE_TYPE)
);

