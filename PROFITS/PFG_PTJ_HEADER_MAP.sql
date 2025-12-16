create table PFG_PTJ_HEADER_MAP
(
    TAG_SET_CODE    CHAR(20) not null,
    ID_PRODUCT      INTEGER  not null,
    ID_TRANSACT     INTEGER  not null,
    ID_JUSTIFIC     INTEGER  not null,
    HEADER_ENTRY_SN DECIMAL(10),
    DESCRIPTION     VARCHAR(500),
    constraint PK_PFGPTJ
        primary key (ID_JUSTIFIC, ID_TRANSACT, ID_PRODUCT, TAG_SET_CODE)
);

