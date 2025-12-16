create table CLC_COVER_DYNAMIC
(
    TAG_SET_CODE     CHAR(20) not null
        constraint CLC_COLLECT_PK_80
            primary key,
    AMN_PFG_TAG      CHAR(10),
    AMN_PFG_SET_SN   INTEGER,
    CURR_PFG_TAG     CHAR(10),
    CURR_PFG_SET_SN  INTEGER,
    CHECK_ADDITIONAL CHAR(1)
);

