create table COLLATERAL_FIELDS
(
    COLL_INTERNAL_SN DECIMAL(10) not null,
    COLL_RECORD_TYPE CHAR(2)     not null,
    PFG_TAG_SET_CODE CHAR(20)    not null,
    PFG_TAG          CHAR(10)    not null,
    PFG_SET_SN       INTEGER     not null,
    PFG_SET_CATEGORY CHAR(1)     not null,
    SHOW_ORDER       INTEGER,
    FIELD_LABEL      CHAR(40),
    FIELD_VALUE      VARCHAR(100),
    constraint PK_COLLFLD
        primary key (PFG_SET_CATEGORY, PFG_SET_SN, PFG_TAG, PFG_TAG_SET_CODE, COLL_RECORD_TYPE, COLL_INTERNAL_SN)
);

create unique index IXN_COL_001
    on COLLATERAL_FIELDS (FIELD_VALUE);

