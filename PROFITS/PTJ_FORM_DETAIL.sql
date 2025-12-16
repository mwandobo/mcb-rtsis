create table PTJ_FORM_DETAIL
(
    PTJ_DTL_LINK_ID    DECIMAL(10) not null,
    FK_PTJ_FORM_HEADER DECIMAL(10) not null,
    FK_FORM_CODE       CHAR(20)    not null,
    PRODUCT_ID         INTEGER,
    PRODUCT_ALL        CHAR(1),
    TRANSACT_ID        INTEGER,
    TRANSACT_ALL       CHAR(1),
    JUSTIFIC_ID        INTEGER,
    JUSTIFIC_ALL       CHAR(1),
    CREATE_USER        CHAR(8),
    CREATE_UNIT        INTEGER,
    CREATE_DATE        DATE,
    CREATE_TMSTAMP     TIMESTAMP(6),
    UPDATE_USER        CHAR(8),
    UPDATE_UNIT        INTEGER,
    UPDATE_DATE        DATE,
    UPDATE_TMSTAMP     TIMESTAMP(6),
    ENTRY_STATUS       CHAR(1),
    FK_GH_GROUP        CHAR(5),
    FK_GD_GROUP        INTEGER,
    constraint PK_PTJ_PARAM_2
        primary key (FK_FORM_CODE, FK_PTJ_FORM_HEADER, PTJ_DTL_LINK_ID)
);

