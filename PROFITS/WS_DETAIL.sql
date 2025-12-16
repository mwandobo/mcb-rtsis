create table WS_DETAIL
(
    WS_CODE       VARCHAR(20) not null,
    INTERNAL_SN   INTEGER     not null,
    FIELD_LABEL   VARCHAR(50),
    DISPLAY_LABEL VARCHAR(50),
    MANDATORY     CHAR(1),
    ENTRY_STATUS  CHAR(1),
    PRINT_FLAG    CHAR(1)
);

create unique index PK_WS_DETAIL
    on WS_DETAIL (WS_CODE, INTERNAL_SN);

