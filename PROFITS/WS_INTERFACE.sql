create table WS_INTERFACE
(
    WS_CODE       VARCHAR(20)  not null,
    WS_CLASSNAME  VARCHAR(254) not null,
    INTERNAL_SN   INTEGER      not null,
    IMP_EXP       CHAR(1),
    FIELD_TYPE    CHAR(2),
    FIELD_LABEL   VARCHAR(50),
    DISPLAY_LABEL VARCHAR(50),
    ENTRY_STATUS  CHAR(1),
    MANDATORY     CHAR(1),
    PRINT_FLAG    CHAR(1),
    PRINT_LABEL   VARCHAR(20),
    constraint PK_WS_INTERFACE
        primary key (WS_CODE, WS_CLASSNAME, INTERNAL_SN)
);

