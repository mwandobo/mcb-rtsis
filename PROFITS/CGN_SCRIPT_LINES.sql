create table CGN_SCRIPT_LINES
(
    SCRIPT_SN        DECIMAL(10) not null,
    SCRIPT_LN_SN     SMALLINT    not null,
    SCRIPT_LINE      VARCHAR(100),
    SCRIPT_LINE_OPER CHAR(3),
    FIELD_SN         DECIMAL(10) not null,
    constraint PK_SQL_SCRIPT_LN
        primary key (SCRIPT_LN_SN, SCRIPT_SN)
);

