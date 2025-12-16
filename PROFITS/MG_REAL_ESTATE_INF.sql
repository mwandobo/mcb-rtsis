create table MG_REAL_ESTATE_INF
(
    FILE_NAME       CHAR(50) not null,
    SERIAL_NO       INTEGER  not null,
    FILE_DETAIL_ID  CHAR(2)  not null,
    REAL_ESTATE_ID  CHAR(40) not null,
    PARAMETER_TYPE  CHAR(5)  not null,
    PARAMETER_VALUE CHAR(30) not null,
    ROW_STATUS      CHAR(1),
    constraint MG_RE_INF
        primary key (FILE_NAME, SERIAL_NO)
);

