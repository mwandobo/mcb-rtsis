create table EXCEPTION_AUTHORIZ
(
    TMSTAMP      TIMESTAMP(6) not null
        constraint PK_EXC_AUTHORIZ
            primary key,
    AUTH_DATE    DATE         not null,
    UNITCODE     INTEGER      not null,
    REQUEST_NO   INTEGER      not null,
    SUPER_1_CODE CHAR(8),
    SUPER_2_CODE CHAR(8),
    USERCODE     CHAR(8)
);

