create table PARAMETER_TYPE
(
    PARAMETER_TYPE   CHAR(5) not null
        constraint PIX_GENERIC_HEADER
            primary key,
    SYSTEM_PARAMETER CHAR(1),
    ABBREVIATION     VARCHAR(15),
    DESCRIPTION      VARCHAR(40),
    DEFAULT_NUM      INTEGER,
    ENTRY_STATUS     CHAR(1),
    TMSTAMP          TIMESTAMP(6)
);

