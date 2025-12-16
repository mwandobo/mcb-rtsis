create table FX_FT_SWT_PARAM
(
    ID_PRODUCT     INTEGER     not null,
    ID_TRANSACT    INTEGER     not null,
    ID_JUSTIFIC    INTEGER     not null,
    COMMAND_TYPE   CHAR(8)     not null,
    PARAMETER_TYPE CHAR(8)     not null,
    SERIAL_NUM     DECIMAL(10) not null,
    VALUE_CHAR1    VARCHAR(40),
    VALUE_CHAR2    VARCHAR(40),
    VALUE_CHAR3    VARCHAR(40),
    VALUE_NUM1     DECIMAL(18, 4),
    VALUE_NUM2     DECIMAL(18, 4),
    VALUE_NUM3     DECIMAL(18, 4),
    DESCRIPTION    VARCHAR(100),
    constraint PK_FX_FT_GP
        primary key (COMMAND_TYPE, ID_JUSTIFIC, ID_TRANSACT, ID_PRODUCT, PARAMETER_TYPE, SERIAL_NUM)
);

