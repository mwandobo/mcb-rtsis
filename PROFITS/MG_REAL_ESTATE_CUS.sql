create table MG_REAL_ESTATE_CUS
(
    FILE_NAME      CHAR(50) not null,
    SERIAL_NO      INTEGER  not null,
    FILE_DETAIL_ID CHAR(2)  not null,
    REAL_ESTATE_ID CHAR(40) not null,
    CUSTOMER_CODE  CHAR(20) not null,
    PARAM_EIKYR    CHAR(30),
    CONTRACT_ID    CHAR(80),
    OWNERSH_PERC   DECIMAL(8, 4),
    ROW_STATUS     CHAR(1),
    constraint PK_MG_RE_CUS
        primary key (SERIAL_NO, FILE_NAME)
);

