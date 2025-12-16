create table RSKCO_VIRTUALDATA
(
    TABLE_NAME   CHAR(40) not null,
    FIELD_NAME   CHAR(50) not null,
    DATE_CREATED DATE,
    FIELD_TYPE   CHAR(1),
    REF_KEY      CHAR(40),
    FIELD_VALUE  CHAR(50),
    constraint IXU_LNS_047
        primary key (TABLE_NAME, FIELD_NAME)
);

