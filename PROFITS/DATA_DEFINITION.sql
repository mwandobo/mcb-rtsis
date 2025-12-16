create table DATA_DEFINITION
(
    VARIABLE_NAME      CHAR(16) not null,
    VARIABLE_POSITION  SMALLINT not null,
    VARIABLE_TYPE      CHAR(1)  not null,
    MAX_NUM_OF_CHAR    SMALLINT not null,
    NUMBER_AFTER_DECIM SMALLINT not null,
    THOUSAND_SEPARATOR CHAR(1)  not null,
    OUTPUT_FORMAT      CHAR(10) not null,
    FK_RPT_REPORTREPOR CHAR(15) not null,
    constraint PKDATA
        primary key (FK_RPT_REPORTREPOR, VARIABLE_NAME)
);

