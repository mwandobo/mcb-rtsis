create table TEMP_GEN_DETAIL
(
    CODE           CHAR(10) not null
        constraint PTXTGDET
            primary key,
    DESCRIPTION    CHAR(40),
    PARAMETER_TYPE CHAR(5)  not null,
    STATUS         CHAR(1)  not null
);

