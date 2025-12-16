create table CSC_APPLICATION
(
    SC_YEAR            SMALLINT not null,
    APP_SN             INTEGER  not null,
    FKGH_HAS_AS_LDEPT  CHAR(5)  not null,
    FKGD_HAS_AS_LDEPT  INTEGER  not null,
    FK_UNITCODE        INTEGER  not null,
    APPLICATION_STATUS CHAR(1)  not null,
    constraint I0000900
        primary key (FKGH_HAS_AS_LDEPT, FKGD_HAS_AS_LDEPT, FK_UNITCODE, APP_SN, SC_YEAR)
);

