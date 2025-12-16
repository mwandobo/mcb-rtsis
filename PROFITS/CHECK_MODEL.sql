create table CHECK_MODEL
(
    MODELCODE    CHAR(10)     not null
        constraint I0000310
            primary key,
    MODELNAME    CHAR(40)     not null,
    MODELID      DECIMAL(10)  not null,
    MODELPREFIX  CHAR(3)      not null,
    LAST_CHECKED TIMESTAMP(6) not null,
    LOCK0        CHAR(1)      not null,
    LOCALNAME    CHAR(20),
    FK_SYSTEMSID SMALLINT
);

create unique index I0000313
    on CHECK_MODEL (FK_SYSTEMSID);

