create table FIELD
(
    ID             DECIMAL(10) not null
        constraint ID5
            primary key,
    NAME           CHAR(32)    not null,
    SEQ            DECIMAL(10) not null,
    FORMAT         CHAR(1)     not null,
    LENGTH         INTEGER     not null,
    DEC_PLACES     INTEGER     not null,
    OPT            CHAR(1)     not null,
    FK_ATTRIBUTEID DECIMAL(10),
    FK_MODELID     DECIMAL(10)
);

create unique index I0000528
    on FIELD (FK_ATTRIBUTEID);

create unique index I0000556
    on FIELD (FK_MODELID);

