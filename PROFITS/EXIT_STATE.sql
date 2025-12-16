create table EXIT_STATE
(
    ID              DECIMAL(10) not null
        constraint ID4
            primary key,
    NAME            CHAR(32)    not null,
    TERMINAT_ACTION CHAR(1)     not null,
    TYPE0           CHAR(1)     not null,
    FK_MODELID      DECIMAL(10)
);

create unique index I0000557
    on EXIT_STATE (FK_MODELID);

