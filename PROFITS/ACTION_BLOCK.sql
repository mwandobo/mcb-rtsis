create table ACTION_BLOCK
(
    ID         DECIMAL(10) not null
        constraint IDB0
            primary key,
    NAME       CHAR(32)    not null,
    INTEXT     CHAR(1)     not null,
    FK_MODELID DECIMAL(10)
);

create unique index I0000568
    on ACTION_BLOCK (FK_MODELID);

