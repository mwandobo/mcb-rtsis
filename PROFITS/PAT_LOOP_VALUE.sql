create table PAT_LOOP_VALUE
(
    ID                INTEGER  not null
        constraint PATLVPK1
            primary key,
    NAME              CHAR(32),
    STATIC_DATA       CHAR(100),
    INDEX1            SMALLINT not null,
    FK_PAT_RUN_SCENID INTEGER,
    FK_PAT_USER_PROID CHAR(10)
);

create unique index PATLVI1
    on PAT_LOOP_VALUE (FK_PAT_USER_PROID);

create unique index PATLVI2
    on PAT_LOOP_VALUE (FK_PAT_RUN_SCENID);

