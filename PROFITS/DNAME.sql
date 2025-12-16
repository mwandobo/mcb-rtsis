create table DNAME
(
    NAME_MODEL_ID INTEGER not null,
    NAME_OBJ_ID   INTEGER not null
        constraint I0000436
            primary key,
    NAME08_PROP_1 CHAR(8) not null,
    NAME08_PROP_2 CHAR(8) not null
);

create unique index I0000435
    on DNAME (NAME_MODEL_ID, NAME_OBJ_ID);

