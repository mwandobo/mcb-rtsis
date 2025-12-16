create table RPT_SUBSYSTEM
(
    ID          INTEGER     not null
        constraint RPT_SUBSYSTEM_PK
            primary key,
    NAME        VARCHAR(50) not null,
    DESCRIPTION VARCHAR(200)
);

