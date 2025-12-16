create table RPT_DATABASE
(
    ID                INTEGER      not null
        constraint RPT_DATABASE_PK
            primary key,
    NAME              VARCHAR(50)  not null,
    TYPE              SMALLINT     not null,
    CONNECTION_STRING VARCHAR(400) not null
);

