create table RPT_FILES
(
    ID          INTEGER               not null
        constraint RPT_FILES_PK
            primary key,
    FILE_TYPE   INTEGER               not null,
    CONTENT     BLOB(209715200)       not null,
    HASH        BIGINT                not null,
    FILE_SIZE   INTEGER  default NULL not null,
    FILE_NAME   VARCHAR(255)          not null,
    FILE_SOURCE SMALLINT default NULL not null
);

