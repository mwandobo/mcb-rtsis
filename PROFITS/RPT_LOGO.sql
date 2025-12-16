create table RPT_LOGO
(
    ID          SMALLINT           not null
        constraint RPT_LOGO_PK
            primary key,
    FK_FILE_ID  INTEGER            not null,
    DESCRIPTION VARCHAR(200),
    STATUS      SMALLINT default 0 not null,
    CREATED     TIMESTAMP(6)       not null,
    CREATED_BY  VARCHAR(50)        not null,
    UPDATED     TIMESTAMP(6)       not null,
    UPDATED_BY  VARCHAR(50)        not null
);

