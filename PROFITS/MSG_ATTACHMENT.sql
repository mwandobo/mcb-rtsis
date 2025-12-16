create table MSG_ATTACHMENT
(
    ID                  DECIMAL(12)        not null
        constraint IXM_ATT_001
            primary key,
    ATTACHMENT_FILENAME VARCHAR(2000)      not null,
    DESCRIPTION         VARCHAR(2000),
    LAST_UPDATED        TIMESTAMP(6),
    TYPE                SMALLINT default 0 not null,
    FILE_TYPE           SMALLINT default 0 not null,
    ATTACHMENT          BLOB(104857600)
);

