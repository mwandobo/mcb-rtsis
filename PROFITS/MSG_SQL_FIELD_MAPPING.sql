create table MSG_SQL_FIELD_MAPPING
(
    ID               SMALLINT     not null,
    FK_SQL_REP_ID    DECIMAL(12)  not null
        constraint FK_SQLRP1
            references MSG_SQL_REPOSITORY,
    COLUMN_NUM       SMALLINT     not null,
    TABLE_NAME       VARCHAR(100) not null,
    FIELD_NAME       VARCHAR(100) not null,
    FIELD_TYPE       SMALLINT     not null,
    LABEL            VARCHAR(100) not null,
    DEFAULT_VALUE    VARCHAR(255),
    WHERE_CLAUSE_FLG SMALLINT default 0,
    FILE_FLG         SMALLINT default 0,
    constraint IXM_SFM_001
        primary key (ID, FK_SQL_REP_ID)
);

