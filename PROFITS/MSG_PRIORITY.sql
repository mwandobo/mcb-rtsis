create table MSG_PRIORITY
(
    ID          SMALLINT    not null
        constraint IXM_PRI_001
            primary key,
    LABEL       VARCHAR(20) not null,
    DESCRIPTION VARCHAR(255)
);

