create table MSG_IMPORTANCE_LVL
(
    ID          SMALLINT    not null
        constraint IXM_IMP_001
            primary key,
    LABEL       VARCHAR(20) not null,
    DESCRIPTION VARCHAR(255)
);

