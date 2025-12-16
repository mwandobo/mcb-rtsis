create table MSG_LANGUAGE
(
    ID          SMALLINT    not null
        constraint IXM_LNG_001
            primary key,
    LABEL       VARCHAR(20) not null,
    DESCRIPTION VARCHAR(255)
);

