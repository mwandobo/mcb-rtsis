create table MSG_SUBSYSTEM
(
    ID          SMALLINT    not null
        constraint IXM_SBS_001
            primary key,
    LABEL       VARCHAR(20) not null,
    DESCRIPTION VARCHAR(255)
);

