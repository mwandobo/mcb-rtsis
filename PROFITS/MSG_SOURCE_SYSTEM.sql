create table MSG_SOURCE_SYSTEM
(
    ID          INTEGER     not null
        constraint IXM_MSS_001
            primary key,
    LABEL       VARCHAR(20) not null,
    DESCRIPTION VARCHAR(255)
);

