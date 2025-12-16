create table DOC_SYSTEMS
(
    SYSTEM_ID          CHAR(4) not null
        constraint IXU_DOC_004
            primary key,
    ACTIVE_FLAG        CHAR(1),
    SYSTEM_DESCRIPTION CHAR(50)
);

