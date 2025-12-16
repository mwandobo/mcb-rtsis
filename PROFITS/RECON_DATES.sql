create table RECON_DATES
(
    PROCESS_SN       BIGINT       not null
        constraint RECON_DATES_PK
            primary key,
    PROCESS_TIMESTMP TIMESTAMP(6) not null,
    PROCESS_COMMENTS VARCHAR(500),
    SOURCE_TYPE      INTEGER      not null,
    SOURCE_FILE      BIGINT       not null,
    SOURCE_ROW       INTEGER      not null,
    DESTINATION_TYPE INTEGER      not null,
    DESTINATION_FILE BIGINT       not null,
    DESTINATION_ROW  INTEGER      not null,
    MAP_UNIQUE       SMALLINT     not null
);

