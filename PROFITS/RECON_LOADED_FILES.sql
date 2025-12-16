create table RECON_LOADED_FILES
(
    FK_TYPE         INTEGER            not null,
    FK_SOURCE       INTEGER            not null,
    UNIQUE_ID       BIGINT             not null
        constraint PK_RECON_LOADED_FILES
            primary key,
    FILE_NAME       VARCHAR(500),
    ENTRY_STATUS    SMALLINT default 0 not null,
    LOADED_TIMESTMP TIMESTAMP(6)       not null,
    TOTAL_RECORDS   INTEGER,
    FILE_HASH_VALUE VARCHAR(88),
    LOAD_USERNAME   VARCHAR(100)
);

create unique index INX_FK_TYPE_FILE_HASH_VALUE
    on RECON_LOADED_FILES (FK_TYPE, FILE_HASH_VALUE);

