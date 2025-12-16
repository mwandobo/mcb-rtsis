create table RECON_GENERIC_TABLE_HISTORY
(
    ENTRY_TIMESTMP        TIMESTAMP(6)       not null,
    USER_NAME             VARCHAR(100),
    FK_TYPE               INTEGER            not null,
    FK_RECON_LOADED_FILES BIGINT             not null,
    ID                    BIGINT             not null,
    ENTRY_STATUS          SMALLINT default 0 not null,
    LAST_PROCESS_DATE     TIMESTAMP(6),
    COMMENTS              VARCHAR(400),
    constraint PK_RECON_GENERIC_TABLE_HISTORY
        primary key (FK_RECON_LOADED_FILES, ID, ENTRY_TIMESTMP)
);

