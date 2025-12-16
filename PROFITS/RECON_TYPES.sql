create table RECON_TYPES
(
    SN             INTEGER            not null
        constraint PK_RECON_1
            primary key,
    RECON_DESCR    VARCHAR(80),
    RECON_ANALYSIS VARCHAR(4000),
    FILE_TYPE      SMALLINT default 0 not null,
    START_ROW      INTEGER  default 1,
    START_COLUMN   INTEGER  default 1,
    COLUMN_COUNT   INTEGER  default 0,
    DELIMITER      VARCHAR(10),
    FK_SOURCE_SN   INTEGER            not null,
    HASH_WINDOW    INTEGER  default 60
);

