create table RECON_LOADING_FILE_MAPPING
(
    FK_RECON_TYPE_SN        INTEGER             not null,
    SN                      SMALLINT            not null,
    FILE_COLUMN             SMALLINT,
    DB_COLUMN               VARCHAR(40)         not null,
    ROW_TYPE                SMALLINT default 1  not null,
    DATA_TYPE               SMALLINT            not null,
    DATA_FORMAT             VARCHAR(200),
    FIELD_DESCRIPTION       VARCHAR(100),
    ORDER_BY                SMALLINT,
    DECIMAL_PLACES          SMALLINT default -1 not null,
    OUTPUT_LENGTH           INTEGER  default 0  not null,
    LEFT_PADDING_CHARACTER  CHAR(1)  default '' not null,
    INPUT_DECIMAL_PLACES    SMALLINT,
    INPUT_DECIMAL_SEPARATOR CHAR(1),
    STARTING_POSITION       INTEGER  default 1,
    TRANSFORMATION_VALUES   CLOB(1048576),
    constraint RECON_LOADING_FILE_MAPPING_PK
        primary key (SN, FK_RECON_TYPE_SN)
);

