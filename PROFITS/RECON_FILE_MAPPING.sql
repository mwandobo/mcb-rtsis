create table RECON_FILE_MAPPING
(
    SOURCE_TYPE            INTEGER            not null,
    SOURCE_MAPPING_SN      SMALLINT           not null,
    DESTINATION_TYPE       INTEGER            not null,
    DESTINATION_MAPPING_SN SMALLINT           not null,
    MAP_UNIQUE             SMALLINT default 1 not null,
    MAP_ALGORITHM          VARCHAR(500),
    MAP_DESCRIPTION        VARCHAR(300),
    MIN_OFFSET             VARCHAR(50),
    MAX_OFFSET             VARCHAR(50),
    MAP_FORMULA            SMALLINT default 0,
    constraint PK_RECON_110
        primary key (SOURCE_TYPE, SOURCE_MAPPING_SN, DESTINATION_TYPE, DESTINATION_MAPPING_SN)
);

