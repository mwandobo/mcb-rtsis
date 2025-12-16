create table RECON_FILE_FIELDS
(
    FIELD_SN           SMALLINT    not null,
    TABLE_ENTITY       VARCHAR(40) not null,
    TABLE_ATTRIBUTE    VARCHAR(40) not null,
    FIELD_DESCRIPTION  VARCHAR(100),
    FK_RECON_SOURCE_SN INTEGER     not null,
    FK_RECON_TYPES_SN  INTEGER     not null,
    constraint PK_RECON_FILE_FIELDS
        primary key (FK_RECON_TYPES_SN, FIELD_SN)
);

