create table DIF_FILE_FORMAT_HD
(
    FORMAT_ID          CHAR(5) not null
        constraint IXU_DFM_002
            primary key,
    FILE_TYPE          CHAR(1) not null,
    FILE_KIND          CHAR(1) not null,
    DELIMITER          CHAR(1),
    ENTRY_STATUS       CHAR(1),
    TMSTAMP            TIMESTAMP(6),
    DESCRIPTION        VARCHAR(40),
    FK_DFM_MECHANISNUM SMALLINT,
    FK_DFM_MECHANISID  INTEGER,
    PRCSS_TRX_CODE     INTEGER,
    FKGH_SWIFT_TYPE    CHAR(5),
    FKGD_SWIFT_TYPE    INTEGER
);

create unique index IND_SEC_FIL_FRM800
    on DIF_FILE_FORMAT_HD (FKGH_SWIFT_TYPE, FKGD_SWIFT_TYPE, FORMAT_ID);

