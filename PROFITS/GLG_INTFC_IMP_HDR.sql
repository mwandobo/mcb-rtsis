create table GLG_INTFC_IMP_HDR
(
    TRX_DATE      DATE     not null,
    SUBSYSTEM     CHAR(2)  not null,
    FILE_SN       SMALLINT not null,
    TRNSF_TOT_REC INTEGER,
    DET_TOT_REC   INTEGER,
    DET_TOT_AM4   DECIMAL(15, 2),
    DET_TOT_AM3   DECIMAL(15, 2),
    DET_TOT_AM2   DECIMAL(15, 2),
    DET_TOT_AM5   DECIMAL(15, 2),
    DET_TOT_AM6   DECIMAL(15, 2),
    DET_TOT_AM1   DECIMAL(15, 2),
    TRNSF_DATE    DATE,
    TIMESTMP      TIMESTAMP(6),
    IMP_DATE      DATE,
    ENTRY_STATUS  CHAR(1),
    FILENAME      VARCHAR(20),
    constraint IXU_GL_038
        primary key (TRX_DATE, SUBSYSTEM, FILE_SN)
);

