create table AML_ACC_OWNER
(
    KD_KUNDNR        INTEGER  not null,
    VERF_TYP         CHAR(5)  not null,
    VFB_KUNDNR       INTEGER  not null,
    PRFT_SYSTEM      SMALLINT not null,
    PRFT_ACCOUNT_NUM CHAR(40) not null,
    GESCHART         INTEGER,
    GEN_DET_SER_NUM  INTEGER,
    KTONR            DECIMAL(11),
    LAST_UPDATE      DATE,
    PROCESSED_FLAG   CHAR(1),
    FILE_FLAG        CHAR(1),
    ANRECHJN         CHAR(1),
    KT_WAEISO        CHAR(3),
    KD_INSTITUTSNR   CHAR(4),
    AML_PARAMTYPE    CHAR(5),
    GESCHNR          CHAR(11),
    ROLLE_01_24      CHAR(24),
    constraint IXU_AML_001
        primary key (KD_KUNDNR, VERF_TYP, VFB_KUNDNR, PRFT_SYSTEM, PRFT_ACCOUNT_NUM)
);

create unique index S1_AML_ACCOWN_001
    on AML_ACC_OWNER (LAST_UPDATE);

