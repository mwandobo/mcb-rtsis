create table TEMP_TBL
(
    FKCUST             INTEGER  not null,
    FK_RELATIONSHIPTYP CHAR(12) not null,
    FKGD_HAS_AS_REL    INTEGER  not null,
    FKGH_HAS_AS_REL    CHAR(5)  not null,
    ACCOUNT_NUMBER     CHAR(40) not null,
    PRFT_SYSTEM        SMALLINT not null,
    ACCOUNT_CD         SMALLINT,
    constraint IXU_TEMP_TBL_002
        primary key (FKCUST, FK_RELATIONSHIPTYP, FKGH_HAS_AS_REL, FKGD_HAS_AS_REL, ACCOUNT_NUMBER, PRFT_SYSTEM)
);

