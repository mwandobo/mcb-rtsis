create table RELATIONSHIP_ACCNT
(
    FKCUST_HAS_AS_FIRS INTEGER  not null,
    FKCUST_HAS_AS_SECO INTEGER  not null,
    FK_RELATIONSHIPTYP CHAR(12) not null,
    FKGD_HAS_AS_REL    INTEGER  not null,
    FKGH_HAS_AS_REL    CHAR(5)  not null,
    ACCOUNT_NUMBER     CHAR(40) not null,
    PRFT_SYSTEM        SMALLINT not null,
    ACCOUNT_CD         SMALLINT,
    constraint IXU_RELACC_002
        primary key (FKCUST_HAS_AS_FIRS, FKCUST_HAS_AS_SECO, FK_RELATIONSHIPTYP, FKGH_HAS_AS_REL, FKGD_HAS_AS_REL,
                     ACCOUNT_NUMBER, PRFT_SYSTEM)
);

