create table PROV_SUB_ACCS
(
    REC_ID             DECIMAL(10) not null,
    OFFICE_ID          DECIMAL(11) not null,
    PROVIDER_ID        DECIMAL(11) not null,
    FK_GENERIC_DETASER INTEGER,
    COM_JUSTIFIC       INTEGER,
    DEB_JUSTIFIC       INTEGER,
    FK_LNS_COMMISSIID  INTEGER,
    TIMESTAMP          TIMESTAMP(6),
    FK_GENERIC_DETAFK  CHAR(5),
    GL_ACCOUNT         CHAR(21),
    PROV_SUBACCNT      CHAR(40),
    COM_DESCRIPTION    CHAR(40),
    PROV_SUBACC_DD     CHAR(40),
    DEB_DESCRIPTION    CHAR(40),
    constraint IXU_CP_102
        primary key (REC_ID, OFFICE_ID, PROVIDER_ID)
);

