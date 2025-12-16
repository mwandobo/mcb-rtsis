create table MONTRAN_BANKS
(
    BANK_CODE       CHAR(9) not null
        constraint IXU_MON_002
            primary key,
    ENTRY_STATUS    CHAR(1),
    EXTERNAL_BANK   CHAR(1),
    GOVERNMENT_BANK CHAR(1),
    DESCRIPTION     VARCHAR(80),
    GL_TRANSIT_CODE CHAR(21)
);

