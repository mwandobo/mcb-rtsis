create table ADMIN_HD_TLF
(
    FILE_SN       INTEGER  not null,
    FILENAME      CHAR(10) not null,
    TOTAL_RECS    INTEGER,
    TIMESTMP      DATE,
    CREATION_DATE DATE,
    BANK_ID       CHAR(4),
    constraint IXU_ATM_031
        primary key (FILE_SN, FILENAME)
);

