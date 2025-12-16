create table ATM_HD_TLF
(
    FILE_SN       INTEGER  not null,
    FILENAME      CHAR(10) not null,
    TOTAL_RECS    INTEGER,
    CREATION_DATE DATE,
    FIID_CODE     CHAR(4),
    TIMESTMP      TIME,
    constraint IXU_ATM_035
        primary key (FILE_SN, FILENAME)
);

