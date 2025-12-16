create table ELTA_HD_TLF
(
    FILE_DATE       DATE           not null,
    FILE_TYPE       CHAR(1)        not null,
    FILE_SN         DECIMAL(10)    not null,
    FILENAME        CHAR(15)       not null,
    TOTAL_AMOUNT    DECIMAL(15, 2) not null,
    TOTAL_RECS      DECIMAL(10)    not null,
    RECONCILED_RECS INTEGER        not null,
    SETTLED_RECS    INTEGER        not null,
    RECONC_START    TIMESTAMP(6),
    RECONC_END      TIMESTAMP(6),
    TMSTAMP         TIMESTAMP(6),
    constraint PK_ELTA_HD_TLF
        primary key (FILE_SN, FILE_TYPE, FILE_DATE)
);

